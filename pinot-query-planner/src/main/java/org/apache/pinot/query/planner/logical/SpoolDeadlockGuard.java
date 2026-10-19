/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.planner.logical;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SetOpNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.WindowNode;


/// Detects plans where a spool can block the query until it times out.
///
/// A spool is a stage that sends its output to more than one consumer stage. It sends each block to all its
/// consumers, so it moves at the pace of the slowest consumer: when the mailbox of one consumer is full, the spool
/// stops for everybody. Some operators read one input only after another input finishes:
///
/// - A join reads its left (probe) input only after its right (build) input finishes.
/// - `INTERSECT` and `EXCEPT` read their left input only after their right input finishes.
/// - `UNION` reads its inputs in order: input `i` only after inputs `0` to `i - 1` finish.
/// - The main operator chain of a stage reads nothing until the pipeline breaker of that stage finishes.
///
/// Such an operator *defers* the receives in its later input behind the receives in its earlier input. The query
/// blocks when a stage `S` that a deferred receive stops is also necessary for the earlier input to finish:
///
/// 1. The deferred receive is not read, so its mailbox fills up and its sender stops.
/// 2. The sender stops reading its inputs, and the stop moves up through every stage on a *streaming* path, up to `S`.
/// 3. `S` stops for all its consumers, so the earlier input never finishes, and step 1 never ends.
///
/// An operator that reads its whole input before it emits its first row ends a streaming path: an aggregate, a
/// window, a sort with collations, the build input of a join, and the right input of `INTERSECT` and `EXCEPT`. The
/// stop does not go through it, because it has already read everything when the stop comes.
///
/// The check is conservative: it does not use the size of the data, so it also refuses plans whose data fits in the
/// mailbox buffers. It also does not trust an early end of the input (for example a `LIMIT`) to free a spool.
///
/// This class is stateless and thread-safe.
public final class SpoolDeadlockGuard {
  private SpoolDeadlockGuard() {
  }

  /// Returns the first case where the plan that starts at `root` can block, or null if no such case exists.
  ///
  /// `root` is the root of the plan after fragmentation: the root of stage 1, or any [MailboxSendNode].
  @Nullable
  public static DeadlockRisk findRisk(PlanNode root) {
    return new Analysis().findRisk(root);
  }

  /// A case where the plan can block.
  ///
  /// The receive from `waitingSenderStageId` in stage `stageId` waits until the receive from
  /// `blockingSenderStageId` finishes, because of the `operator` in that stage. Both receives depend on
  /// `sharedStageId`, so the blocking receive never finishes.
  public static final class DeadlockRisk {
    private final int _stageId;
    private final String _operator;
    private final int _waitingSenderStageId;
    private final int _blockingSenderStageId;
    private final int _sharedStageId;

    DeadlockRisk(int stageId, String operator, int waitingSenderStageId, int blockingSenderStageId,
        int sharedStageId) {
      _stageId = stageId;
      _operator = operator;
      _waitingSenderStageId = waitingSenderStageId;
      _blockingSenderStageId = blockingSenderStageId;
      _sharedStageId = sharedStageId;
    }

    public int getStageId() {
      return _stageId;
    }

    public String getOperator() {
      return _operator;
    }

    public int getWaitingSenderStageId() {
      return _waitingSenderStageId;
    }

    public int getBlockingSenderStageId() {
      return _blockingSenderStageId;
    }

    public int getSharedStageId() {
      return _sharedStageId;
    }

    @Override
    public String toString() {
      return "In stage " + _stageId + ", " + _operator + " reads the input from stage " + _waitingSenderStageId
          + " only after the input from stage " + _blockingSenderStageId + " finishes, and both depend on stage "
          + _sharedStageId;
    }
  }

  /// A pair of receives in one stage where `_waiting` is read only after `_blocking` finishes.
  private static final class Deferral {
    private final MailboxReceiveNode _waiting;
    private final MailboxReceiveNode _blocking;
    private final String _operator;

    Deferral(MailboxReceiveNode waiting, MailboxReceiveNode blocking, String operator) {
      _waiting = waiting;
      _blocking = blocking;
      _operator = operator;
    }
  }

  /// The receives of one subtree inside a stage.
  private static final class Receives {
    /// Every receive in the subtree.
    private final List<MailboxReceiveNode> _all = new ArrayList<>();
    /// The receives that reach the root of the subtree through a streaming path.
    private final List<MailboxReceiveNode> _streaming = new ArrayList<>();

    void addAll(Receives other) {
      _all.addAll(other._all);
      _streaming.addAll(other._streaming);
    }
  }

  /// What the analysis knows about one stage.
  private static final class StageInfo {
    private final Receives _receives;
    private final List<Deferral> _deferrals;

    StageInfo(Receives receives, List<Deferral> deferrals) {
      _receives = receives;
      _deferrals = deferrals;
    }
  }

  /// One run of the check. Not thread-safe; each call to [SpoolDeadlockGuard#findRisk] creates its own instance.
  private static final class Analysis {
    private final Map<PlanNode, StageInfo> _stageInfos = new IdentityHashMap<>();
    /// For each sender, the stages that stop when nobody reads the output of that sender.
    private final Map<MailboxSendNode, Set<MailboxSendNode>> _stopped = new IdentityHashMap<>();
    /// For each sender, the stages that must finish before that sender finishes.
    private final Map<MailboxSendNode, Set<MailboxSendNode>> _needed = new IdentityHashMap<>();

    @Nullable
    DeadlockRisk findRisk(PlanNode root) {
      ArrayDeque<PlanNode> pending = new ArrayDeque<>();
      Set<PlanNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
      pending.add(root);
      visited.add(root);
      while (!pending.isEmpty()) {
        PlanNode stageRoot = pending.poll();
        StageInfo info = getStageInfo(stageRoot);
        for (Deferral deferral : info._deferrals) {
          MailboxSendNode shared =
              firstShared(getStopped(deferral._waiting.getSender()), getNeeded(deferral._blocking.getSender()));
          if (shared != null) {
            return new DeadlockRisk(stageRoot.getStageId(), deferral._operator,
                deferral._waiting.getSenderStageId(), deferral._blocking.getSenderStageId(), shared.getStageId());
          }
        }
        for (MailboxReceiveNode receive : info._receives._all) {
          MailboxSendNode sender = receive.getSender();
          if (visited.add(sender)) {
            pending.add(sender);
          }
        }
      }
      return null;
    }

    @Nullable
    private static MailboxSendNode firstShared(Set<MailboxSendNode> stopped, Set<MailboxSendNode> needed) {
      for (MailboxSendNode node : stopped) {
        if (needed.contains(node)) {
          return node;
        }
      }
      return null;
    }

    private Set<MailboxSendNode> getStopped(MailboxSendNode sender) {
      Set<MailboxSendNode> result = _stopped.get(sender);
      if (result == null) {
        result = Collections.newSetFromMap(new IdentityHashMap<>());
        result.add(sender);
        for (MailboxReceiveNode receive : getStageInfo(sender)._receives._streaming) {
          result.addAll(getStopped(receive.getSender()));
        }
        _stopped.put(sender, result);
      }
      return result;
    }

    private Set<MailboxSendNode> getNeeded(MailboxSendNode sender) {
      Set<MailboxSendNode> result = _needed.get(sender);
      if (result == null) {
        result = Collections.newSetFromMap(new IdentityHashMap<>());
        result.add(sender);
        for (MailboxReceiveNode receive : getStageInfo(sender)._receives._all) {
          result.addAll(getNeeded(receive.getSender()));
        }
        _needed.put(sender, result);
      }
      return result;
    }

    private StageInfo getStageInfo(PlanNode stageRoot) {
      StageInfo info = _stageInfos.get(stageRoot);
      if (info == null) {
        List<Deferral> deferrals = new ArrayList<>();
        Receives receives = new Receives();
        PlanNode treeRoot = stageRoot instanceof MailboxSendNode ? stageRoot.getInputs().get(0) : stageRoot;
        receives.addAll(collect(treeRoot, deferrals));
        addPipelineBreakerDeferrals(receives, deferrals);
        info = new StageInfo(receives, deferrals);
        _stageInfos.put(stageRoot, info);
      }
      return info;
    }

    /// The main operator chain of a stage reads nothing until the pipeline breaker reads all its receives.
    private static void addPipelineBreakerDeferrals(Receives receives, List<Deferral> deferrals) {
      List<MailboxReceiveNode> breakers = new ArrayList<>();
      for (MailboxReceiveNode receive : receives._all) {
        if (receive.getExchangeType() == PinotRelExchangeType.PIPELINE_BREAKER) {
          breakers.add(receive);
        }
      }
      if (breakers.isEmpty()) {
        return;
      }
      for (MailboxReceiveNode receive : receives._all) {
        if (receive.getExchangeType() != PinotRelExchangeType.PIPELINE_BREAKER) {
          for (MailboxReceiveNode breaker : breakers) {
            deferrals.add(new Deferral(receive, breaker, "the pipeline breaker"));
          }
        }
      }
      receives._streaming.removeAll(breakers);
    }

    /// Collects the receives of the subtree that starts at `node`, without crossing into other stages, and adds to
    /// `deferrals` every pair of receives that an operator of the subtree reads one after the other.
    private static Receives collect(PlanNode node, List<Deferral> deferrals) {
      Receives result = new Receives();
      if (node instanceof MailboxReceiveNode) {
        MailboxReceiveNode receive = (MailboxReceiveNode) node;
        result._all.add(receive);
        result._streaming.add(receive);
        return result;
      }
      List<Receives> inputs = new ArrayList<>(node.getInputs().size());
      for (PlanNode input : node.getInputs()) {
        inputs.add(collect(input, deferrals));
      }
      if (node instanceof JoinNode) {
        // BaseJoinOperator reads the whole right input to build its table before it reads the left input.
        Receives left = inputs.get(0);
        Receives right = inputs.get(1);
        addDeferrals(left, right, "JOIN", deferrals);
        result._all.addAll(left._all);
        result._all.addAll(right._all);
        result._streaming.addAll(left._streaming);
        return result;
      }
      if (node instanceof SetOpNode) {
        SetOpNode setOp = (SetOpNode) node;
        if (setOp.getSetOpType() == SetOpNode.SetOpType.UNION) {
          // UnionOperator and UnionAllOperator read their inputs one after the other, each one until it finishes.
          for (int i = 1; i < inputs.size(); i++) {
            for (int j = 0; j < i; j++) {
              addDeferrals(inputs.get(i), inputs.get(j), setOp.getSetOpType().name(), deferrals);
            }
          }
          for (Receives input : inputs) {
            result.addAll(input);
          }
        } else {
          // BinarySetOperator reads the whole right input before it reads the left input.
          Receives left = inputs.get(0);
          Receives right = inputs.get(1);
          addDeferrals(left, right, setOp.getSetOpType().name(), deferrals);
          result._all.addAll(left._all);
          result._all.addAll(right._all);
          result._streaming.addAll(left._streaming);
        }
        return result;
      }
      for (Receives input : inputs) {
        result.addAll(input);
      }
      if (readsWholeInputFirst(node)) {
        result._streaming.clear();
      }
      return result;
    }

    private static void addDeferrals(Receives waiting, Receives blocking, String operator, List<Deferral> deferrals) {
      for (MailboxReceiveNode waitingReceive : waiting._all) {
        for (MailboxReceiveNode blockingReceive : blocking._all) {
          deferrals.add(new Deferral(waitingReceive, blockingReceive, operator));
        }
      }
    }

    /// Returns true for single-input operators that read their whole input before they emit their first row.
    ///
    /// The streaming group-by and distinct operators of the leaf stage emit before they finish, but a leaf stage
    /// receives data only through its pipeline breaker, which [#addPipelineBreakerDeferrals] already treats as read
    /// in full.
    private static boolean readsWholeInputFirst(PlanNode node) {
      if (node instanceof AggregateNode || node instanceof WindowNode) {
        return true;
      }
      // SortOperator streams when there are no collations (LimitSortOperator); otherwise it buffers every row.
      return node instanceof SortNode && !((SortNode) node).getCollations().isEmpty();
    }
  }
}
