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
/// - The main operator chain of a stage reads nothing until the pipeline breaker of that stage finishes, and the
///   pipeline breaker reads its own receives in round-robin with blocking reads.
///
/// Such an operator *defers* the receives in its later input behind the receives in its earlier input. While a
/// deferred receive waits, nobody reads it:
///
/// 1. Its mailbox fills up and its sender stops.
/// 2. The sender stops reading its inputs, and the stop moves up through every stage on a *streaming* path.
/// 3. Each spool on that path stops for all its consumers.
///
/// A waiting receive therefore *blocks* every deferral whose earlier input needs one of the stopped stages: that
/// earlier input cannot finish, so the receives that this other deferral defers wait too. The query blocks when the
/// deferrals block each other in a cycle. The simplest cycle has one deferral: a stage that the deferred receive stops
/// is necessary for the earlier input of the same operator. Longer cycles span several operators, for example two
/// joins where each one reads, as its build input, the spool that feeds the probe input of the other.
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

  /// Returns a cycle of deferrals that can block the plan that starts at `root`, or null if there is none.
  ///
  /// `root` is the root of the plan after fragmentation: the root of stage 1, or any [MailboxSendNode].
  @Nullable
  public static DeadlockRisk findRisk(PlanNode root) {
    return new Analysis().findRisk(root);
  }

  /// A cycle of deferrals that can block the query.
  ///
  /// Each element of `cycle` is a wait that cannot end until the next wait (the first one after the last) ends.
  public record DeadlockRisk(List<Wait> cycle) {
    public DeadlockRisk {
      cycle = List.copyOf(cycle);
    }

    @Override
    public String toString() {
      if (cycle.size() == 1) {
        Wait wait = cycle.get(0);
        return wait + ", and both depend on stage " + wait.sharedStageId();
      }
      StringBuilder builder = new StringBuilder("Circular wait: ");
      for (int i = 0; i < cycle.size(); i++) {
        Wait wait = cycle.get(i);
        if (i > 0) {
          builder.append("; ");
        }
        builder.append(wait).append(", and stage ").append(wait.blockingSenderStageId())
            .append(" depends on stage ").append(wait.sharedStageId()).append(", which the next wait stops");
      }
      return builder.toString();
    }
  }

  /// In stage `stageId`, the `operator` reads the receive from `waitingSenderStageId` only after the receive from
  /// `blockingSenderStageId` finishes. The blocking receive needs `sharedStageId`, a stage that the next wait of the
  /// cycle stops.
  public record Wait(int stageId, String operator, int waitingSenderStageId, int blockingSenderStageId,
                     int sharedStageId) {
    @Override
    public String toString() {
      return "In stage " + stageId + ", " + operator + " reads the input from stage " + waitingSenderStageId
          + " only after the input from stage " + blockingSenderStageId + " finishes";
    }
  }

  /// A pair of receives in one stage where `waiting` is read only after `blocking` finishes.
  private record Deferral(MailboxReceiveNode waiting, MailboxReceiveNode blocking, String operator) {
  }

  /// The receives of one subtree inside a stage. The lists are mutable: [Analysis#collect] fills them while it walks
  /// the subtree.
  ///
  /// @param all every receive in the subtree
  /// @param streaming the receives that reach the root of the subtree through a streaming path
  private record Receives(List<MailboxReceiveNode> all, List<MailboxReceiveNode> streaming) {
    Receives() {
      this(new ArrayList<>(), new ArrayList<>());
    }

    void addAll(Receives other) {
      all.addAll(other.all);
      streaming.addAll(other.streaming);
    }
  }

  /// What the analysis knows about one stage.
  private record StageInfo(Receives receives, List<Deferral> deferrals) {
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
      List<Deferral> deferrals = collectDeferrals(root);
      int numDeferrals = deferrals.size();
      // shared[i][j] is a stage that the waiting receive of deferral i stops and that the blocking receive of
      // deferral j needs, or null. When it is not null, deferral i blocks deferral j.
      MailboxSendNode[][] shared = new MailboxSendNode[numDeferrals][numDeferrals];
      for (int i = 0; i < numDeferrals; i++) {
        Set<MailboxSendNode> stopped = getStopped(deferrals.get(i).waiting().getSender());
        for (int j = 0; j < numDeferrals; j++) {
          shared[i][j] = firstShared(stopped, getNeeded(deferrals.get(j).blocking().getSender()));
        }
      }
      for (int i = 0; i < numDeferrals; i++) {
        if (shared[i][i] != null) {
          return new DeadlockRisk(List.of(toWait(deferrals.get(i), shared[i][i])));
        }
      }
      List<Integer> cycle = findCycle(shared);
      if (cycle == null) {
        return null;
      }
      // In the cycle, deferral cycle[k] blocks deferral cycle[k + 1]. A wait describes what keeps it waiting: its
      // blocking receive needs a stage that the next wait stops, so the waits are listed in the reverse order.
      List<Wait> waits = new ArrayList<>(cycle.size());
      for (int k = cycle.size() - 1; k >= 0; k--) {
        int blocked = cycle.get(k);
        int blocker = cycle.get(k == 0 ? cycle.size() - 1 : k - 1);
        waits.add(toWait(deferrals.get(blocked), shared[blocker][blocked]));
      }
      return new DeadlockRisk(waits);
    }

    private static Wait toWait(Deferral deferral, MailboxSendNode sharedStage) {
      // Both receives of a deferral belong to the stage of the operator that defers one behind the other.
      return new Wait(deferral.waiting().getStageId(), deferral.operator(), deferral.waiting().getSenderStageId(),
          deferral.blocking().getSenderStageId(), sharedStage.getStageId());
    }

    /// Returns the deferrals of every stage reachable from `root`. Two deferrals with the same pair of senders behave
    /// the same, so only the first one is kept.
    private List<Deferral> collectDeferrals(PlanNode root) {
      List<Deferral> result = new ArrayList<>();
      Map<MailboxSendNode, Set<MailboxSendNode>> seenPairs = new IdentityHashMap<>();
      ArrayDeque<PlanNode> pending = new ArrayDeque<>();
      Set<PlanNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
      pending.add(root);
      visited.add(root);
      while (!pending.isEmpty()) {
        PlanNode stageRoot = pending.poll();
        StageInfo info = getStageInfo(stageRoot);
        for (Deferral deferral : info.deferrals()) {
          Set<MailboxSendNode> blockingSenders = seenPairs.computeIfAbsent(deferral.waiting().getSender(),
              k -> Collections.newSetFromMap(new IdentityHashMap<>()));
          if (blockingSenders.add(deferral.blocking().getSender())) {
            result.add(deferral);
          }
        }
        for (MailboxReceiveNode receive : info.receives().all()) {
          MailboxSendNode sender = receive.getSender();
          if (visited.add(sender)) {
            pending.add(sender);
          }
        }
      }
      return result;
    }

    /// Returns the nodes of a cycle in the graph where `i -> j` exists when `edges[i][j]` is not null, in the order of
    /// the edges, or null if the graph has no cycle.
    @Nullable
    private static List<Integer> findCycle(Object[][] edges) {
      int numNodes = edges.length;
      // 0 = not visited, 1 = on the current path, 2 = done
      int[] state = new int[numNodes];
      int[] parent = new int[numNodes];
      for (int start = 0; start < numNodes; start++) {
        if (state[start] != 0) {
          continue;
        }
        // Iterative depth-first search: next[n] is the next successor of n to try.
        int[] next = new int[numNodes];
        ArrayDeque<Integer> path = new ArrayDeque<>();
        path.push(start);
        state[start] = 1;
        parent[start] = -1;
        while (!path.isEmpty()) {
          int node = path.peek();
          if (next[node] == numNodes) {
            state[node] = 2;
            path.pop();
            continue;
          }
          int successor = next[node]++;
          if (edges[node][successor] == null) {
            continue;
          }
          if (state[successor] == 1) {
            List<Integer> cycle = new ArrayList<>();
            for (int n = node; n != successor; n = parent[n]) {
              cycle.add(n);
            }
            cycle.add(successor);
            Collections.reverse(cycle);
            return cycle;
          }
          if (state[successor] == 0) {
            state[successor] = 1;
            parent[successor] = node;
            path.push(successor);
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
        for (MailboxReceiveNode receive : getStageInfo(sender).receives().streaming()) {
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
        for (MailboxReceiveNode receive : getStageInfo(sender).receives().all()) {
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
    ///
    /// The pipeline breaker itself reads its receives in round-robin with blocking reads, so a receive that has no data
    /// can keep the others unread. Each pair of pipeline-breaker receives is therefore treated as deferred both ways.
    private static void addPipelineBreakerDeferrals(Receives receives, List<Deferral> deferrals) {
      List<MailboxReceiveNode> breakers = new ArrayList<>();
      for (MailboxReceiveNode receive : receives.all()) {
        if (receive.getExchangeType() == PinotRelExchangeType.PIPELINE_BREAKER) {
          breakers.add(receive);
        }
      }
      if (breakers.isEmpty()) {
        return;
      }
      for (MailboxReceiveNode receive : receives.all()) {
        for (MailboxReceiveNode breaker : breakers) {
          if (receive != breaker) {
            deferrals.add(new Deferral(receive, breaker, "the pipeline breaker"));
          }
        }
      }
      receives.streaming().removeAll(breakers);
    }

    /// Collects the receives of the subtree that starts at `node`, without crossing into other stages, and adds to
    /// `deferrals` every pair of receives that an operator of the subtree reads one after the other.
    private static Receives collect(PlanNode node, List<Deferral> deferrals) {
      Receives result = new Receives();
      if (node instanceof MailboxReceiveNode) {
        MailboxReceiveNode receive = (MailboxReceiveNode) node;
        result.all().add(receive);
        result.streaming().add(receive);
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
        result.all().addAll(left.all());
        result.all().addAll(right.all());
        result.streaming().addAll(left.streaming());
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
          result.all().addAll(left.all());
          result.all().addAll(right.all());
          result.streaming().addAll(left.streaming());
        }
        return result;
      }
      for (Receives input : inputs) {
        result.addAll(input);
      }
      if (readsWholeInputFirst(node)) {
        result.streaming().clear();
      }
      return result;
    }

    private static void addDeferrals(Receives waiting, Receives blocking, String operator, List<Deferral> deferrals) {
      for (MailboxReceiveNode waitingReceive : waiting.all()) {
        for (MailboxReceiveNode blockingReceive : blocking.all()) {
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
