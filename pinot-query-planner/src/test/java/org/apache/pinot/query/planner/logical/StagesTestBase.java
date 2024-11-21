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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNodeVisitor;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;


/**
 * A base test class that can be used to write tests for stages using a fluent DSL.
 *
 * This class provides two features:
 * <ul>
 *   <li>Builders that can be used to create nodes in a fluent way.</li>
 *   <li>Access to the stages that were created during the test with {@link #stage(int)}.</li>
 * </ul>
 *
 * It is expected that each test method will call {@link #when(SimpleChildBuilder)} to create a new plan, which will
 * populate the list of stages. After that, the test can look for the stages with the {@link #stage(int)} method and
 * assert the expected behavior.
 */
public class StagesTestBase {
  private final HashMap<Integer, MailboxSendNode> _stageRoots = new HashMap<>();

  /**
   * Clears the list of stages.
   *
   * This method is automatically called by the test framework, ensuring each test starts with a clean slate.
   * This method can also be called in middle of the test, but that is not recommended given it usually means that the
   * test is getting too complex and difficult to read and/or get insights from it in case of failure.
   */
  @AfterMethod
  public void cleanup() {
    _stageRoots.clear();
  }

  /**
   * Creates a new plan that will have an initial stage.
   *
   * The stage will have a default {@link MailboxSendNode} whose stage will be 0 and its child the one created by the
   * builder.
   *
   * Notice that this method does not offer any way to customize the initial send mailbox.
   */
  public MailboxSendNode when(SimpleChildBuilder<? extends PlanNode> builder) {
    return sendMailbox(0, builder).build(0);
  }

  /**
   * Returns a builder that can be used to create a new mailbox receive node.
   *
   * It is usually recommended to use {@link #exchange(int, SimpleChildBuilder)} instead of this method, given that
   * {@code exchange} creates a pair of send and receive mailboxes and deals with the stageId management.
   */
  public SimpleChildBuilder<MailboxReceiveNode> receiveMailbox(SimpleChildBuilder<MailboxSendNode> childBuilder) {
    return (stageId, mySchema, myHints) -> {
      MailboxSendNode mailbox = childBuilder.build(stageId);
      int nextStageId = mailbox.getStageId();
      return new MailboxReceiveNode(stageId, mySchema, nextStageId, null, null, null, null, false, false,
          mailbox);
    };
  }

  /**
   * Creates a join node that will have the left and right nodes as children.
   *
   * The join type will be {@link JoinRelType#FULL}, the join strategy will be {@link JoinNode.JoinStrategy#HASH} and
   * there will be no conditions. If custom joins are needed feel free to add more builder methods or create your own
   * instance of {@link SimpleChildBuilder}.
   */
  public SimpleChildBuilder<JoinNode> join(
      SimpleChildBuilder<? extends PlanNode> leftBuilder,
      SimpleChildBuilder<? extends PlanNode> rightBuilder) {
    return (stageId, mySchema, myHints) -> {
      PlanNode left = leftBuilder.build(stageId);
      PlanNode right = rightBuilder.build(stageId);
      return new JoinNode(stageId, mySchema, myHints, List.of(left, right), JoinRelType.FULL,
          Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), JoinNode.JoinStrategy.HASH);
    };
  }

  /**
   * Creates a pair of receiver and sender nodes that will be logically connected.
   *
   * Whenever this builder is used to create a node, the mailbox send node will be added to the list of mailboxes.
   *
   * Although there are builder methods to create send and receive mailboxes separately, this method is recommended
   * because it deals with the stageId management and creates tests that are easier to read.
   */
  public ExchangeBuilder exchange(
      int nextStageId, SimpleChildBuilder<? extends PlanNode> childBuilder) {
    return new ExchangeBuilder() {
      @Override
      public MailboxReceiveNode build(int stageId, DataSchema dataSchema, PlanNode.NodeHint hints,
          PinotRelExchangeType exchangeType, RelDistribution.Type distribution, List<Integer> keys,
          boolean prePartitioned, List<RelFieldCollation> collations, boolean sort, boolean sortedOnSender) {
        PlanNode input = childBuilder.build(nextStageId);
        MailboxSendNode mailboxSendNode = new MailboxSendNode(nextStageId, input.getDataSchema(), List.of(input),
            stageId, exchangeType, distribution, keys, prePartitioned, collations, sort);
        MailboxSendNode old = _stageRoots.put(nextStageId, mailboxSendNode);
        Preconditions.checkState(old == null, "Mailbox already exists for stageId: %s", nextStageId);
        return new MailboxReceiveNode(stageId, input.getDataSchema(), nextStageId, exchangeType, distribution, keys,
            collations, sort, sortedOnSender, mailboxSendNode);
      }
    };
  }

  public interface ExchangeBuilder extends SimpleChildBuilder<MailboxReceiveNode> {
    MailboxReceiveNode build(int stageId, DataSchema dataSchema, PlanNode.NodeHint hints,
        PinotRelExchangeType exchangeType, RelDistribution.Type distribution, List<Integer> keys,
        boolean prePartitioned, List<RelFieldCollation> collations, boolean sort, boolean sortedOnSender);

    default MailboxReceiveNode build(int stageId, DataSchema dataSchema, PlanNode.NodeHint hints) {
      return build(stageId, null, null, null, null, null, false, null, false, false);
    }

    default ExchangeBuilder withDistributionType(RelDistribution.Type distribution) {
      return (stageId, dataSchema, hints, exchangeType, distribution1, keys, prePartitioned, collations, sort,
          sortedOnSender) ->
        build(stageId, dataSchema, hints, exchangeType, distribution, keys, prePartitioned, collations, sort,
            sortedOnSender);
    }
  }

  /**
   * Creates a table scan node with the given table name.
   */
  public SimpleChildBuilder<TableScanNode> tableScan(String tableName) {
    return (stageId, mySchema, myHints) -> new TableScanNode(stageId, mySchema, myHints, List.of(), tableName,
        Collections.emptyList());
  }

  /**
   * Looks for the mailbox that corresponds to the given stageId.
   * @throws IllegalStateException if the mailbox is not found.
   */
  public MailboxSendNode stage(int stageId) {
    MailboxSendNode result = _stageRoots.get(stageId);
    Preconditions.checkState(result != null, "Mailbox not found for stageId: %s", stageId);
    return result;
  }

  /**
   * Returns a builder that can be used to create a new mailbox send node.
   *
   * Whenever this builder is used to create a node, the created node will be added to the list of mailboxes.
   *
   * It is usually recommended to use {@link #exchange(int, SimpleChildBuilder)} instead of this method, given that
   * {@code exchange} creates a pair of send and receive mailboxes and deals with the stageId management.
   */
  public SimpleChildBuilder<MailboxSendNode> sendMailbox(
      int newStageId, SimpleChildBuilder<? extends PlanNode> childBuilder) {
    return (stageId, mySchema, myHints) -> {
      PlanNode input = childBuilder.build(stageId);
      MailboxSendNode mailboxSendNode = new MailboxSendNode(newStageId, mySchema, List.of(input), stageId, null,
          null, null, false, null, false);
      MailboxSendNode old = _stageRoots.put(stageId, mailboxSendNode);
      Preconditions.checkState(old == null, "Mailbox already exists for stageId: %s", stageId);
      return mailboxSendNode;
    };
  }

  /**
   * A builder that can be used to create a child node.
   *
   * It is not expected for test writers to implement this class. Instead it is recommended to use methods like
   * {@link #exchange(int, SimpleChildBuilder)}, {@link #join(SimpleChildBuilder, SimpleChildBuilder)},
   * {@link #tableScan(String)} and others to chain instances of this class.
   */
  @FunctionalInterface
  public interface ChildBuilder<P extends PlanNode> {
    P build(int stageId, @Nullable DataSchema dataSchema, @Nullable PlanNode.NodeHint hints);

    /**
     * This can be used to set the data schema for the node being built in a fluent way.
     *
     * For example:
     *
     * <pre>
     *   when(
     *     tableScan("T1")
     *       .withDataSchema(new DataSchema(...))
     *   );
     * </pre>
     */
    default SimpleChildBuilder<P> withDataSchema(DataSchema dataSchema) {
      return (stageId, dataSchema1, hints) -> build(stageId, dataSchema, hints);
    }

    /**
     * This can be used to set the hints for the node being built in a fluent way.
     *
     * For example:
     * <pre>
     *   when(
     *     tableScan("T1")
     *       .withHints("hint1", Map.of("key1", "value1"))
     *   );
     * </pre>
     */
    default SimpleChildBuilder<P> withHints(String key, Map<String, String> values) {
      return (stageId, dataSchema, hints1) -> {
        PlanNode.NodeHint myHints = hints1 == null
            ? new PlanNode.NodeHint(ImmutableMap.of(key, values))
            : hints1.with(key, values);
        return build(stageId, dataSchema, myHints);
      };
    }
  }

  /**
   * A marker interface that extends {@link ChildBuilder} that is used to create a child node without any additional
   * customization.
   *
   * Usually this is the kind of builder used by most builder methods (like {@link #tableScan(String)}) because hints
   * and data schema are not usually needed to be modified from the parent node.
   * @param <P>
   */
  @FunctionalInterface
  public interface SimpleChildBuilder<P extends PlanNode> extends ChildBuilder<P> {
    default P build(int stageId) {
      return build(stageId, null, null);
    }
  }

  /**
   * A helper class that can be used to create a spool in the context of a test.
   * <p>
   * These spools are used to create a single sender that will send data to multiple receivers.
   * This class is just a helper to make it easier to create the sender and the receivers in a single fluent way during
   * a test. A spool breaks by definition the idea that plan nodes are tree-like. Instead once spools are used, the
   * plan nodes are a directed graph that should not have cycles. The latter is not enforced by this class but a
   * responsibility of the test writer.
   * <p>
   * Graphs are more complex to write in a nice readable way and require some mutation on the nodes that are created.
   * In order to help, this class has two states: the initial state and the sealed state. When a new spool is created,
   * it is in the initial state and can {@link #newReceiver()} can be called multiple times to create multiple
   * receivers. Once one of these receivers is built, the spool is sealed and no more receivers can be created.
   * <p>
   * Usually this class should be used in the following manner:
   * <p>
   * <pre>
   *   Spool readT1 = new Spool(3, tableScan("T1")); // here the spool is created
   *   ExchangeBuilder builder = exchange(1,
   *     join(
   *       readT1.newReceiver(), // here a new receiver is created
   *       readT1.newReceiver() // another receiver is created
   *     )
   *   );
   *   // here the builder is called, which recursively calls the build method on the receivers, which seals the spool
   *   when(builder);
   * </pre>
   * <p>
   *
   * Notice that usually the builder is not stored as a variable but directly used as argument to when. For example,
   * {@code when(exchange(1, ...));}. This is completely fine and recommended. The snippet above splits the creation of
   * the builder from the call to when to make it easier to understand the flow of the test.
   * <p>
   * This means that if more than one spool is needed in a test, the test writer should create multiple instances of
   * this class.
   */
  public static class SpoolBuilder {
    private final int _senderStageId;
    /**
     * The set of receiver builders. A new element is added every time {@link #newReceiver()} is called.
     * When the first builder is built, {@link #seal()} is called, which creates the sender node.
     */
    private final Set<SpoolReceiverBuilder> _receiverBuilder = Collections.newSetFromMap(new IdentityHashMap<>());
    private MailboxSendNode _sender;
    private final SimpleChildBuilder<? extends PlanNode> _childBuilder;

    /**
     * Creates a new spool with the given sender stage id and child builder.
     *
     * The child builder will be used to create the child node that will generate the data that will be sent to the
     * multiple receivers.
     */
    public SpoolBuilder(int senderStageId, SimpleChildBuilder<? extends PlanNode> spoolChildBuilder) {
      _senderStageId = senderStageId;
      _childBuilder = spoolChildBuilder;
    }

    /**
     * Returns the sender node for this spool.
     *
     * This method can only be called after the spool is sealed, otherwise the sender won't be available and this method
     * will fail with an exception.
     */
    public MailboxSendNode getSender() {
      Preconditions.checkState(isSealed(), "Spool not sealed");
      return _sender;
    }

    /**
     * Returns whether the spool is sealed or not.
     */
    public boolean isSealed() {
      return _sender != null;
    }

    /**
     * Creates a new receiver builder that can be used to create a new receiver for this spool.
     *
     * This method is similar to other builder methods (like {@link #tableScan(String)} or
     * {@link #join(SimpleChildBuilder, SimpleChildBuilder)}) and can be called multiple times to create multiple
     * receivers.
     *
     * In most scenarios, the overloaded method {@link #newReceiver()} is good enough. This method is useful when the
     * test writer wants to customize the receiver in some way (for example, changing the data schema or hints).
     * The customize function will be called with a base builder that creates the receiver with the same data schema
     * as the server and no hints.
     */
    public SimpleChildBuilder<MailboxReceiveNode> newReceiver(
        Function<SimpleChildBuilder<MailboxReceiveNode>, SimpleChildBuilder<MailboxReceiveNode>> customize) {
      Preconditions.checkState(!isSealed(), "Spool already sealed");

      SpoolReceiverBuilder spoolReceiverBuilder = new SpoolReceiverBuilder(customize);

      _receiverBuilder.add(spoolReceiverBuilder);
      return spoolReceiverBuilder;
    }


    /**
     * Creates a new receiver builder that can be used to create a new receiver for this spool.
     *
     * This method is similar to other builder methods (like {@link #tableScan(String)} or
     * {@link #join(SimpleChildBuilder, SimpleChildBuilder)}) and can be called multiple times to create multiple
     * receivers.
     *
     * This method creates a receiver with the same data schema as the sender and no hints. In case the test writer
     * wants to customize the receiver, the method {@link #newReceiver(Function)} should be used.
     */
    public SimpleChildBuilder<MailboxReceiveNode> newReceiver() {
      return newReceiver(a -> a);
    }

    private void seal() {
      if (isSealed()) { // for simplicity the seal method may be called multiple times
        return;
      }

      PlanNode input = _childBuilder.build(_senderStageId);
      DataSchema mySchema = input.getDataSchema();
      _sender = new MailboxSendNode(_senderStageId, mySchema, List.of(input), null,
          null, null, false, null, false);
    }

    /**
     * This is the internal class returned as a result of the {@link #newReceiver(Function)} method.
     *
     * They don't just create the receiver, but also end up sealing the spool and modify the sender to add the receiver
     * to the list of receivers.
     */
    private class SpoolReceiverBuilder implements SimpleChildBuilder<MailboxReceiveNode> {
      @Nullable
      private MailboxReceiveNode _receiver;
      private final Function<SimpleChildBuilder<MailboxReceiveNode>, SimpleChildBuilder<MailboxReceiveNode>> _customize;

      public SpoolReceiverBuilder(
          Function<SimpleChildBuilder<MailboxReceiveNode>, SimpleChildBuilder<MailboxReceiveNode>> customize) {
        _customize = customize;
      }

      @Override
      public MailboxReceiveNode build(int stageId, @Nullable DataSchema dataSchema, @Nullable PlanNode.NodeHint hints) {
        Preconditions.checkState(dataSchema == null, "Data schema for spool must be set internally");
        Preconditions.checkState(hints == null, "Hints for spool must be set internally");
        if (_receiver == null) {
          seal();
          SimpleChildBuilder<MailboxReceiveNode> baseBuilder = (currentStageId, ignoreSchema, ignoreHints) -> {
            DataSchema mySchema = _sender.getDataSchema();
            return new MailboxReceiveNode(currentStageId, mySchema, _senderStageId, null, null, null, null, false,
                false, _sender);
          };
          SimpleChildBuilder<MailboxReceiveNode> receiveBuilder = _customize.apply(baseBuilder);
          _receiver = receiveBuilder.build(stageId);
          _sender.addReceiver(_receiver);
        }
        Preconditions.checkState(_receiver.getStageId() == stageId, "Receiver stageId mismatch. "
                + "Expected %s, received %s", _receiver.getStageId(), stageId);
        assert _receiver != null;
        return _receiver;
      }
    }
  }

  public void assertEqualPlan(PlanNode actual, PlanNode expected) {
    if (expected == null || actual == null) {
      if (expected == null && actual == null) {
        return;
      }
      throw new AssertionError("Expected: \n" + expected + ", actual: \n" + actual);
    }
    if (Objects.equals(expected, actual)) {
      return;
    }
    Assert.fail("Expected: \n" + explainNode(expected) + ", actual: \n" + explainNode(actual));
  }

  private String explainNode(PlanNode node) {
    StringBuilder sb = new StringBuilder();
    NodePrinter nodePrinter = new NodePrinter(sb);
    node.visit(nodePrinter, null);
    return sb.toString();
  }

  private static class NodePrinter extends PlanNodeVisitor.DepthFirstVisitor<Void, Void> {
    private final StringBuilder _builder;
    private int _indent;

    public NodePrinter(StringBuilder builder) {
      _builder = builder;
    }

    @Override
    protected Void preChildren(PlanNode node, Void context) {
      int stageId = node.getStageId();
      for (int i = 0; i < _indent; i++) {
        _builder.append("  ");
      }
      _builder.append('[')
          .append(stageId)
          .append("]: ")
          .append(node.explain())
          .append('\n');
      _indent++;
      return null;
    }

    @Override
    protected Void postChildren(PlanNode node, Void context) {
      _indent--;
      return super.postChildren(node, context);
    }
  }
}
