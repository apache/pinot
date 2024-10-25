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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
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
  public void setUp() {
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
      return new MailboxReceiveNode(stageId, mySchema, List.of(), nextStageId, null, null, null, null, false, false,
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
  public SimpleChildBuilder<MailboxReceiveNode> exchange(
      int nextStageId, SimpleChildBuilder<? extends PlanNode> childBuilder) {
    return (stageId, mySchema, myHints) -> {
      PlanNode input = childBuilder.build(stageId);
      MailboxSendNode mailboxSendNode = new MailboxSendNode(nextStageId, null, List.of(input), stageId, null, null,
          null, false, null, false);
      MailboxSendNode old = _stageRoots.put(nextStageId, mailboxSendNode);
      Preconditions.checkState(old == null, "Mailbox already exists for stageId: %s", nextStageId);
      return new MailboxReceiveNode(stageId, null, List.of(), nextStageId, null, null, null, null,
          false, false, mailboxSendNode);
    };
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
      MailboxSendNode mailboxSendNode = new MailboxSendNode(newStageId, mySchema, List.of(input), stageId, null, null,
          null, false, null, false);
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
  public interface SimpleChildBuilder<P extends PlanNode> extends ChildBuilder<P> {
    default P build(int stageId) {
      return build(stageId, null, null);
    }
  }
}
