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
package org.apache.pinot.query.runtime.plan;

import com.google.protobuf.ByteString;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.SortOperator;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link MultiStageStatsTreeEncoder}.
 *
 * <p>The operator tree is built with mocked {@link MultiStageOperator} instances; only {@link
 * MultiStageOperator#getChildOperators()} is stubbed because that's the only method the encoder reads. Plan-node ids
 * are supplied via the {@code Function<MultiStageOperator, List<Integer>>} test entry point so we don't need to build
 * a full {@link OpChainExecutionContext}.
 */
public class MultiStageStatsTreeEncoderTest {

  /**
   * Linear chain: MailboxReceive -> Sort -> MailboxSend (root). Verifies inorder ordering between operator tree and
   * flat stats list, and that stat map bytes round-trip.
   */
  @Test
  public void testLinearChainEncode()
      throws IOException {
    MultiStageOperator receive = mockOperator();
    MultiStageOperator sort = mockOperator(receive);
    MultiStageOperator send = mockOperator(sort);

    StatMap<BaseMailboxReceiveOperator.StatKey> receiveStat =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class)
            .merge(BaseMailboxReceiveOperator.StatKey.EXECUTION_TIME_MS, 100)
            .merge(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS, 10);
    StatMap<SortOperator.StatKey> sortStat =
        new StatMap<>(SortOperator.StatKey.class)
            .merge(SortOperator.StatKey.EXECUTION_TIME_MS, 5)
            .merge(SortOperator.StatKey.EMITTED_ROWS, 8);
    StatMap<MailboxSendOperator.StatKey> sendStat =
        new StatMap<>(MailboxSendOperator.StatKey.class)
            .merge(MailboxSendOperator.StatKey.EXECUTION_TIME_MS, 3);

    MultiStageQueryStats stats = new MultiStageQueryStats.Builder(2)
        .customizeOpen(open -> open
            .addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE, receiveStat)
            .addLastOperator(MultiStageOperator.Type.SORT_OR_LIMIT, sortStat)
            .addLastOperator(MultiStageOperator.Type.MAILBOX_SEND, sendStat))
        .build();

    Map<MultiStageOperator, List<Integer>> idMap = new HashMap<>();
    idMap.put(receive, List.of(2));
    idMap.put(sort, List.of(1));
    idMap.put(send, List.of(0));

    Worker.MultiStageStatsTree tree = MultiStageStatsTreeEncoder.encode(send, stats, asResolver(idMap));

    Assert.assertEquals(tree.getCurrentStageId(), 2);
    Worker.StageStatsNode rootNode = tree.getCurrentStage();
    Assert.assertEquals(rootNode.getOperatorTypeId(), MultiStageOperator.Type.MAILBOX_SEND.getId());
    Assert.assertEquals(rootNode.getPlanNodeIdsList(), List.of(0));
    Assert.assertEquals(rootNode.getChildrenCount(), 1);

    Worker.StageStatsNode sortNode = rootNode.getChildren(0);
    Assert.assertEquals(sortNode.getOperatorTypeId(), MultiStageOperator.Type.SORT_OR_LIMIT.getId());
    Assert.assertEquals(sortNode.getPlanNodeIdsList(), List.of(1));
    Assert.assertEquals(sortNode.getChildrenCount(), 1);

    Worker.StageStatsNode receiveNode = sortNode.getChildren(0);
    Assert.assertEquals(receiveNode.getOperatorTypeId(), MultiStageOperator.Type.MAILBOX_RECEIVE.getId());
    Assert.assertEquals(receiveNode.getPlanNodeIdsList(), List.of(2));
    Assert.assertEquals(receiveNode.getChildrenCount(), 0);

    StatMap<BaseMailboxReceiveOperator.StatKey> deserializedReceiveStat = deserialize(
        receiveNode.getStatMap(), BaseMailboxReceiveOperator.StatKey.class);
    Assert.assertEquals(deserializedReceiveStat, receiveStat);
  }

  /**
   * N-ary set op (3 inputs). Verifies the encoder records all children regardless of arity, in left-to-right order,
   * and that the flat stats list is consumed in inorder (leftmost-leaf-first).
   */
  @Test
  public void testNaryEncode()
      throws IOException {
    MultiStageOperator left = mockOperator();
    MultiStageOperator mid = mockOperator();
    MultiStageOperator right = mockOperator();
    MultiStageOperator union = mockOperator(left, mid, right);

    StatMap<BaseMailboxReceiveOperator.StatKey> receiveStatLeft =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class)
            .merge(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS, 1);
    StatMap<BaseMailboxReceiveOperator.StatKey> receiveStatMid =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class)
            .merge(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS, 2);
    StatMap<BaseMailboxReceiveOperator.StatKey> receiveStatRight =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class)
            .merge(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS, 3);
    StatMap<AggregateOperator.StatKey> unionStat =
        new StatMap<>(AggregateOperator.StatKey.class)
            .merge(AggregateOperator.StatKey.EMITTED_ROWS, 6);

    MultiStageQueryStats stats = new MultiStageQueryStats.Builder(1)
        .customizeOpen(open -> open
            .addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE, receiveStatLeft)
            .addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE, receiveStatMid)
            .addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE, receiveStatRight)
            .addLastOperator(MultiStageOperator.Type.AGGREGATE, unionStat))
        .build();

    Map<MultiStageOperator, List<Integer>> idMap = new HashMap<>();
    idMap.put(left, List.of(1));
    idMap.put(mid, List.of(2));
    idMap.put(right, List.of(3));
    idMap.put(union, List.of(0));

    Worker.MultiStageStatsTree tree = MultiStageStatsTreeEncoder.encode(union, stats, asResolver(idMap));

    Worker.StageStatsNode rootNode = tree.getCurrentStage();
    Assert.assertEquals(rootNode.getOperatorTypeId(), MultiStageOperator.Type.AGGREGATE.getId());
    Assert.assertEquals(rootNode.getChildrenCount(), 3);
    Assert.assertEquals(rootNode.getChildren(0).getPlanNodeIdsList(), List.of(1));
    Assert.assertEquals(rootNode.getChildren(1).getPlanNodeIdsList(), List.of(2));
    Assert.assertEquals(rootNode.getChildren(2).getPlanNodeIdsList(), List.of(3));

    StatMap<BaseMailboxReceiveOperator.StatKey> roundTripped = deserialize(
        rootNode.getChildren(1).getStatMap(), BaseMailboxReceiveOperator.StatKey.class);
    Assert.assertEquals(roundTripped, receiveStatMid);
  }

  /**
   * One-to-many planNodeIds for a leaf operator: a single operator records multiple plan-node ids.
   */
  @Test
  public void testLeafPlanNodeFanOut()
      throws IOException {
    MultiStageOperator leaf = mockOperator();

    StatMap<AggregateOperator.StatKey> leafStat = new StatMap<>(AggregateOperator.StatKey.class)
        .merge(AggregateOperator.StatKey.EMITTED_ROWS, 42);
    MultiStageQueryStats stats = new MultiStageQueryStats.Builder(0)
        .customizeOpen(open -> open.addLastOperator(MultiStageOperator.Type.AGGREGATE, leafStat))
        .build();

    Map<MultiStageOperator, List<Integer>> idMap = new HashMap<>();
    idMap.put(leaf, Arrays.asList(10, 11, 12, 13));

    Worker.MultiStageStatsTree tree = MultiStageStatsTreeEncoder.encode(leaf, stats, asResolver(idMap));

    Assert.assertEquals(tree.getCurrentStage().getPlanNodeIdsList(), List.of(10, 11, 12, 13));
  }

  /**
   * Tree-vs-flat-list size mismatch: encoder must throw rather than emit a malformed tree.
   */
  @Test(expectedExceptions = IllegalStateException.class)
  public void testTreeFlatListMismatchThrows()
      throws IOException {
    MultiStageOperator leaf = mockOperator();
    MultiStageOperator root = mockOperator(leaf);

    // Only one entry in the flat list, but tree has two operators.
    MultiStageQueryStats stats = new MultiStageQueryStats.Builder(0)
        .customizeOpen(open -> open.addLastOperator(MultiStageOperator.Type.AGGREGATE,
            new StatMap<>(AggregateOperator.StatKey.class)))
        .build();

    MultiStageStatsTreeEncoder.encode(root, stats, op -> List.of());
  }

  // ---- helpers ----

  private static MultiStageOperator mockOperator(MultiStageOperator... children) {
    MultiStageOperator op = Mockito.mock(MultiStageOperator.class);
    Mockito.when(op.getChildOperators()).thenReturn(children.length == 0
        ? Collections.emptyList()
        : Arrays.asList(children));
    return op;
  }

  private static Function<MultiStageOperator, List<Integer>> asResolver(
      Map<MultiStageOperator, List<Integer>> idMap) {
    return op -> idMap.getOrDefault(op, List.of());
  }

  private static <K extends Enum<K> & StatMap.Key> StatMap<K> deserialize(ByteString bytes, Class<K> keyClass)
      throws IOException {
    try (DataInputStream input = new DataInputStream(bytes.newInput())) {
      return StatMap.deserialize(input, keyClass);
    }
  }
}
