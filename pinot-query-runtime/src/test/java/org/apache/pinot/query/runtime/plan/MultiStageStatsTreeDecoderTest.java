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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.SortOperator;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link MultiStageStatsTreeDecoder} and {@link StageStatsTreeNode#merge(StageStatsTreeNode)}.
 */
public class MultiStageStatsTreeDecoderTest {

  /**
   * Round-trip: build a proto tree by hand, decode it, verify shape, planNodeIds, and stat-map contents.
   */
  @Test
  public void testDecodeRoundTrip()
      throws Exception {
    StatMap<BaseMailboxReceiveOperator.StatKey> receiveStat =
        new StatMap<>(BaseMailboxReceiveOperator.StatKey.class)
            .merge(BaseMailboxReceiveOperator.StatKey.EXECUTION_TIME_MS, 100)
            .merge(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS, 10);
    StatMap<MailboxSendOperator.StatKey> sendStat =
        new StatMap<>(MailboxSendOperator.StatKey.class)
            .merge(MailboxSendOperator.StatKey.EXECUTION_TIME_MS, 3);

    Worker.StageStatsNode receiveNode = Worker.StageStatsNode.newBuilder()
        .setOperatorTypeId(MultiStageOperator.Type.MAILBOX_RECEIVE.getId())
        .addPlanNodeIds(7)
        .setStatMap(serialize(receiveStat))
        .build();
    Worker.StageStatsNode sendNode = Worker.StageStatsNode.newBuilder()
        .setOperatorTypeId(MultiStageOperator.Type.MAILBOX_SEND.getId())
        .addPlanNodeIds(0)
        .setStatMap(serialize(sendStat))
        .addChildren(receiveNode)
        .build();
    Worker.MultiStageStatsTree proto = Worker.MultiStageStatsTree.newBuilder()
        .setCurrentStageId(2)
        .setCurrentStage(sendNode)
        .build();

    MultiStageStatsTreeDecoder.Decoded decoded = MultiStageStatsTreeDecoder.decode(proto);
    Assert.assertEquals(decoded.getCurrentStageId(), 2);
    StageStatsTreeNode root = decoded.getCurrentStage();
    Assert.assertEquals(root.getType(), MultiStageOperator.Type.MAILBOX_SEND);
    Assert.assertEquals(root.getPlanNodeIds(), List.of(0));
    Assert.assertEquals(root.getStatMap(), sendStat);
    Assert.assertEquals(root.getChildren().size(), 1);

    StageStatsTreeNode receive = root.getChildren().get(0);
    Assert.assertEquals(receive.getType(), MultiStageOperator.Type.MAILBOX_RECEIVE);
    Assert.assertEquals(receive.getPlanNodeIds(), List.of(7));
    Assert.assertEquals(receive.getStatMap(), receiveStat);
    Assert.assertTrue(receive.getChildren().isEmpty());
  }

  /**
   * Merging two trees of the same shape sums the counters per node.
   */
  @Test
  public void testMergeSameShape()
      throws Exception {
    StageStatsTreeNode a = leafNode(MultiStageOperator.Type.AGGREGATE,
        new StatMap<>(AggregateOperator.StatKey.class).merge(AggregateOperator.StatKey.EMITTED_ROWS, 5));
    StageStatsTreeNode b = leafNode(MultiStageOperator.Type.AGGREGATE,
        new StatMap<>(AggregateOperator.StatKey.class).merge(AggregateOperator.StatKey.EMITTED_ROWS, 7));

    a.merge(b);
    @SuppressWarnings("unchecked")
    StatMap<AggregateOperator.StatKey> merged = (StatMap<AggregateOperator.StatKey>) a.getStatMap();
    Assert.assertEquals(merged.getLong(AggregateOperator.StatKey.EMITTED_ROWS), 12);
  }

  /**
   * Merging a tree with mismatched operator type at the root throws ShapeMismatchException.
   */
  @Test(expectedExceptions = StageStatsTreeNode.ShapeMismatchException.class)
  public void testMergeTypeMismatchThrows()
      throws Exception {
    StageStatsTreeNode a = leafNode(MultiStageOperator.Type.AGGREGATE,
        new StatMap<>(AggregateOperator.StatKey.class));
    StageStatsTreeNode b = leafNode(MultiStageOperator.Type.SORT_OR_LIMIT,
        new StatMap<>(SortOperator.StatKey.class));
    a.merge(b);
  }

  /**
   * Merging a tree with a different number of children throws ShapeMismatchException.
   */
  @Test(expectedExceptions = StageStatsTreeNode.ShapeMismatchException.class)
  public void testMergeArityMismatchThrows()
      throws Exception {
    StageStatsTreeNode a = new StageStatsTreeNode(MultiStageOperator.Type.AGGREGATE, List.of(),
        new StatMap<>(AggregateOperator.StatKey.class), List.of(
        leafNode(MultiStageOperator.Type.MAILBOX_RECEIVE,
            new StatMap<>(BaseMailboxReceiveOperator.StatKey.class))));
    StageStatsTreeNode b = leafNode(MultiStageOperator.Type.AGGREGATE,
        new StatMap<>(AggregateOperator.StatKey.class));
    a.merge(b);
  }

  /**
   * Decoder rejects an unknown operator type id with DecodeFailedException — exercises mixed-version safety where a
   * newer server emits a type id this older broker doesn't recognise.
   */
  @Test(expectedExceptions = MultiStageStatsTreeDecoder.DecodeFailedException.class)
  public void testDecodeUnknownTypeThrows()
      throws Exception {
    Worker.StageStatsNode bad = Worker.StageStatsNode.newBuilder()
        .setOperatorTypeId(Integer.MAX_VALUE)
        .build();
    Worker.MultiStageStatsTree proto = Worker.MultiStageStatsTree.newBuilder()
        .setCurrentStageId(0)
        .setCurrentStage(bad)
        .build();
    MultiStageStatsTreeDecoder.decode(proto);
  }

  // ---- helpers ----

  private static StageStatsTreeNode leafNode(MultiStageOperator.Type type, StatMap<?> statMap) {
    return new StageStatsTreeNode(type, List.of(), statMap, List.of());
  }

  private static ByteString serialize(StatMap<?> statMap)
      throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(baos)) {
      statMap.serialize(output);
      output.flush();
      return ByteString.copyFrom(baos.toByteArray());
    }
  }
}
