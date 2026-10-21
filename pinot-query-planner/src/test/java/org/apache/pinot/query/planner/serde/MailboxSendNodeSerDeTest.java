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
package org.apache.pinot.query.planner.serde;

import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ReceiverSpec;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class MailboxSendNodeSerDeTest {
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"a", "b"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});

  private static MailboxSendNode node() {
    TableScanNode scan = new TableScanNode(1, SCHEMA, PlanNode.NodeHint.EMPTY, List.of(), "t", List.of("a", "b"));
    return new MailboxSendNode(1, SCHEMA, List.of(scan), List.of(2, 3, 4), PinotRelExchangeType.STREAMING,
        RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), false, null, false, KeySelector.DEFAULT_HASH_ALGORITHM);
  }

  @Test
  public void specsSurviveSerDe() {
    MailboxSendNode node = node();
    node.setReceiverSpec(new ReceiverSpec(2, RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), List.of(1)));
    // An empty mask (the receiver reads no column) must stay apart from a missing mask (it reads every column).
    node.setReceiverSpec(new ReceiverSpec(3, RelDistribution.Type.RANDOM_DISTRIBUTED, List.of(), List.of()));
    node.setReceiverSpec(new ReceiverSpec(4, RelDistribution.Type.HASH_DISTRIBUTED, List.of(1), null));

    MailboxSendNode deserialized = (MailboxSendNode) PlanNodeDeserializer.process(PlanNodeSerializer.process(node));

    assertEquals(deserialized, node);
    assertEquals(List.copyOf(deserialized.getReceiverSpecs()), List.copyOf(node.getReceiverSpecs()));
  }

  /// A node whose receivers all use the defaults keeps the wire format of the versions without specs.
  @Test
  public void uniformReceiversWriteNoSpec() {
    MailboxSendNode node = node();
    node.setReceiverSpec(node.getDefaultReceiverSpec(3));
    Plan.PlanNode proto = PlanNodeSerializer.process(node);
    assertEquals(proto.getMailboxSendNode().getReceiverSpecsCount(), 0);
    MailboxSendNode deserialized = (MailboxSendNode) PlanNodeDeserializer.process(proto);
    assertTrue(deserialized.getReceiverSpecs().isEmpty());
    assertTrue(deserialized.hasUniformReceivers());
  }

  /// Only the specs that differ from the default are written.
  @Test
  public void defaultSpecsAreNotWritten() {
    MailboxSendNode node = node();
    node.setReceiverSpec(node.getDefaultReceiverSpec(2));
    node.setReceiverSpec(new ReceiverSpec(3, RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), null));
    Plan.PlanNode proto = PlanNodeSerializer.process(node);
    assertEquals(proto.getMailboxSendNode().getReceiverSpecsCount(), 1);
    assertEquals(proto.getMailboxSendNode().getReceiverSpecs(0).getReceiverStageId(), 3);
  }
}
