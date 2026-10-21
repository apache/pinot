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
package org.apache.pinot.query.planner.plannode;

import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class MailboxSendNodeReceiverSpecTest {
  private static final DataSchema SCHEMA = new DataSchema(new String[]{"a", "b", "c"},
      new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING, ColumnDataType.LONG});

  private static MailboxSendNode node(RelDistribution.Type distribution, boolean prePartitioned) {
    return new MailboxSendNode(1, SCHEMA, List.of(), List.of(2, 3), PinotRelExchangeType.STREAMING, distribution,
        List.of(0), prePartitioned, null, false, KeySelector.DEFAULT_HASH_ALGORITHM);
  }

  @Test
  public void receiversWithoutSpecUseTheNode() {
    MailboxSendNode node = node(RelDistribution.Type.HASH_DISTRIBUTED, false);
    ReceiverSpec spec = node.getReceiverSpec(3);
    assertEquals(spec, new ReceiverSpec(3, RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), null));
    assertNull(spec.columnMaskArray());
    assertEquals(spec.receiverSchema(SCHEMA), SCHEMA);
    assertTrue(node.hasUniformReceivers());
    assertTrue(node.getReceiverSpecs().isEmpty());
  }

  @Test
  public void explicitSpecChangesOneReceiver() {
    MailboxSendNode node = node(RelDistribution.Type.HASH_DISTRIBUTED, false);
    ReceiverSpec spec = new ReceiverSpec(3, RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), List.of(2, 0));
    node.setReceiverSpec(spec);
    assertEquals(node.getReceiverSpec(3), spec);
    assertEquals(node.getReceiverSpec(2), node.getDefaultReceiverSpec(2));
    assertFalse(node.hasUniformReceivers());
    assertEquals(spec.receiverSchema(SCHEMA), new DataSchema(new String[]{"c", "a"},
        new ColumnDataType[]{ColumnDataType.LONG, ColumnDataType.INT}));
    assertEquals(spec.columnMaskArray(), new int[]{2, 0});
    assertTrue(node.explain().endsWith("[3:BROADCAST_DISTRIBUTEDcols[2, 0]]"), node.explain());
  }

  @Test
  public void specEqualToTheDefaultKeepsReceiversUniform() {
    MailboxSendNode node = node(RelDistribution.Type.HASH_DISTRIBUTED, false);
    node.setReceiverSpec(node.getDefaultReceiverSpec(3));
    assertTrue(node.hasUniformReceivers());
    assertEquals(node.explain(), "MAIL_SEND(HASH_DISTRIBUTED)");
  }

  @Test
  public void specsTakePartInEqualityAndCopies() {
    MailboxSendNode node = node(RelDistribution.Type.HASH_DISTRIBUTED, false);
    MailboxSendNode other = node(RelDistribution.Type.HASH_DISTRIBUTED, false);
    assertEquals(node, other);
    node.setReceiverSpec(new ReceiverSpec(3, RelDistribution.Type.RANDOM_DISTRIBUTED, List.of(), null));
    assertNotEquals(node, other);
    MailboxSendNode copy = (MailboxSendNode) node.withInputs(List.of());
    assertEquals(copy.getReceiverSpec(3), node.getReceiverSpec(3));
    assertEquals(copy, node);
  }

  @Test
  public void removingAReceiverRemovesItsSpec() {
    MailboxSendNode node = node(RelDistribution.Type.HASH_DISTRIBUTED, false);
    node.setReceiverSpec(new ReceiverSpec(3, RelDistribution.Type.RANDOM_DISTRIBUTED, List.of(), null));
    MailboxReceiveNode receiver = new MailboxReceiveNode(3, SCHEMA, 1, PinotRelExchangeType.STREAMING,
        RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), null, false, false, node);
    node.removeReceiver(receiver);
    assertTrue(node.getReceiverSpecs().isEmpty());
  }

  @Test
  public void invalidSpecsAreRejected() {
    MailboxSendNode node = node(RelDistribution.Type.HASH_DISTRIBUTED, false);
    assertThrows(IllegalArgumentException.class,
        () -> node.setReceiverSpec(new ReceiverSpec(9, RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), null)));
    assertThrows(IllegalArgumentException.class,
        () -> node.setReceiverSpec(new ReceiverSpec(3, RelDistribution.Type.SINGLETON, List.of(), null)));
    assertThrows(IllegalArgumentException.class,
        () -> node.setReceiverSpec(new ReceiverSpec(3, RelDistribution.Type.HASH_DISTRIBUTED, List.of(3), null)));
    assertThrows(IllegalArgumentException.class,
        () -> node.setReceiverSpec(new ReceiverSpec(3, RelDistribution.Type.HASH_DISTRIBUTED, List.of(0),
            List.of(1, 5))));
  }

  /// A local exchange and a pre-partitioned exchange wire the workers 1-to-1, so their receivers cannot differ.
  @Test
  public void nodesWiredOneToOneRejectSpecs() {
    ReceiverSpec spec = new ReceiverSpec(3, RelDistribution.Type.BROADCAST_DISTRIBUTED, List.of(), null);
    assertThrows(IllegalStateException.class,
        () -> node(RelDistribution.Type.SINGLETON, false).setReceiverSpec(spec));
    assertThrows(IllegalStateException.class,
        () -> node(RelDistribution.Type.HASH_DISTRIBUTED, true).setReceiverSpec(spec));
  }
}
