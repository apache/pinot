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
package org.apache.pinot.query.planner.physical;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class FragmentTypeTest {

  private static final DataSchema SCHEMA = new DataSchema(
      new String[]{"col1"}, new ColumnDataType[]{ColumnDataType.INT});

  @Test
  public void testNonMailboxSendRootReturnsNull() {
    PlanNode filterNode = new FilterNode(0, SCHEMA, null, List.of(), null);
    assertNull(FragmentType.classify(filterNode, true, Map.of()));
  }

  @Test
  public void testNullRootReturnsNull() {
    assertNull(FragmentType.classify(null, true, Map.of()));
  }

  @Test
  public void testNoScannedTableReturnsIntermediate() {
    MailboxSendNode sendNode = new MailboxSendNode(0, SCHEMA, List.of(),
        1, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");
    assertEquals(FragmentType.classify(sendNode, false, Map.of()), FragmentType.INTERMEDIATE);
  }

  @Test
  public void testLeafWithNoSingletonReceive() {
    // A send node with no receive children (pure leaf scan)
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");
    assertEquals(FragmentType.classify(sendNode, true, Map.of()), FragmentType.LEAF);
  }

  @Test
  public void testIntermediateWhenHashReceiveFromUnknownSender() {
    // A HASH receive from a sender with no metadata (unknown/non-leaf) should return INTERMEDIATE
    MailboxReceiveNode receiveNode = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(receiveNode),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");
    assertEquals(FragmentType.classify(sendNode, true, Map.of()), FragmentType.INTERMEDIATE);
  }

  @Test
  public void testLeafWithHashReceiveFromLeafSender() {
    // A HASH receive from a leaf sender (has scanned tables) should still be LEAF
    MailboxReceiveNode receiveNode = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(receiveNode),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    DispatchablePlanMetadata meta2 = new DispatchablePlanMetadata();
    meta2.addScannedTable("factTable");
    metadataMap.put(2, meta2);

    assertEquals(FragmentType.classify(sendNode, true, metadataMap), FragmentType.LEAF);
  }

  @Test
  public void testIntermediateWhenHashReceiveFromNonLeafSender() {
    // A HASH receive from a non-leaf sender (no scanned tables) should return INTERMEDIATE
    MailboxReceiveNode receiveNode = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(receiveNode),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    DispatchablePlanMetadata meta2 = new DispatchablePlanMetadata();
    metadataMap.put(2, meta2);

    assertEquals(FragmentType.classify(sendNode, true, metadataMap), FragmentType.INTERMEDIATE);
  }

  @Test
  public void testIntermediateWhenBroadcastReceiveFromNonLeafSender() {
    // A BROADCAST receive from a non-leaf sender should return INTERMEDIATE
    MailboxReceiveNode receiveNode = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.BROADCAST_DISTRIBUTED,
        null, null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(receiveNode),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    DispatchablePlanMetadata meta2 = new DispatchablePlanMetadata();
    metadataMap.put(2, meta2);

    assertEquals(FragmentType.classify(sendNode, true, metadataMap), FragmentType.INTERMEDIATE);
  }

  @Test
  public void testIntermediateWhenMixedSingletonLeafAndHashFromNonLeaf() {
    // Stage scans table, SINGLETON from leaf, but HASH from non-leaf → INTERMEDIATE
    MailboxReceiveNode singletonReceive = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON,
        null, null, false, false, null);
    MailboxReceiveNode hashReceive = new MailboxReceiveNode(1, SCHEMA, 3,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(singletonReceive, hashReceive),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    DispatchablePlanMetadata meta2 = new DispatchablePlanMetadata();
    meta2.addScannedTable("dimTable");
    metadataMap.put(2, meta2);
    DispatchablePlanMetadata meta3 = new DispatchablePlanMetadata();
    metadataMap.put(3, meta3);

    assertEquals(FragmentType.classify(sendNode, true, metadataMap), FragmentType.INTERMEDIATE);
  }

  @Test
  public void testSingletonLeafWhenAllReceivesFromLeaves() {
    // Stage scans table, SINGLETON from leaf, HASH from leaf → SINGLETON_LEAF
    MailboxReceiveNode singletonReceive = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON,
        null, null, false, false, null);
    MailboxReceiveNode hashReceive = new MailboxReceiveNode(1, SCHEMA, 3,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(singletonReceive, hashReceive),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    DispatchablePlanMetadata meta2 = new DispatchablePlanMetadata();
    meta2.addScannedTable("dimTable");
    metadataMap.put(2, meta2);
    DispatchablePlanMetadata meta3 = new DispatchablePlanMetadata();
    meta3.addScannedTable("factTable");
    metadataMap.put(3, meta3);

    assertEquals(FragmentType.classify(sendNode, true, metadataMap), FragmentType.SINGLETON_LEAF);
  }

  @Test
  public void testSingletonLeafWhenSenderIsLeaf() {
    // Stage 1 has a SINGLETON receive from stage 2, and stage 2 has scanned tables
    MailboxReceiveNode receiveNode = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON,
        null, null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(receiveNode),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    DispatchablePlanMetadata senderMeta = new DispatchablePlanMetadata();
    senderMeta.addScannedTable("dimTable");
    metadataMap.put(2, senderMeta);

    assertEquals(FragmentType.classify(sendNode, true, metadataMap), FragmentType.SINGLETON_LEAF);
  }

  @Test
  public void testIntermediateWhenSingletonSenderHasNoScannedTables() {
    // Stage 1 has a SINGLETON receive from stage 2, but stage 2 has no scanned tables
    MailboxReceiveNode receiveNode = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON,
        null, null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(receiveNode),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    DispatchablePlanMetadata senderMeta = new DispatchablePlanMetadata();
    metadataMap.put(2, senderMeta);

    assertEquals(FragmentType.classify(sendNode, true, metadataMap), FragmentType.INTERMEDIATE);
  }

  @Test
  public void testIntermediateWhenSingletonSenderMetadataMissing() {
    // Stage 1 has a SINGLETON receive from stage 2, but stage 2 metadata is null
    MailboxReceiveNode receiveNode = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON,
        null, null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(receiveNode),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    assertEquals(FragmentType.classify(sendNode, true, Map.of()), FragmentType.INTERMEDIATE);
  }

  @Test
  public void testMultipleSingletonReceivesAllLeaves() {
    // Two SINGLETON receives, both senders have scanned tables
    MailboxReceiveNode receive1 = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON,
        null, null, false, false, null);
    MailboxReceiveNode receive2 = new MailboxReceiveNode(1, SCHEMA, 3,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON,
        null, null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(receive1, receive2),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    DispatchablePlanMetadata meta2 = new DispatchablePlanMetadata();
    meta2.addScannedTable("dim1");
    metadataMap.put(2, meta2);
    DispatchablePlanMetadata meta3 = new DispatchablePlanMetadata();
    meta3.addScannedTable("dim2");
    metadataMap.put(3, meta3);

    assertEquals(FragmentType.classify(sendNode, true, metadataMap), FragmentType.SINGLETON_LEAF);
  }

  @Test
  public void testMultipleSingletonReceivesOnlyOneIsLeaf() {
    // Two SINGLETON receives; one sender has scanned tables, the other does not
    MailboxReceiveNode receive1 = new MailboxReceiveNode(1, SCHEMA, 2,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON,
        null, null, false, false, null);
    MailboxReceiveNode receive2 = new MailboxReceiveNode(1, SCHEMA, 3,
        PinotRelExchangeType.STREAMING, RelDistribution.Type.SINGLETON,
        null, null, false, false, null);
    MailboxSendNode sendNode = new MailboxSendNode(1, SCHEMA, List.of(receive1, receive2),
        0, PinotRelExchangeType.STREAMING, RelDistribution.Type.HASH_DISTRIBUTED,
        List.of(0), false, null, false, "murmur");

    Map<Integer, DispatchablePlanMetadata> metadataMap = new HashMap<>();
    DispatchablePlanMetadata meta2 = new DispatchablePlanMetadata();
    meta2.addScannedTable("dim1");
    metadataMap.put(2, meta2);
    DispatchablePlanMetadata meta3 = new DispatchablePlanMetadata();
    metadataMap.put(3, meta3);

    assertEquals(FragmentType.classify(sendNode, true, metadataMap), FragmentType.INTERMEDIATE);
  }
}
