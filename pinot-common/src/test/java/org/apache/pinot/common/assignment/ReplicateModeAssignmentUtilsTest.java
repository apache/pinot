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
package org.apache.pinot.common.assignment;

import java.util.List;
import java.util.Map;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.expectThrows;


/// Tests role persistence and its isolation from segment and partition-pinned assignments.
public class ReplicateModeAssignmentUtilsTest {
  @Test
  public void testRoleAndSegmentAssignmentsSurviveSerialization() {
    IdealState idealState = new IdealState("testTable_REALTIME");
    idealState.setPartitionState("seg1", "A", "ONLINE");
    idealState.setPartitionState("seg2", "A", "CONSUMING");
    ReplicateModeAssignmentUtils.setAssignment(idealState, 0, "A", List.of("C", "A", "B"));
    ZNRecordSerializer serializer = new ZNRecordSerializer();
    IdealState restored =
        new IdealState((ZNRecord) serializer.deserialize(serializer.serialize(idealState.getRecord())));

    assertEquals(restored.getRecord().getSimpleField("replicateMode.leader.0"), "A");
    assertEquals(restored.getRecord().getListField("replicateMode.replicas.0"), List.of("A", "B", "C"));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(restored, 0), "A");
    assertEquals(ReplicateModeAssignmentUtils.getFollowers(restored, 0), List.of("B", "C"));
    assertEquals(restored.getInstanceStateMap("seg2"), Map.of("A", "CONSUMING"));
    assertEquals(restored.getRecord().getMapFields(), idealState.getRecord().getMapFields());
  }

  @Test
  public void testPartitionPinnedNamespaceIsIndependent() {
    IdealState idealState = new IdealState("testTable_REALTIME");
    PartitionAssignmentUtils.setInstances(idealState, 0, List.of("X", "Y", "Z"));
    ReplicateModeAssignmentUtils.setAssignment(idealState, 0, "B", List.of("A", "B", "C"));
    ReplicateModeAssignmentUtils.setAssignment(idealState, 10000, "C", List.of("A", "B", "C"));
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState), Map.of(0, List.of("X", "Y", "Z")));
    assertEquals(ReplicateModeAssignmentUtils.getLeaders(idealState), Map.of(0, "B", 10000, "C"));
    PartitionAssignmentUtils.setPartitionAssignment(idealState, Map.of());
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), List.of("A", "B", "C"));
    assertEquals(ReplicateModeAssignmentUtils.getLeaders(idealState), Map.of(0, "B", 10000, "C"));
  }

  @Test
  public void testNoAssignmentAndSingleReplica() {
    IdealState idealState = new IdealState("testTable_REALTIME");
    assertNull(ReplicateModeAssignmentUtils.getLeader(idealState, 0));
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), List.of());
    assertEquals(ReplicateModeAssignmentUtils.getFollowers(idealState, 0), List.of());
    assertEquals(ReplicateModeAssignmentUtils.getLeaders(idealState), Map.of());
    ReplicateModeAssignmentUtils.setAssignment(idealState, 0, "A", List.of("A"));
    assertEquals(ReplicateModeAssignmentUtils.getFollowers(idealState, 0), List.of());
  }

  @Test
  public void testInvalidAssignmentDoesNotOverwriteExistingRoles() {
    IdealState idealState = new IdealState("testTable_REALTIME");
    ReplicateModeAssignmentUtils.setAssignment(idealState, 0, "A", List.of("A", "B", "C"));
    expectThrows(IllegalArgumentException.class,
        () -> ReplicateModeAssignmentUtils.setAssignment(idealState, 0, "D", List.of("A", "B", "C")));
    expectThrows(IllegalArgumentException.class,
        () -> ReplicateModeAssignmentUtils.setAssignment(idealState, 0, "B", List.of("A", "B", "B")));
    assertEquals(ReplicateModeAssignmentUtils.getLeader(idealState, 0), "A");
    assertEquals(ReplicateModeAssignmentUtils.getReplicas(idealState, 0), List.of("A", "B", "C"));
  }
}
