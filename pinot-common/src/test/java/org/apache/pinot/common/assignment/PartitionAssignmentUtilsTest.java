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
import java.util.Set;
import org.apache.helix.model.IdealState;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class PartitionAssignmentUtilsTest {

  @Test
  public void testPartitionAssignment() {
    IdealState idealState = new IdealState("testTable_REALTIME");
    // List fields that are not partition entries are left alone
    idealState.getRecord().setListField("other", List.of("a", "b"));
    assertNull(PartitionAssignmentUtils.getInstances(idealState, 0));
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState), Map.of());

    // Entries are stored sorted
    assertEquals(PartitionAssignmentUtils.setInstances(idealState, 1, List.of("server_2", "server_0", "server_1")),
        List.of("server_0", "server_1", "server_2"));
    PartitionAssignmentUtils.setInstances(idealState, 0, List.of("server_3"));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 1), List.of("server_0", "server_1", "server_2"));
    assertEquals(PartitionAssignmentUtils.getInstances(idealState, 0), List.of("server_3"));
    assertNull(PartitionAssignmentUtils.getInstances(idealState, 2));
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState),
        Map.of(0, List.of("server_3"), 1, List.of("server_0", "server_1", "server_2")));
    assertEquals(PartitionAssignmentUtils.getAllInstances(idealState),
        Set.of("server_0", "server_1", "server_2", "server_3"));

    // Replacing the partition assignment drops the entries of the partitions not in the new one
    PartitionAssignmentUtils.setPartitionAssignment(idealState,
        Map.of(1, List.of("server_5", "server_4"), 10, List.of("server_6")));
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState),
        Map.of(1, List.of("server_4", "server_5"), 10, List.of("server_6")));
    assertNull(PartitionAssignmentUtils.getInstances(idealState, 0));
    assertEquals(idealState.getRecord().getListField("other"), List.of("a", "b"));

    PartitionAssignmentUtils.setPartitionAssignment(idealState, Map.of());
    assertEquals(PartitionAssignmentUtils.getPartitionAssignment(idealState), Map.of());
    assertEquals(idealState.getRecord().getListFields(), Map.of("other", List.of("a", "b")));
  }
}
