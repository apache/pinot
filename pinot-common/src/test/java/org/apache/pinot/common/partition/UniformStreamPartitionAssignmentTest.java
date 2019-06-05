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
package org.apache.pinot.common.partition;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link UniformStreamPartitionAssignmentStrategy}
 */
public class UniformStreamPartitionAssignmentTest {

  /**
   * Tests that we validate necessary variables and honor the uniform sticky assignment
   * @throws InvalidConfigException
   */
  @Test
  public void testUniformStreamPartitionAssignmentStrategy()
      throws InvalidConfigException {
    UniformStreamPartitionAssignmentStrategy uniformStreamPartitionAssignmentStrategy =
        new UniformStreamPartitionAssignmentStrategy();

    String tableNameWithType = "tableName_REALTIME";
    List<String> partitions = Lists.newArrayList("0", "1", "2", "3", "4", "5");
    int numReplicas = 3;
    List<String> allTaggedInstances = Lists
        .newArrayList("server_1", "server_2", "server_3", "server_4", "server_5", "server_6", "server_7", "server_8");

    // num replicas more than tagged instances
    boolean exception = false;
    try {
      uniformStreamPartitionAssignmentStrategy
          .getStreamPartitionAssignment(null, tableNameWithType, partitions, 10, allTaggedInstances);
    } catch (InvalidConfigException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // 0 partitions
    PartitionAssignment uniformPartitionAssignment = uniformStreamPartitionAssignmentStrategy
        .getStreamPartitionAssignment(null, tableNameWithType, Collections.emptyList(), numReplicas,
            allTaggedInstances);
    Assert.assertEquals(uniformPartitionAssignment.getNumPartitions(), 0);

    // verify sticky uniform assignment
    uniformPartitionAssignment = uniformStreamPartitionAssignmentStrategy
        .getStreamPartitionAssignment(null, tableNameWithType, partitions, numReplicas, allTaggedInstances);

    List<String> instancesUsed = new ArrayList<>();
    for (int p = 0; p < uniformPartitionAssignment.getNumPartitions(); p++) {
      for (String instance : uniformPartitionAssignment.getInstancesListForPartition(String.valueOf(p))) {
        if (!instancesUsed.contains(instance)) {
          instancesUsed.add(instance);
        }
      }
    }
    Assert.assertTrue(instancesUsed.containsAll(allTaggedInstances));

    int serverIndex = 0;
    for (int p = 0; p < uniformPartitionAssignment.getNumPartitions(); p++) {
      List<String> instancesListForPartition =
          uniformPartitionAssignment.getInstancesListForPartition(String.valueOf(p));
      for (String instance : instancesListForPartition) {
        Assert.assertEquals(instance, instancesUsed.get(serverIndex++));
        if (serverIndex == instancesUsed.size()) {
          serverIndex = 0;
        }
      }
    }
  }
}
