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
package org.apache.pinot.broker.routing.instanceselector;

import java.util.List;
import java.util.Map;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class BaseInstanceSelectorTest {

  @Test
  public void testServerToReplicaGroupMap() {
    String ipName = InstancePartitionsUtils.getInstancePartitionsName("testTable_OFFLINE", "OFFLINE");
    ZNRecord znRecord = new ZNRecord("testTable_OFFLINE");
    // Keys:
    // 0_0 -> A,B
    // 0_1 -> C
    // 1_0 -> B
    // 1_1 -> A
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + ipName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_0", List.of("A", "B"));
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + ipName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "0_1", List.of("C", "D"));
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + ipName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "1_0", List.of("E", "F"));
    znRecord.setListField(InstancePartitionsUtils.IDEAL_STATE_IP_PREFIX + ipName
        + InstancePartitionsUtils.IDEAL_STATE_IP_SEPARATOR + "1_1", List.of("G", "H"));
    IdealState is = new IdealState(znRecord);

    Map<String, Integer> map = BaseInstanceSelector.serverToReplicaGroupMap(is);
    // Each server should map to its replica group id
    assertEquals(map.get("A").intValue(), 0);
    assertEquals(map.get("B").intValue(), 0);
    assertEquals(map.get("C").intValue(), 1);
    assertEquals(map.get("D").intValue(), 1);
    assertEquals(map.get("E").intValue(), 0);
    assertEquals(map.get("F").intValue(), 0);
    assertEquals(map.get("G").intValue(), 1);
    assertEquals(map.get("H").intValue(), 1);
  }
}
