/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.controller.helix.core.realtime;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.mockito.ArgumentMatcher;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class PinotLLCRealtimeSegmentManagerTest {
  private final String clusterName = "testCluster";
  private final String server1 = "Server_1";
  private final String server2 = "Server_2";
  private final String server3 = "Server_3";

  private HelixManager createMockHelixManager() {
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getClusterName()).thenReturn(clusterName);
    return  helixManager;
  }

  private HelixAdmin createMockHelixAdmin() {
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    return helixAdmin;
  }

  private ZkHelixPropertyStore createMockPropertyStore() {
    ZkHelixPropertyStore propertyStore = mock(ZkHelixPropertyStore.class);
    return propertyStore;
  }

  private PinotHelixResourceManager createMockHelixResourceManager() {
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    return helixResourceManager;
  }


  @Test
  public void testKafkaAssignment() throws Exception {
    final HelixAdmin helixAdmin = createMockHelixAdmin();
    final HelixManager helixManager = createMockHelixManager();
    final PinotHelixResourceManager helixResourceManager = createMockHelixResourceManager();
    final ZkHelixPropertyStore propertyStore = createMockPropertyStore();

    PinotLLCRealtimeSegmentManager.create(helixAdmin, helixManager, propertyStore, helixResourceManager);
    PinotLLCRealtimeSegmentManager segmentManager = PinotLLCRealtimeSegmentManager.getInstance();

    final String topic = "someTopic";
    final String rtTableName = "table_REALTIME";
    final int nPartitions = 4;
    final int nReplicas = 2;
    String[] instances = {server1, server2, server3};

    // Populate 'partitionSet' with all kafka partitions,
    // As we find partitions in the assigment, we will remove the partition from this set.
    Set<Integer> partitionSet = new HashSet<>(nPartitions);
    for (int i = 0; i < nPartitions; i++) {
      partitionSet.add(i);
    }

    ZNRecord znRecord = segmentManager.assignKafkaPartitions(topic, nPartitions, Arrays.asList(instances), nReplicas);

    Map<String, List<String>> assignmentMap = znRecord.getListFields();
    Assert.assertEquals(assignmentMap.size(), nPartitions);
    // The map looks something like this:
    // {
    //  "0" : [S1, S2],
    //  "1" : [S2, S3],
    //  "2" : [S3, S4],
    // }
    // Walk through the map, making sure that every partition (and no more) appears in the key, and
    // every one of them has exactly as many elements as the number of replicas.
    for (Map.Entry<String, List<String>> entry : assignmentMap.entrySet()) {
      int p = Integer.valueOf(entry.getKey());
      Assert.assertTrue(partitionSet.contains(p));
      partitionSet.remove(p);
      Assert.assertEquals(entry.getValue().size(), nReplicas);
      // Make sure that we have unique server entries in the list for that partition
      Set allServers = allServers();
      for (String server : entry.getValue()) {
        Assert.assertTrue(allServers.contains(server));
        allServers.remove(server);
      }
      // allServers may not be empty here.
    }
    Assert.assertTrue(partitionSet.isEmpty());    // We should have no more partitions left.
    segmentManager.writeKafkaPartitionAssignemnt(rtTableName, znRecord);
    verify(propertyStore).set(eq("KAFKA_PARTITIONS/" + rtTableName), argThat(new ZNRecordMatcher(znRecord)),  eq(AccessOption.PERSISTENT));
  }

  private Set allServers() {
    Set<String> result = new HashSet<>(3);
    result.add(server1);
    result.add(server2);
    result.add(server3);
    return result;
  }

  class ZNRecordMatcher extends ArgumentMatcher {
    private final ZNRecord _znRecord;
    public ZNRecordMatcher(final ZNRecord znRecord) {
      _znRecord = znRecord;
    }
    @Override
    public boolean matches(Object o) {
      if (o == _znRecord) {
        return true;
      }
      return false;
    }
  }
}
