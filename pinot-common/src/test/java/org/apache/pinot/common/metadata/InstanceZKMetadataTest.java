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
package org.apache.pinot.common.metadata;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.testng.Assert;
import org.testng.annotations.Test;


public class InstanceZKMetadataTest {

  @Test
  public void instanceZKMetadataConversionTest() {
    ZNRecord znRecord = getTestInstanceZNRecord();
    InstanceZKMetadata instanceMetadataFromZNRecord = new InstanceZKMetadata(znRecord);

    InstanceZKMetadata instanceMetadata = getTestInstanceMetadata();
    ZNRecord znRecordFromMetadata = instanceMetadata.toZNRecord();

    Assert.assertTrue(MetadataUtils.comparisonZNRecords(znRecord, znRecordFromMetadata));
    Assert.assertTrue(instanceMetadata.equals(instanceMetadataFromZNRecord));

    Assert.assertTrue(instanceMetadata.equals(new InstanceZKMetadata(instanceMetadata.toZNRecord())));
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(znRecord, new InstanceZKMetadata(znRecord).toZNRecord()));
  }

  private ZNRecord getTestInstanceZNRecord() {
    ZNRecord record = new ZNRecord("Server_localhost_1234");
    Map<String, String> groupIdMap = new HashMap<>();
    Map<String, String> partitionMap = new HashMap<>();

    for (int i = 0; i < 10; i++) {
      groupIdMap.put("testRes" + i + "_REALTIME", "groupId" + i);
      partitionMap.put("testRes" + i + "_REALTIME", "part" + i);
    }
    record.setMapField("KAFKA_HLC_GROUP_MAP", groupIdMap);
    record.setMapField("KAFKA_HLC_PARTITION_MAP", partitionMap);
    return record;
  }

  private InstanceZKMetadata getTestInstanceMetadata() {
    InstanceZKMetadata instanceMetadata = new InstanceZKMetadata();
    instanceMetadata.setInstanceType("Server");
    instanceMetadata.setInstanceName("localhost");
    instanceMetadata.setInstancePort(1234);
    for (int i = 0; i < 10; i++) {
      instanceMetadata.setGroupId("testRes" + i, "groupId" + i);
      instanceMetadata.setPartition("testRes" + i, "part" + i);
    }
    return instanceMetadata;
  }
}
