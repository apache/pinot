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
package org.apache.pinot.broker.routing.segmentpartition;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.partition.function.CustomPartitionFunction;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.*;


/// Tests table-vs-segment partition function config matching in [SegmentPartitionMetadataManager].
public class SegmentPartitionMetadataManagerFunctionConfigTest {
  private static final String OFFLINE_TABLE_NAME = "testTable_OFFLINE";
  private static final String PARTITION_COLUMN = "memberId";
  private static final String PARTITION_COLUMN_FUNC = "Murmur";
  private static final int NUM_PARTITIONS = 2;
  private static final String SERVER_0 = "server0";

  @Test
  public void testBuiltInPartitionFunctionIgnoresEmptyConfigWhenSegmentMetadataHasNoConfig()
      throws IOException {
    SegmentPartitionMetadataManager partitionMetadataManager =
        new SegmentPartitionMetadataManager(OFFLINE_TABLE_NAME, PARTITION_COLUMN, PARTITION_COLUMN_FUNC,
            NUM_PARTITIONS, Collections.emptyMap());
    String segment = "segmentWithNullFunctionConfig";
    partitionMetadataManager.init(new IdealState(OFFLINE_TABLE_NAME), getExternalView(segment),
        Collections.singletonList(segment), Collections.singletonList(
            getSegmentZKMetadataRecord(segment, PARTITION_COLUMN_FUNC, NUM_PARTITIONS, 0, null)));

    TablePartitionReplicatedServersInfo tablePartitionReplicatedServersInfo = partitionMetadataManager
        .getTablePartitionReplicatedServersInfo();
    assertTrue(tablePartitionReplicatedServersInfo.getSegmentsWithInvalidPartition().isEmpty());
    TablePartitionReplicatedServersInfo.PartitionInfo[] partitionInfoMap = tablePartitionReplicatedServersInfo
        .getPartitionInfoMap();
    assertEquals(partitionInfoMap[0]._fullyReplicatedServers, Collections.singleton(SERVER_0));
    assertEquals(partitionInfoMap[0]._segments, Collections.singleton(segment));
  }

  @Test
  public void testCustomPartitionFunctionComparesFunctionConfig()
      throws IOException {
    Map<String, String> tableFunctionConfig =
        Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, "plus(memberId,0)");
    Map<String, String> segmentFunctionConfig =
        Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, "plus(memberId,1)");
    SegmentPartitionMetadataManager partitionMetadataManager =
        new SegmentPartitionMetadataManager(OFFLINE_TABLE_NAME, PARTITION_COLUMN, CustomPartitionFunction.NAME,
            NUM_PARTITIONS, tableFunctionConfig);
    String segment = "segmentWithDifferentCustomFunctionConfig";
    partitionMetadataManager.init(new IdealState(OFFLINE_TABLE_NAME), getExternalView(segment),
        Collections.singletonList(segment), Collections.singletonList(
            getSegmentZKMetadataRecord(segment, CustomPartitionFunction.NAME, NUM_PARTITIONS, 0,
                segmentFunctionConfig)));

    assertEquals(partitionMetadataManager.getTablePartitionReplicatedServersInfo().getSegmentsWithInvalidPartition(),
        Collections.singletonList(segment));
  }

  private static ExternalView getExternalView(String segment) {
    ExternalView externalView = new ExternalView(OFFLINE_TABLE_NAME);
    externalView.getRecord().getMapFields().put(segment, Collections.singletonMap(SERVER_0, ONLINE));
    return externalView;
  }

  private static ZNRecord getSegmentZKMetadataRecord(String segment, String partitionFunction, int numPartitions,
      int partitionId, Map<String, String> functionConfig)
      throws IOException {
    ZNRecord znRecord = new ZNRecord(segment);
    znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA,
        new SegmentPartitionMetadata(Collections.singletonMap(PARTITION_COLUMN,
            new ColumnPartitionMetadata(partitionFunction, numPartitions, Collections.singleton(partitionId),
                functionConfig))).toJsonString());
    return znRecord;
  }
}
