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
package org.apache.pinot.broker.routing.segmentpruner;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.partition.function.CustomPartitionFunction;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests broker-side pruning for [CustomPartitionFunction] with segment partition metadata encoded in the broker
/// metadata format.
public class CustomPartitionFunctionSegmentPrunerTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";
  private static final String PARTITION_COLUMN = "memberId";
  private static final int NUM_PARTITIONS = 5;
  private static final Map<String, String> FUNCTION_CONFIG =
      Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, "plus(memberId,0)");

  @Test
  public void testPruningUsesCustomPartitionFunction()
      throws IOException {
    BrokerRequest noFilterBrokerRequest = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable");
    BrokerRequest prunedBrokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable WHERE memberId = 0");
    BrokerRequest singlePartitionBrokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable WHERE memberId = 7");
    BrokerRequest multiPartitionBrokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable WHERE memberId IN (7, 8)");

    SinglePartitionColumnSegmentPruner segmentPruner =
        new SinglePartitionColumnSegmentPruner(TABLE_NAME_WITH_TYPE, PARTITION_COLUMN);
    String segmentWithPartition2 = "segmentWithPartition2";
    String segmentWithPartition3 = "segmentWithPartition3";
    segmentPruner.init(Mockito.mock(IdealState.class), Mockito.mock(ExternalView.class),
        List.of(segmentWithPartition2, segmentWithPartition3),
        List.of(createPartitionMetadataRecord(segmentWithPartition2, 2, FUNCTION_CONFIG),
            createPartitionMetadataRecord(segmentWithPartition3, 3, FUNCTION_CONFIG)));

    Set<String> segments = Set.of(segmentWithPartition2, segmentWithPartition3);
    assertEquals(segmentPruner.prune(noFilterBrokerRequest, segments), segments);
    assertEquals(segmentPruner.prune(prunedBrokerRequest, segments), Set.of());
    assertEquals(segmentPruner.prune(singlePartitionBrokerRequest, segments), Set.of(segmentWithPartition2));
    assertEquals(segmentPruner.prune(multiPartitionBrokerRequest, segments), segments);
  }

  @Test
  public void testInvalidCustomPartitionIdDoesNotPrune()
      throws IOException {
    BrokerRequest invalidPartitionIdBrokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable WHERE memberId = 'not-a-number'");

    SinglePartitionColumnSegmentPruner segmentPruner =
        new SinglePartitionColumnSegmentPruner(TABLE_NAME_WITH_TYPE, PARTITION_COLUMN);
    String segmentWithPartition2 = "segmentWithPartition2";
    String segmentWithPartition3 = "segmentWithPartition3";
    Map<String, String> functionConfig =
        Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, "plus(memberId,0)");
    segmentPruner.init(Mockito.mock(IdealState.class), Mockito.mock(ExternalView.class),
        List.of(segmentWithPartition2, segmentWithPartition3),
        List.of(createPartitionMetadataRecord(segmentWithPartition2, 2, functionConfig),
            createPartitionMetadataRecord(segmentWithPartition3, 3, functionConfig)));

    Set<String> segments = Set.of(segmentWithPartition2, segmentWithPartition3);
    assertEquals(segmentPruner.prune(invalidPartitionIdBrokerRequest, segments), segments);
  }

  @Test
  public void testOutOfRangeCustomPartitionIdDoesNotPrune()
      throws IOException {
    BrokerRequest outOfRangePartitionIdBrokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable WHERE memberId = 99");

    SinglePartitionColumnSegmentPruner singleColumnPruner =
        new SinglePartitionColumnSegmentPruner(TABLE_NAME_WITH_TYPE, PARTITION_COLUMN);
    MultiPartitionColumnsSegmentPruner multiColumnPruner =
        new MultiPartitionColumnsSegmentPruner(TABLE_NAME_WITH_TYPE, Set.of(PARTITION_COLUMN, "accountId"));
    String segmentWithPartition2 = "segmentWithPartition2";
    Map<String, String> functionConfig =
        Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, "plus(memberId,0)", "partitionIdNormalizer",
            "NO_OP");
    ZNRecord znRecord = createPartitionMetadataRecord(segmentWithPartition2, 2, functionConfig);
    singleColumnPruner.init(Mockito.mock(IdealState.class), Mockito.mock(ExternalView.class),
        List.of(segmentWithPartition2), List.of(znRecord));
    multiColumnPruner.init(Mockito.mock(IdealState.class), Mockito.mock(ExternalView.class),
        List.of(segmentWithPartition2), List.of(znRecord));

    Set<String> segments = Set.of(segmentWithPartition2);
    assertEquals(singleColumnPruner.prune(outOfRangePartitionIdBrokerRequest, segments), segments);
    assertEquals(multiColumnPruner.prune(outOfRangePartitionIdBrokerRequest, segments), segments);
  }

  private static ZNRecord createPartitionMetadataRecord(String segmentName, int partitionId,
      Map<String, String> functionConfig)
      throws IOException {
    SegmentPartitionMetadata segmentPartitionMetadata = new SegmentPartitionMetadata(
        Map.of(PARTITION_COLUMN,
            new ColumnPartitionMetadata(CustomPartitionFunction.NAME, NUM_PARTITIONS, Set.of(partitionId),
                functionConfig), "accountId", new ColumnPartitionMetadata("Modulo", NUM_PARTITIONS, Set.of(0), null)));
    ZNRecord znRecord = new ZNRecord(segmentName);
    znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, segmentPartitionMetadata.toJsonString());
    return znRecord;
  }
}
