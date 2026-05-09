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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionPipelineFunction;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class PartitionFunctionExprSegmentPrunerTest {
  private static final String TABLE_NAME_WITH_TYPE = "testTable_OFFLINE";
  private static final String PARTITION_COLUMN = "memberId";
  private static final String UUID_PARTITION_COLUMN = "id";

  @Test
  public void testSinglePartitionColumnPrunerWithFunctionExpr()
      throws Exception {
    SinglePartitionColumnSegmentPruner pruner =
        new SinglePartitionColumnSegmentPruner(TABLE_NAME_WITH_TYPE, PARTITION_COLUMN);
    String functionExpr = "murmur2(lower(memberId))";
    PartitionFunction partitionFunction =
        PartitionFunctionFactory.getPartitionFunction(PARTITION_COLUMN, null, 8, null, functionExpr);
    int matchingPartition = partitionFunction.getPartition("Pinot");
    String firstNonMatchingValue = findValueWithDifferentPartition(partitionFunction, matchingPartition, "Kafka",
        "Trino", "StarTree", "Presto", "Druid");
    String secondNonMatchingValue = findValueWithDifferentPartition(partitionFunction, matchingPartition, "Flink",
        "Spark", "Hive", "Superset", "PinotDB");
    String segment = "segmentExpr";
    pruner.refreshSegment(segment, createPartitionMetadataRecord(segment, PARTITION_COLUMN,
        new ColumnPartitionMetadata(partitionFunction, Set.of(matchingPartition))));

    Set<String> input = Set.of(segment);
    BrokerRequest eqMatch = CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable WHERE memberId = "
        + "'Pinot'");
    BrokerRequest eqMiss = CalciteSqlCompiler.compileToBrokerRequest(
        String.format("SELECT * FROM testTable WHERE memberId = '%s'", firstNonMatchingValue));
    BrokerRequest inMatch = CalciteSqlCompiler.compileToBrokerRequest(
        String.format("SELECT * FROM testTable WHERE memberId IN ('%s', 'PiNoT')", firstNonMatchingValue));
    BrokerRequest inMiss = CalciteSqlCompiler.compileToBrokerRequest(
        String.format("SELECT * FROM testTable WHERE memberId IN ('%s', '%s')", firstNonMatchingValue,
            secondNonMatchingValue));

    assertEquals(pruner.prune(eqMatch, input), input);
    assertEquals(pruner.prune(eqMiss, input), Set.of());
    assertEquals(pruner.prune(inMatch, input), input);
    assertEquals(pruner.prune(inMiss, input), Set.of());
  }

  @Test
  public void testSinglePartitionColumnPrunerWithMd5FnvFunctionExpr()
      throws Exception {
    SinglePartitionColumnSegmentPruner pruner =
        new SinglePartitionColumnSegmentPruner(TABLE_NAME_WITH_TYPE, UUID_PARTITION_COLUMN);
    String functionExpr = "fnv1a_32(md5(id))";
    String matchingValue = "000016be-9d72-466c-9632-cfa680dc8fa3";
    PartitionFunction partitionFunction =
        PartitionFunctionFactory.getPartitionFunction(UUID_PARTITION_COLUMN, null, 128, null, functionExpr, "MASK");
    int matchingPartition = partitionFunction.getPartition(matchingValue);
    String nonMatchingValue = findValueWithDifferentPartition(partitionFunction, matchingPartition,
        "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002",
        "00000000-0000-0000-0000-000000000003");
    String segment = "segmentMd5FnvExpr";
    pruner.refreshSegment(segment, createPartitionMetadataRecord(segment, UUID_PARTITION_COLUMN,
        new ColumnPartitionMetadata(partitionFunction, Set.of(matchingPartition))));

    Set<String> input = Set.of(segment);
    BrokerRequest eqMatch = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT * FROM testTable WHERE id = '000016be-9d72-466c-9632-cfa680dc8fa3'");
    BrokerRequest eqMiss = CalciteSqlCompiler.compileToBrokerRequest(
        String.format("SELECT * FROM testTable WHERE id = '%s'", nonMatchingValue));

    assertEquals(matchingPartition, 104);
    assertEquals(pruner.prune(eqMatch, input), input);
    assertEquals(pruner.prune(eqMiss, input), Set.of());
  }

  @Test
  public void testSinglePartitionColumnPrunerFailsOpenOnInvalidLiteral()
      throws Exception {
    String partitionColumn = "eventTimeMillis";
    String segment = "segmentInvalidLiteralExpr";
    SinglePartitionColumnSegmentPruner pruner =
        new SinglePartitionColumnSegmentPruner(TABLE_NAME_WITH_TYPE, partitionColumn);
    PartitionFunction partitionFunction =
        PartitionFunctionFactory.getPartitionFunction(partitionColumn, null, 128, null,
            "intDiv(eventTimeMillis, 1000)");
    pruner.refreshSegment(segment, createPartitionMetadataRecord(segment, partitionColumn,
        new ColumnPartitionMetadata(partitionFunction, Set.of(54))));

    Set<String> input = Set.of(segment);
    BrokerRequest eqInvalid = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT * FROM testTable WHERE eventTimeMillis = 'not_a_number'");
    BrokerRequest inInvalid = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT * FROM testTable WHERE eventTimeMillis IN ('still_bad', 'not_a_number')");

    assertEquals(pruner.prune(eqInvalid, input), input);
    assertEquals(pruner.prune(inInvalid, input), input);
  }

  @Test
  public void testSinglePartitionColumnPrunerTreatsInvalidPartitionMetadataAsUnprunable()
      throws Exception {
    String segment = "segmentInvalidExprMetadata";
    SinglePartitionColumnSegmentPruner pruner =
        new SinglePartitionColumnSegmentPruner(TABLE_NAME_WITH_TYPE, PARTITION_COLUMN);
    pruner.refreshSegment(segment,
        createRawPartitionMetadataRecord(segment, PARTITION_COLUMN, PartitionPipelineFunction.NAME, 8, Set.of(1),
            "sha256(memberId)", null));

    Set<String> input = Set.of(segment);
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT * FROM testTable WHERE memberId = 'Pinot'");

    assertEquals(pruner.prune(brokerRequest, input), input);
  }

  private ZNRecord createPartitionMetadataRecord(String segmentName, String partitionColumn,
      ColumnPartitionMetadata columnPartitionMetadata)
      throws IOException {
    ZNRecord znRecord = new ZNRecord(segmentName);
    SegmentPartitionMetadata segmentPartitionMetadata =
        new SegmentPartitionMetadata(Map.of(partitionColumn, columnPartitionMetadata));
    znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, segmentPartitionMetadata.toJsonString());
    return znRecord;
  }

  private ZNRecord createRawPartitionMetadataRecord(String segmentName, String partitionColumn, String functionName,
      int numPartitions, Set<Integer> partitions, String functionExpr, String partitionIdNormalizer)
      throws IOException {
    ZNRecord znRecord = new ZNRecord(segmentName);
    Map<String, Object> columnPartitionMetadata = new HashMap<>();
    columnPartitionMetadata.put("functionName", functionName);
    columnPartitionMetadata.put("numPartitions", numPartitions);
    columnPartitionMetadata.put("partitions", partitions);
    columnPartitionMetadata.put("functionExpr", functionExpr);
    if (partitionIdNormalizer != null) {
      columnPartitionMetadata.put("partitionIdNormalizer", partitionIdNormalizer);
    }
    znRecord.setSimpleField(CommonConstants.Segment.PARTITION_METADATA, JsonUtils.objectToString(
        Map.of("columnPartitionMap", Map.of(partitionColumn, columnPartitionMetadata))));
    return znRecord;
  }

  private String findValueWithDifferentPartition(PartitionFunction partitionFunction, int partition, String... values) {
    for (String value : values) {
      if (partitionFunction.getPartition(value) != partition) {
        return value;
      }
    }
    throw new IllegalStateException("Failed to find value on a different partition");
  }
}
