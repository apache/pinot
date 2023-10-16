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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that enables segment partition for the LLC real-time table.
 */
public class SegmentPartitionLLCRealtimeClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final String PARTITION_COLUMN = "DestState";
  // Number of documents in the first and second Avro file
  private static final long NUM_DOCS_IN_FIRST_AVRO_FILE = 9292;
  private static final long NUM_DOCS_IN_SECOND_AVRO_FILE = 8736;
  private static final long NUM_DOCS_IN_THIRD_AVRO_FILE = 9378;

  private List<File> _avroFiles;
  private String _partitionColumn;
  private long _countStarResult;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    _avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config with reduced number of columns and partition config
    Schema schema = new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(PARTITION_COLUMN, DataType.STRING)
        .addDateTime("DaysSinceEpoch", DataType.INT, "1:DAYS:EPOCH", "1:DAYS").build();
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setSegmentPartitionConfig(new SegmentPartitionConfig(
        Collections.singletonMap(PARTITION_COLUMN, new ColumnPartitionConfig("murmur", 2))));
    tableConfig.setRoutingConfig(
        new RoutingConfig(null, Collections.singletonList(RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE), null));
    addTableConfig(tableConfig);

    // Push data into Kafka (only ingest the first Avro file)
    _partitionColumn = PARTITION_COLUMN;
    pushAvroIntoKafka(Collections.singletonList(_avroFiles.get(0)));

    // Wait for all documents loaded
    _countStarResult = NUM_DOCS_IN_FIRST_AVRO_FILE;
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected long getCountStarResult() {
    return _countStarResult;
  }

  @Nullable
  @Override
  protected String getPartitionColumn() {
    return _partitionColumn;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @Test
  public void testPartitionMetadata() {
    int[] numSegmentsForPartition = new int[2];
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<SegmentZKMetadata> segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(realtimeTableName);
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
      assertNotNull(segmentPartitionMetadata);
      Map<String, ColumnPartitionMetadata> columnPartitionMetadataMap =
          segmentPartitionMetadata.getColumnPartitionMap();
      assertEquals(columnPartitionMetadataMap.size(), 1);
      ColumnPartitionMetadata columnPartitionMetadata = columnPartitionMetadataMap.get(PARTITION_COLUMN);
      assertNotNull(columnPartitionMetadata);
      assertTrue(columnPartitionMetadata.getFunctionName().equalsIgnoreCase("murmur"));
      assertEquals(columnPartitionMetadata.getNumPartitions(), 2);
      int partitionGroupId = new LLCSegmentName(segmentZKMetadata.getSegmentName()).getPartitionGroupId();
      assertEquals(columnPartitionMetadata.getPartitions(), Collections.singleton(partitionGroupId));
      numSegmentsForPartition[partitionGroupId]++;
    }

    // There should be 2 segments for partition 0, 2 segments for partition 1
    assertEquals(numSegmentsForPartition[0], 2);
    assertEquals(numSegmentsForPartition[1], 2);
  }

  @Test(dependsOnMethods = "testPartitionMetadata")
  public void testPartitionRouting()
      throws Exception {
    // Query partition 0
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE DestState = 'CA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE DestState BETWEEN 'CA' AND 'CA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should only query the segments for partition 0
      assertEquals(response.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), 2);
      assertEquals(responseToCompare.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), 4);

      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(),
          responseToCompare.get("resultTable").get("rows").get(0).get(0).asInt());
    }

    // Query partition 1
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE DestState = 'FL'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE DestState BETWEEN 'FL' AND 'FL'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should only query the segments for partition 1
      assertEquals(response.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), 2);
      assertEquals(responseToCompare.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), 4);

      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(),
          responseToCompare.get("resultTable").get("rows").get(0).get(0).asInt());
    }
  }

  @Test(dependsOnMethods = "testPartitionRouting")
  public void testNonPartitionedStream()
      throws Exception {
    // Push the second Avro file into Kafka without partitioning
    _partitionColumn = null;
    pushAvroIntoKafka(Collections.singletonList(_avroFiles.get(1)));

    // Wait for all documents loaded
    _countStarResult += NUM_DOCS_IN_SECOND_AVRO_FILE;
    waitForAllDocsLoaded(600_000L);

    // Check partition metadata
    int[] numSegmentsForPartition = new int[2];
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<SegmentZKMetadata> segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(realtimeTableName);
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
      assertNotNull(segmentPartitionMetadata);
      Map<String, ColumnPartitionMetadata> columnPartitionMetadataMap =
          segmentPartitionMetadata.getColumnPartitionMap();
      assertEquals(columnPartitionMetadataMap.size(), 1);
      ColumnPartitionMetadata columnPartitionMetadata = columnPartitionMetadataMap.get(PARTITION_COLUMN);
      assertNotNull(columnPartitionMetadata);
      assertTrue(columnPartitionMetadata.getFunctionName().equalsIgnoreCase("murmur"));
      assertEquals(columnPartitionMetadata.getNumPartitions(), 2);
      int partitionGroupId = new LLCSegmentName(segmentZKMetadata.getSegmentName()).getPartitionGroupId();
      numSegmentsForPartition[partitionGroupId]++;

      if (segmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
        // For consuming segment, the partition metadata should only contain the stream partition
        assertEquals(columnPartitionMetadata.getPartitions(), Collections.singleton(partitionGroupId));
      } else {
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentZKMetadata.getSegmentName());
        int sequenceNumber = llcSegmentName.getSequenceNumber();
        if (sequenceNumber == 0) {
          // The partition metadata for the first completed segment should only contain the stream partition
          assertEquals(columnPartitionMetadata.getPartitions(), Collections.singleton(partitionGroupId));
        } else {
          // The partition metadata for the new completed segments should contain both partitions
          assertEquals(columnPartitionMetadata.getPartitions(), new HashSet<>(Arrays.asList(0, 1)));
        }
      }
    }

    // There should be 4 segments for partition 0, 4 segments for partition 1
    assertEquals(numSegmentsForPartition[0], 4);
    assertEquals(numSegmentsForPartition[1], 4);

    // Check partition routing
    int numSegments = segmentsZKMetadata.size();

    // Query partition 0
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE DestState = 'CA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE DestState BETWEEN 'CA' AND 'CA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should skip the first completed segments and the consuming segment for partition 1
      assertEquals(response.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), numSegments - 2);
      assertEquals(responseToCompare.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), numSegments);

      // The result won't match because the consuming segment for partition 1 is pruned out
    }

    // Query partition 1
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE DestState = 'FL'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE DestState BETWEEN 'FL' AND 'FL'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should skip the first completed segments and the consuming segment for partition 0
      assertEquals(response.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), numSegments - 2);
      assertEquals(responseToCompare.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), numSegments);

      // The result won't match because the consuming segment for partition 0 is pruned out
    }

    // Push the third Avro file into Kafka with partitioning
    _partitionColumn = PARTITION_COLUMN;
    pushAvroIntoKafka(Collections.singletonList(_avroFiles.get(2)));

    // Wait for all documents loaded
    _countStarResult += NUM_DOCS_IN_THIRD_AVRO_FILE;
    waitForAllDocsLoaded(600_000L);

    // Check partition metadata
    numSegmentsForPartition = new int[2];
    segmentsZKMetadata = _helixResourceManager.getSegmentsZKMetadata(realtimeTableName);
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
      assertNotNull(segmentPartitionMetadata);
      Map<String, ColumnPartitionMetadata> columnPartitionMetadataMap =
          segmentPartitionMetadata.getColumnPartitionMap();
      assertEquals(columnPartitionMetadataMap.size(), 1);
      ColumnPartitionMetadata columnPartitionMetadata = columnPartitionMetadataMap.get(PARTITION_COLUMN);
      assertNotNull(columnPartitionMetadata);
      assertTrue(columnPartitionMetadata.getFunctionName().equalsIgnoreCase("murmur"));
      assertEquals(columnPartitionMetadata.getNumPartitions(), 2);
      int partitionGroupId = new LLCSegmentName(segmentZKMetadata.getSegmentName()).getPartitionGroupId();
      numSegmentsForPartition[partitionGroupId]++;

      if (segmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
        // For consuming segment, the partition metadata should only contain the stream partition
        assertEquals(columnPartitionMetadata.getPartitions(), Collections.singleton(partitionGroupId));
      } else {
        // The partition metadata for the new completed segments should only contain the stream partition
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentZKMetadata.getSegmentName());
        int sequenceNumber = llcSegmentName.getSequenceNumber();
        if (sequenceNumber == 0 || sequenceNumber >= 4) {
          // The partition metadata for the first and new completed segments should only contain the stream partition
          assertEquals(columnPartitionMetadata.getPartitions(), Collections.singleton(partitionGroupId));
        } else {
          // The partition metadata for the completed segments containing records from the second Avro file should
          // contain both partitions
          assertEquals(columnPartitionMetadata.getPartitions(), new HashSet<>(Arrays.asList(0, 1)));
        }
      }
    }

    // There should be 6 segments for partition 0, 6 segments for partition 1
    assertEquals(numSegmentsForPartition[0], 6);
    assertEquals(numSegmentsForPartition[1], 6);

    // Check partition routing
    numSegments = segmentsZKMetadata.size();

    // Query partition 0
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE DestState = 'CA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE DestState BETWEEN 'CA' AND 'CA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should skip 2 completed segments and the consuming segment for partition 1
      assertEquals(response.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), numSegments - 3);
      assertEquals(responseToCompare.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), numSegments);

      // The result should match again after all the segments with the non-partitioning records are committed
      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(),
          responseToCompare.get("resultTable").get("rows").get(0).get(0).asInt());
    }

    // Query partition 1
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE DestState = 'FL'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE DestState BETWEEN 'FL' AND 'FL'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should skip 2 completed segments and the consuming segment for partition 0
      assertEquals(response.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), numSegments - 3);
      assertEquals(responseToCompare.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(), numSegments);

      // The result should match again after all the segments with the non-partitioning records are committed
      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(),
          responseToCompare.get("resultTable").get("rows").get(0).get(0).asInt());
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
