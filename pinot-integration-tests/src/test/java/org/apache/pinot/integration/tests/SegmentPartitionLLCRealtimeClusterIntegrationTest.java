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
import org.apache.pinot.common.metadata.segment.ColumnPartitionMetadata;
import org.apache.pinot.common.metadata.segment.RealtimeSegmentZKMetadata;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that enables segment partition for the LLC real-time table.
 */
public class SegmentPartitionLLCRealtimeClusterIntegrationTest extends BaseClusterIntegrationTest {
  private static final String PARTITION_COLUMN = "Carrier";
  // Number of documents in the first and second Avro file
  private static final long NUM_DOCS_IN_FIRST_AVRO_FILE = 9292;
  private static final long NUM_DOCS_IN_SECOND_AVRO_FILE = 8736;

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
    Schema schema = new Schema.SchemaBuilder().setSchemaName(getSchemaName())
        .addSingleValueDimension(PARTITION_COLUMN, DataType.STRING)
        .addDateTime("DaysSinceEpoch", DataType.INT, "1:DAYS:EPOCH", "1:DAYS").build();
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(_avroFiles.get(0));
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setSegmentPartitionConfig(
        new SegmentPartitionConfig(Collections.singletonMap(PARTITION_COLUMN, new ColumnPartitionConfig("murmur", 5))));
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

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Nullable
  @Override
  protected String getPartitionColumn() {
    return _partitionColumn;
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
    int[] numCompletedSegmentsForPartition = new int[2];
    List<RealtimeSegmentZKMetadata> segmentZKMetadataList =
        _helixResourceManager.getRealtimeSegmentMetadata(getTableName());
    for (RealtimeSegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      if (segmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
        // For consuming segment, there should be no partition metadata
        assertNull(segmentZKMetadata.getPartitionMetadata());
      } else {
        // Completed segment

        SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
        assertNotNull(segmentPartitionMetadata);
        Map<String, ColumnPartitionMetadata> columnPartitionMetadataMap =
            segmentPartitionMetadata.getColumnPartitionMap();
        assertEquals(columnPartitionMetadataMap.size(), 1);
        ColumnPartitionMetadata columnPartitionMetadata = columnPartitionMetadataMap.get(PARTITION_COLUMN);
        assertNotNull(columnPartitionMetadata);

        // The function name should be aligned with the partition config in the table config
        assertTrue(columnPartitionMetadata.getFunctionName().equalsIgnoreCase("murmur"));

        // Number of partitions should be the same as number of stream partitions
        assertEquals(columnPartitionMetadata.getNumPartitions(), 2);

        // Should contain only one partition, which is the same as the stream partition
        int streamPartition = new LLCSegmentName(segmentZKMetadata.getSegmentName()).getPartitionId();
        assertEquals(columnPartitionMetadata.getPartitions(), Collections.singleton(streamPartition));

        numCompletedSegmentsForPartition[streamPartition]++;
      }
    }

    // There should be 0 completed segments for partition 0, 2 completed segments for partition 1
    assertEquals(numCompletedSegmentsForPartition[0], 0);
    assertEquals(numCompletedSegmentsForPartition[1], 2);
  }

  @Test(dependsOnMethods = "testPartitionMetadata")
  public void testPartitionRouting()
      throws Exception {
    // Query partition 0
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE Carrier = 'UA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE Carrier BETWEEN 'UA' AND 'UA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should only query the consuming segment for both partition 0 and partition 1
      assertEquals(response.get(DataTable.NUM_SEGMENTS_QUERIED).asInt(), 2);
      assertEquals(responseToCompare.get(DataTable.NUM_SEGMENTS_QUERIED).asInt(), 4);

      assertEquals(response.get("aggregationResults").get(0).get("value").asInt(),
          responseToCompare.get("aggregationResults").get(0).get("value").asInt());
    }

    // Query partition 1
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE Carrier = 'AA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE Carrier BETWEEN 'AA' AND 'AA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should query all the segments
      assertEquals(response.get(DataTable.NUM_SEGMENTS_QUERIED).asInt(), 4);
      assertEquals(responseToCompare.get(DataTable.NUM_SEGMENTS_QUERIED).asInt(), 4);

      assertEquals(response.get("aggregationResults").get(0).get("value").asInt(),
          responseToCompare.get("aggregationResults").get(0).get("value").asInt());
    }
  }

  @Test(dependsOnMethods = "testPartitionRouting")
  public void testNonPartitionedStream()
      throws Exception {
    // Push the second Avro file into Kafka without partitioning
    _partitionColumn = null;
    pushAvroIntoKafka(Collections.singletonList(_avroFiles.get(1)));

    // Wait for all documents loaded
    _countStarResult = NUM_DOCS_IN_FIRST_AVRO_FILE + NUM_DOCS_IN_SECOND_AVRO_FILE;
    waitForAllDocsLoaded(600_000L);

    // Check partition metadata
    List<RealtimeSegmentZKMetadata> segmentZKMetadataList =
        _helixResourceManager.getRealtimeSegmentMetadata(getTableName());
    for (RealtimeSegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      if (segmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
        // For consuming segment, there should be no partition metadata
        assertNull(segmentZKMetadata.getPartitionMetadata());
      } else {
        // Completed segment

        SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
        assertNotNull(segmentPartitionMetadata);
        Map<String, ColumnPartitionMetadata> columnPartitionMetadataMap =
            segmentPartitionMetadata.getColumnPartitionMap();
        assertEquals(columnPartitionMetadataMap.size(), 1);
        ColumnPartitionMetadata columnPartitionMetadata = columnPartitionMetadataMap.get(PARTITION_COLUMN);
        assertNotNull(columnPartitionMetadata);

        // The function name should be aligned with the partition config in the table config
        assertTrue(columnPartitionMetadata.getFunctionName().equalsIgnoreCase("murmur"));

        // Number of partitions should be the same as number of stream partitions
        assertEquals(columnPartitionMetadata.getNumPartitions(), 2);

        // The partition metadata for the new completed segments should contain both partitions
        LLCSegmentName llcSegmentName = new LLCSegmentName(segmentZKMetadata.getSegmentName());
        int streamPartition = llcSegmentName.getPartitionId();
        int sequenceNumber = llcSegmentName.getSequenceNumber();
        if (streamPartition == 0 || (streamPartition == 1 && sequenceNumber >= 2)) {
          assertEquals(columnPartitionMetadata.getPartitions(), new HashSet<>(Arrays.asList(0, 1)));
        }
      }
    }

    // Check partition routing
    int numSegments = segmentZKMetadataList.size();

    // Query partition 0
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE Carrier = 'UA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE Carrier BETWEEN 'UA' AND 'UA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should skip the first 2 completed segments for partition 1
      assertEquals(response.get(DataTable.NUM_SEGMENTS_QUERIED).asInt(), numSegments - 2);
      assertEquals(responseToCompare.get(DataTable.NUM_SEGMENTS_QUERIED).asInt(), numSegments);

      assertEquals(response.get("aggregationResults").get(0).get("value").asInt(),
          responseToCompare.get("aggregationResults").get(0).get("value").asInt());
    }

    // Query partition 1
    {
      String query = "SELECT COUNT(*) FROM mytable WHERE Carrier = 'AA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM mytable WHERE Carrier BETWEEN 'AA' AND 'AA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should query all the segments
      assertEquals(response.get(DataTable.NUM_SEGMENTS_QUERIED).asInt(), numSegments);
      assertEquals(responseToCompare.get(DataTable.NUM_SEGMENTS_QUERIED).asInt(), numSegments);

      assertEquals(response.get("aggregationResults").get(0).get("value").asInt(),
          responseToCompare.get("aggregationResults").get(0).get("value").asInt());
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
