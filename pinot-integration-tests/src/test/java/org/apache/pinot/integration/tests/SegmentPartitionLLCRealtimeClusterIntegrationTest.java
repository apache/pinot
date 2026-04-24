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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
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
public class SegmentPartitionLLCRealtimeClusterIntegrationTest extends SharedRichClusterIntegrationTest {
  private static final String SHARED_TABLE_NAME = "segment_partition_llc_realtime";
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

    cleanupRealtimeTableAndSchema();
    resetKafkaTopic();

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
        new RoutingConfig(null, Collections.singletonList(RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE), null, false));
    addTableConfig(tableConfig);

    // Push data into Kafka (only ingest the first Avro file)
    _partitionColumn = PARTITION_COLUMN;
    pushAvroIntoKafka(Collections.singletonList(_avroFiles.get(0)));

    // Wait for all documents loaded
    _countStarResult = NUM_DOCS_IN_FIRST_AVRO_FILE;
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
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
    List<SegmentZKMetadata> segmentsZKMetadata = getSegmentsZKMetadata();
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

    assertSegmentsLoadedForBothPartitions(numSegmentsForPartition);
  }

  @Test(dependsOnMethods = "testPartitionMetadata")
  public void testPartitionRouting()
      throws Exception {
    List<SegmentZKMetadata> segmentsZKMetadata = getSegmentsZKMetadata();

    // Query partition 0
    {
      String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState = 'CA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState BETWEEN 'CA' AND 'CA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should only query the segments for partition 0
      assertPartitionRoutingSegmentCounts(response, responseToCompare, segmentsZKMetadata, 0);

      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(),
          responseToCompare.get("resultTable").get("rows").get(0).get(0).asInt());
    }

    // Query partition 1
    {
      String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState = 'FL'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState BETWEEN 'FL' AND 'FL'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should only query the segments for partition 1
      assertPartitionRoutingSegmentCounts(response, responseToCompare, segmentsZKMetadata, 1);

      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(),
          responseToCompare.get("resultTable").get("rows").get(0).get(0).asInt());
    }
  }

  @Test(dependsOnMethods = "testPartitionRouting")
  public void testPartitionIdVirtualColumn()
      throws Exception {
    // Query to get partition ID information for all segments
    String query =
        "SELECT $partitionId, $segmentName FROM " + getTableName() + " GROUP BY $segmentName, $partitionId";
    JsonNode response = postQuery(query);

    JsonNode resultTable = response.get("resultTable");
    JsonNode rows = resultTable.get("rows");

    assertTrue(rows.size() > 0, "Should have at least one segment result");

    // Define expected partition ID values
    Set<String> expectedPartitions = new HashSet<>();
    expectedPartitions.add("DestState_0");
    expectedPartitions.add("DestState_1");

    // Validate that $partitionId virtual column returns expected results
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      String partitionIdResult = row.get(0).asText();
      String segmentName = row.get(1).asText();

      assertNotNull(partitionIdResult);
      assertNotNull(segmentName);
      assertTrue(LLCSegmentName.isLLCSegment(segmentName));

      if (partitionIdResult.isBlank()) {
        continue;
      }
      // Validate that partitionIdResult is one of the expected partition IDs
      assertTrue(expectedPartitions.contains(partitionIdResult),
          "Expected one of " + expectedPartitions + " but found: " + partitionIdResult);
    }
  }

  @Test(dependsOnMethods = "testPartitionIdVirtualColumn")
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
    List<SegmentZKMetadata> segmentsZKMetadata = getSegmentsZKMetadata();
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

      assertValidPartitionMetadata(columnPartitionMetadata, partitionGroupId);
    }

    assertSegmentsLoadedForBothPartitions(numSegmentsForPartition);

    // Check partition routing

    // Query partition 0
    {
      String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState = 'CA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState BETWEEN 'CA' AND 'CA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should skip the first completed segments and the consuming segment for partition 1
      assertPartitionRoutingSegmentCounts(response, responseToCompare, segmentsZKMetadata, 0);

      // The result won't match because the consuming segment for partition 1 is pruned out
    }

    // Query partition 1
    {
      String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState = 'FL'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState BETWEEN 'FL' AND 'FL'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should skip the first completed segments and the consuming segment for partition 0
      assertPartitionRoutingSegmentCounts(response, responseToCompare, segmentsZKMetadata, 1);

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
    segmentsZKMetadata = getSegmentsZKMetadata();
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

      assertValidPartitionMetadata(columnPartitionMetadata, partitionGroupId);
    }

    assertSegmentsLoadedForBothPartitions(numSegmentsForPartition);

    // Check partition routing

    // Query partition 0
    {
      String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState = 'CA'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState BETWEEN 'CA' AND 'CA'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should skip 2 completed segments and the consuming segment for partition 1
      assertPartitionRoutingSegmentCounts(response, responseToCompare, segmentsZKMetadata, 0);

      // The result should match again after all the segments with the non-partitioning records are committed
      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(),
          responseToCompare.get("resultTable").get("rows").get(0).get(0).asInt());
    }

    // Query partition 1
    {
      String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState = 'FL'";
      JsonNode response = postQuery(query);

      String queryToCompare = "SELECT COUNT(*) FROM " + getTableName() + " WHERE DestState BETWEEN 'FL' AND 'FL'";
      JsonNode responseToCompare = postQuery(queryToCompare);

      // Should skip 2 completed segments and the consuming segment for partition 0
      assertPartitionRoutingSegmentCounts(response, responseToCompare, segmentsZKMetadata, 1);

      // The result should match again after all the segments with the non-partitioning records are committed
      assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(),
          responseToCompare.get("resultTable").get("rows").get(0).get(0).asInt());
    }
  }

  private List<SegmentZKMetadata> getSegmentsZKMetadata() {
    return _helixResourceManager.getSegmentsZKMetadata(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
  }

  private void assertSegmentsLoadedForBothPartitions(int[] numSegmentsForPartition) {
    assertTrue(numSegmentsForPartition[0] > 0, "Expected segments for partition 0");
    assertTrue(numSegmentsForPartition[1] > 0, "Expected segments for partition 1");
  }

  private void assertValidPartitionMetadata(ColumnPartitionMetadata columnPartitionMetadata, int partitionGroupId) {
    Set<Integer> partitions = columnPartitionMetadata.getPartitions();
    assertTrue(partitions.contains(partitionGroupId), "Expected metadata to include stream partition");
    assertTrue(partitions.stream().allMatch(partition -> partition == 0 || partition == 1),
        "Expected metadata to stay within the configured partition range");
  }

  private void assertPartitionRoutingSegmentCounts(JsonNode response, JsonNode responseToCompare,
      List<SegmentZKMetadata> segmentsZKMetadata, int partitionId) {
    assertEquals(response.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(),
        getNumSegmentsForPartition(segmentsZKMetadata, partitionId));
    assertEquals(responseToCompare.get(MetadataKey.NUM_SEGMENTS_QUERIED.getName()).asInt(),
        segmentsZKMetadata.size());
  }

  private int getNumSegmentsForPartition(List<SegmentZKMetadata> segmentsZKMetadata, int partitionId) {
    int numSegmentsForPartition = 0;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
      assertNotNull(segmentPartitionMetadata);
      ColumnPartitionMetadata columnPartitionMetadata =
          segmentPartitionMetadata.getColumnPartitionMap().get(PARTITION_COLUMN);
      assertNotNull(columnPartitionMetadata);
      if (columnPartitionMetadata.getPartitions().contains(partitionId)) {
        numSegmentsForPartition++;
      }
    }
    return numSegmentsForPartition;
  }

  private void resetKafkaTopic() {
    deleteKafkaTopicIfPresent();
    createKafkaTopic(getKafkaTopic());
  }

  private void cleanupRealtimeTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    if (_helixResourceManager.getTableConfig(realtimeTableName) != null) {
      dropRealtimeTable(getTableName());
      waitForTableDataManagerRemoved(realtimeTableName);
      waitForEVToDisappear(realtimeTableName);
    }
    if (_helixResourceManager.getSchema(getTableName()) != null) {
      deleteSchema(getTableName());
    }
  }

  private void deleteKafkaTopicIfPresent() {
    if (isKafkaTopicPresent()) {
      deleteKafkaTopic(getKafkaTopic());
    }
  }

  private boolean isKafkaTopicPresent() {
    if (_kafkaStarters == null || _kafkaStarters.isEmpty()) {
      return false;
    }
    Properties adminProps = new Properties();
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBrokerList());
    adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
    adminProps.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000");
    try (AdminClient adminClient = AdminClient.create(adminProps)) {
      return adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(getKafkaTopic());
    } catch (Exception e) {
      return false;
    }
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception firstException = null;
    try {
      cleanupRealtimeTableAndSchema();
    } catch (Exception e) {
      firstException = e;
    }
    try {
      deleteKafkaTopicIfPresent();
    } catch (Exception e) {
      if (firstException != null) {
        firstException.addSuppressed(e);
      } else {
        firstException = e;
      }
    }
    try {
      stopServer();
      stopBroker();
      stopController();
      stopKafka();
      stopZk();
    } finally {
      FileUtils.deleteDirectory(_tempDir);
    }
    if (firstException != null) {
      throw firstException;
    }
  }
}
