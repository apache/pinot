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
import static org.testng.Assert.assertTrue;


/**
 * Integration test that enables segment partition for the LLC real-time table.
 */
public class SegmentPartitionLLCRealtimeClusterIntegrationTest extends BaseClusterIntegrationTest {
  // Number of documents in the first Avro file
  private static final long NUM_DOCS = 9292;

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
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config with reduced number of columns and partition config
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(getSchemaName()).addSingleValueDimension("Carrier", DataType.STRING)
            .addDateTime("DaysSinceEpoch", DataType.INT, "1:DAYS:EPOCH", "1:DAYS").build();
    addSchema(schema);

    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setSegmentPartitionConfig(
        new SegmentPartitionConfig(Collections.singletonMap("Carrier", new ColumnPartitionConfig("murmur", 5))));
    tableConfig.setRoutingConfig(
        new RoutingConfig(null, Collections.singletonList(RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE), null));
    addTableConfig(tableConfig);

    // Push data into Kafka (only ingest the first Avro file)
    pushAvroIntoKafka(Collections.singletonList(avroFiles.get(0)));

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  protected boolean useLlc() {
    return true;
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
    List<RealtimeSegmentZKMetadata> segmentZKMetadataList =
        _helixResourceManager.getRealtimeSegmentMetadata(getTableName());
    for (RealtimeSegmentZKMetadata segmentZKMetadata : segmentZKMetadataList) {
      SegmentPartitionMetadata segmentPartitionMetadata = segmentZKMetadata.getPartitionMetadata();
      assertNotNull(segmentPartitionMetadata);
      Map<String, ColumnPartitionMetadata> columnPartitionMetadataMap =
          segmentPartitionMetadata.getColumnPartitionMap();
      assertEquals(columnPartitionMetadataMap.size(), 1);
      ColumnPartitionMetadata columnPartitionMetadata = columnPartitionMetadataMap.get("Carrier");
      assertNotNull(columnPartitionMetadata);

      // The function name should be aligned with the partition config in the table config
      assertTrue(columnPartitionMetadata.getFunctionName().equalsIgnoreCase("murmur"));

      if (segmentZKMetadata.getStatus() == Status.IN_PROGRESS) {
        // Consuming segment

        // Number of partitions should be aligned with the partition config in the table config
        assertEquals(columnPartitionMetadata.getNumPartitions(), 5);

        // Should contain only the stream partition
        assertEquals(columnPartitionMetadata.getPartitions(),
            Collections.singleton(new LLCSegmentName(segmentZKMetadata.getSegmentName()).getPartitionId()));
      } else {
        // Completed segment

        // Number of partitions should be the same as number of stream partitions
        assertEquals(columnPartitionMetadata.getNumPartitions(), 2);

        // Should contain the partitions based on the ingested records. Since the records are not partitioned in Kafka,
        // it should contain all the partitions.
        assertEquals(columnPartitionMetadata.getPartitions(), new HashSet<>(Arrays.asList(0, 1)));
      }
    }
  }

  // TODO: Add test on partition routing once the consuming segment behavior is fixed.
  //       Currently the partition info is cached in the PartitionSegmentPruner, and won't be reloaded when the
  //       consuming segment gets committed. The segment will be pruned based on the consuming segment partition info
  //       (using stream partition as the segment partition), even if the partition info changed for the completed
  //       segment.

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
