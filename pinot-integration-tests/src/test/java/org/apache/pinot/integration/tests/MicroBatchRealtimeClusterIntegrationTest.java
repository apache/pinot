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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.core.controllerjob.ControllerJobTypes;
import org.apache.pinot.plugin.stream.microbatch.kafka30.MicroBatchPayloadV1;
import org.apache.pinot.plugin.stream.microbatch.kafka30.MicroBatchProtocol;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for MicroBatch consumer.
 * This test validates end-to-end ingestion using the MicroBatch pattern where Kafka messages
 * contain JSON protocol references to batch files (stored in PinotFS) rather than raw records.
 */
public class MicroBatchRealtimeClusterIntegrationTest extends BaseRealtimeClusterIntegrationTest {
  private File _microBatchDataDir;
  private long _expectedRecordCount;
  private long _startTime;

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    _startTime = System.currentTimeMillis();
    // Register LocalPinotFS for file:// URIs before parent setUp
    PinotFSFactory.register("file", LocalPinotFS.class.getName(), new PinotConfiguration());
    _microBatchDataDir = new File(FileUtils.getTempDirectory(), "microbatch-" + System.currentTimeMillis());
    _microBatchDataDir.mkdirs();
    super.setUp();
  }

  @Override
  protected long getCountStarResult() {
    // Return the actual count we calculated during pushAvroIntoKafka
    return _expectedRecordCount;
  }

  @Override
  protected Map<String, String> getStreamConfigMap() {
    Map<String, String> streamConfigMap = super.getStreamConfigMap();
    String streamType = streamConfigMap.get(StreamConfigProperties.STREAM_TYPE);
    // Override consumer factory to use MicroBatch consumer
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType,
            StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        "org.apache.pinot.plugin.stream.microbatch.kafka30.KafkaMicroBatchConsumerFactory");
    // Use passthrough decoder since MicroBatch consumer returns GenericRow directly
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType,
            StreamConfigProperties.STREAM_DECODER_CLASS),
        GenericRowPassThroughDecoder.class.getName());
    return streamConfigMap;
  }

  @Override
  protected void pushAvroIntoKafka(List<File> avroFiles)
      throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + getKafkaPort());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "microbatch-integration-test-producer");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
      int messageIndex = 0;
      long totalRecords = 0;
      for (File avroFile : avroFiles) {
        // Copy the Avro file to the microbatch data directory and count records
        File destFile = new File(_microBatchDataDir, "batch-" + messageIndex + ".avro");
        long recordCount = copyAvroFile(avroFile, destFile);
        totalRecords += recordCount;

        // Create JSON protocol message with file:// URI and numRecords
        byte[] protocolMessage = MicroBatchProtocol.createUriMessage(
            destFile.toURI().toString(), MicroBatchPayloadV1.Format.AVRO, recordCount);

        // Send protocol message to Kafka - distribute across partitions for parallel consumption
        int partition = messageIndex % getNumKafkaPartitions();
        producer.send(
            new ProducerRecord<>(getKafkaTopic(), partition, System.currentTimeMillis(),
                "key_" + messageIndex, protocolMessage));
        messageIndex++;
      }
      producer.flush();
      _expectedRecordCount = totalRecords;
    }
  }

  /**
   * Copy Avro file to destination, preserving the schema and records.
   * @return the number of records copied
   */
  private long copyAvroFile(File sourceFile, File destFile)
      throws Exception {
    long recordCount = 0;
    try (DataFileReader<GenericRecord> reader =
             new DataFileReader<>(sourceFile, new GenericDatumReader<>())) {
      try (DataFileWriter<GenericRecord> writer =
               new DataFileWriter<>(new GenericDatumWriter<>(reader.getSchema()))) {
        writer.create(reader.getSchema(), destFile);
        while (reader.hasNext()) {
          writer.append(reader.next());
          recordCount++;
        }
      }
    }
    return recordCount;
  }

  @AfterClass
  @Override
  public void tearDown()
      throws Exception {
    super.tearDown();
    if (_microBatchDataDir != null && _microBatchDataDir.exists()) {
      FileUtils.deleteDirectory(_microBatchDataDir);
    }
  }

  // ==================== Additional Tests ====================

  @Test
  public void testSegmentFlushSize() {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<SegmentZKMetadata> segmentsZKMetadata =
        ZKMetadataProvider.getSegmentsZKMetadata(_propertyStore, realtimeTableName);
    for (SegmentZKMetadata segMetadata : segmentsZKMetadata) {
      if (segMetadata.getStatus() != CommonConstants.Segment.Realtime.Status.UPLOADED) {
        assertEquals(segMetadata.getSizeThresholdToFlushSegment(),
            getRealtimeSegmentFlushSize() / getNumKafkaPartitions());
      }
    }
  }

  @Test
  public void testReload()
      throws Exception {
    testReload(false);
  }

  @Test
  public void testSortedColumn()
      throws Exception {
    // There should be no inverted index or range index because the sorted column is not configured with them
    JsonNode columnIndexSize = getColumnIndexSize(getSortedColumn());
    assertFalse(columnIndexSize.has(StandardIndexes.INVERTED_ID));
    assertFalse(columnIndexSize.has(StandardIndexes.RANGE_ID));

    // For point lookup query on sorted column:
    // - Committed segments have sorted index (no scan needed)
    // - Consuming segments have inverted index (no scan needed)
    String query = "SELECT COUNT(*) FROM myTable WHERE Carrier = 'DL'";
    JsonNode response = postQuery(query);
    long numEntriesScannedInFilter = response.get("numEntriesScannedInFilter").asLong();
    // With sorted/inverted index, scan should be minimal or zero
    long numTotalDocs = getCountStarResult();
    assertTrue(numEntriesScannedInFilter < numTotalDocs,
        "Expected minimal scan with sorted/inverted index, but got " + numEntriesScannedInFilter);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testAddRemoveDictionaryAndInvertedIndex(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    notSupportedInV2();
    String query = "SELECT COUNT(*) FROM myTable WHERE ActualElapsedTime = -9999";
    long numTotalDocs = getCountStarResult();

    JsonNode queryResponse = postQuery(query);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    // Full table scan without dictionary
    assertEquals(queryResponse.get("numEntriesScannedInFilter").asLong(), numTotalDocs);
    long queryResult = queryResponse.get("resultTable").get("rows").get(0).get(0).asLong();

    // Enable dictionary and inverted index
    TableConfig tableConfig = getRealtimeTableConfig();
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    assertNotNull(indexingConfig.getNoDictionaryColumns());
    assertNotNull(indexingConfig.getInvertedIndexColumns());
    indexingConfig.getNoDictionaryColumns().remove("ActualElapsedTime");
    indexingConfig.getInvertedIndexColumns().add("ActualElapsedTime");
    updateTableConfig(tableConfig);
    String enableDictReloadId = reloadTableAndValidateResponse(getTableName(), TableType.REALTIME, false);
    TestUtils.waitForCondition(aVoid -> {
      try {
        // Query result and total docs should not change during reload
        JsonNode queryResponse1 = postQuery(query);
        assertEquals(queryResponse1.get("resultTable").get("rows").get(0).get(0).asLong(), queryResult);
        assertEquals(queryResponse1.get("totalDocs").asLong(), numTotalDocs);

        long numConsumingSegmentsQueried = queryResponse1.get("numConsumingSegmentsQueried").asLong();
        long minConsumingFreshnessTimeMs = queryResponse1.get("minConsumingFreshnessTimeMs").asLong();
        return numConsumingSegmentsQueried == getNumKafkaPartitions()
            && minConsumingFreshnessTimeMs > _startTime
            && minConsumingFreshnessTimeMs < System.currentTimeMillis()
            && isReloadJobCompleted(enableDictReloadId);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate dictionary and inverted index");
    // numEntriesScannedInFilter should be zero with inverted index
    queryResponse = postQuery(query);
    assertEquals(queryResponse.get("numEntriesScannedInFilter").asLong(), 0L);

    // Disable dictionary and inverted index
    indexingConfig.getNoDictionaryColumns().add("ActualElapsedTime");
    indexingConfig.getInvertedIndexColumns().remove("ActualElapsedTime");
    updateTableConfig(tableConfig);
    String disableDictReloadId = reloadTableAndValidateResponse(getTableName(), TableType.REALTIME, false);
    TestUtils.waitForCondition(aVoid -> {
      try {
        // Query result and total docs should not change during reload
        JsonNode queryResponse1 = postQuery(query);
        assertEquals(queryResponse1.get("resultTable").get("rows").get(0).get(0).asLong(), queryResult);
        assertEquals(queryResponse1.get("totalDocs").asLong(), numTotalDocs);
        return isReloadJobCompleted(disableDictReloadId);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Failed to remove dictionary and inverted index");
    // Should get back to full table scan
    queryResponse = postQuery(query);
    assertEquals(queryResponse.get("numEntriesScannedInFilter").asLong(), numTotalDocs);
  }

  @Test
  public void testReset() {
    testReset(TableType.REALTIME);
  }

  @Test
  public void testForceCommit()
      throws Exception {
    String realtimeTableName = TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    Set<String> consumingSegments = getConsumingSegmentsFromIdealState(realtimeTableName);
    // Use regular force commit without batch
    String jobId = forceCommit(realtimeTableName);
    testForceCommitInternal(realtimeTableName, jobId, consumingSegments, 60000L);
  }

  private Set<String> getConsumingSegmentsFromIdealState(String realtimeTableName) {
    IdealState idealState = _helixResourceManager.getTableIdealState(realtimeTableName);
    assertNotNull(idealState);
    Set<String> consumingSegments = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      if (entry.getValue().containsValue(CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING)) {
        consumingSegments.add(entry.getKey());
      }
    }
    return consumingSegments;
  }

  private String forceCommit(String tableName)
      throws Exception {
    String response = sendPostRequest(_controllerRequestURLBuilder.forTableForceCommit(tableName), null);
    return JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();
  }

  private void testForceCommitInternal(String realtimeTableName, String jobId, Set<String> consumingSegments,
      long timeoutMs) {
    Map<String, String> jobMetadata =
        _helixResourceManager.getControllerJobZKMetadata(jobId, ControllerJobTypes.FORCE_COMMIT);
    assertNotNull(jobMetadata);
    assertNotNull(jobMetadata.get(CommonConstants.ControllerJob.CONSUMING_SEGMENTS_FORCE_COMMITTED_LIST));

    TestUtils.waitForCondition(aVoid -> {
      try {
        if (isForceCommitJobCompleted(jobId)) {
          for (String segmentName : consumingSegments) {
            SegmentZKMetadata segmentZKMetadata =
                _helixResourceManager.getSegmentZKMetadata(realtimeTableName, segmentName);
            assertNotNull(segmentZKMetadata);
            assertEquals(segmentZKMetadata.getStatus(), CommonConstants.Segment.Realtime.Status.DONE);
          }
          return true;
        }
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, timeoutMs, "Error verifying force commit operation on table!");
  }

  private boolean isForceCommitJobCompleted(String forceCommitJobId)
      throws Exception {
    String jobStatusResponse = sendGetRequest(_controllerRequestURLBuilder.forForceCommitJobStatus(forceCommitJobId));
    JsonNode jobStatus = JsonUtils.stringToJsonNode(jobStatusResponse);

    assertEquals(jobStatus.get("jobId").asText(), forceCommitJobId);
    assertEquals(jobStatus.get("jobType").asText(), "FORCE_COMMIT");

    Set<String> allSegments = JsonUtils.stringToObject(
        jobStatus.get(CommonConstants.ControllerJob.CONSUMING_SEGMENTS_FORCE_COMMITTED_LIST).asText(), HashSet.class);
    Set<String> pendingSegments = new HashSet<>();
    for (JsonNode element : jobStatus.get(CommonConstants.ControllerJob.CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED_LIST)) {
      pendingSegments.add(element.asText());
    }

    assertTrue(pendingSegments.size() <= allSegments.size());
    assertEquals(jobStatus.get(CommonConstants.ControllerJob.NUM_CONSUMING_SEGMENTS_YET_TO_BE_COMMITTED).asInt(-1),
        pendingSegments.size());

    return pendingSegments.isEmpty();
  }

  // ==================== Helper Classes ====================

  /**
   * Pass-through decoder for MicroBatch consumer.
   * MicroBatch consumer returns GenericRow directly (already decoded from Avro files),
   * so this decoder simply passes through the GenericRow without additional decoding.
   */
  public static class GenericRowPassThroughDecoder implements StreamMessageDecoder<GenericRow> {

    @Override
    public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
        throws Exception {
      // No initialization needed
    }

    @Override
    public void init(Set<String> fieldsToRead, StreamConfig streamConfig, TableConfig tableConfig, Schema schema)
        throws Exception {
      // No initialization needed
    }

    @Override
    public GenericRow decode(GenericRow payload, GenericRow destination) {
      // Copy all fields from payload to destination
      destination.init(payload);
      return destination;
    }

    @Override
    public GenericRow decode(GenericRow payload, int offset, int length, GenericRow destination) {
      // offset and length are not applicable for GenericRow
      return decode(payload, destination);
    }
  }
}
