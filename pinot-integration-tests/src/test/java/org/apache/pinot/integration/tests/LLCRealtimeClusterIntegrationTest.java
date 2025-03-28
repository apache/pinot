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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.controllerjob.ControllerJobType;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.plugin.stream.kafka.KafkaMessageBatch;
import org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory;
import org.apache.pinot.plugin.stream.kafka20.KafkaPartitionLevelConsumer;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Integration test for low-level Kafka consumer.
 * TODO: Add separate module-level tests and remove the randomness of this test
 */
public class LLCRealtimeClusterIntegrationTest extends BaseRealtimeClusterIntegrationTest {
  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);

  private final boolean _isDirectAlloc = RANDOM.nextBoolean();
  private final boolean _isConsumerDirConfigured = RANDOM.nextBoolean();
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();
  private final long _startTime = System.currentTimeMillis();

  @Override
  protected boolean injectTombstones() {
    return true;
  }

  @Override
  protected String getLoadMode() {
    return ReadMode.mmap.name();
  }

  @Override
  public void startController()
      throws Exception {
    super.startController();
    enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, _isDirectAlloc);
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    // Make sure the realtime segment validation manager does not run by itself, only when we invoke it.
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_FREQUENCY_PERIOD, "2h");
    properties.put(ControllerConf.ControllerPeriodicTasksConf.REALTIME_SEGMENT_VALIDATION_INITIAL_DELAY_IN_SECONDS,
        3600);
  }

  @Override
  protected void runValidationJob(long timeoutMs)
      throws Exception {
    int partition = ExceptingKafkaConsumerFactory.PARTITION_FOR_EXCEPTIONS;
    if (partition < 0) {
      return;
    }
    for (int seqNum : new int[]{
        ExceptingKafkaConsumerFactory.SEQ_NUM_FOR_CREATE_EXCEPTION,
        ExceptingKafkaConsumerFactory.SEQ_NUM_FOR_CONSUME_EXCEPTION
    }) {
      TestUtils.waitForCondition(aVoid -> isOffline(partition, seqNum), timeoutMs,
          "Failed to find OFFLINE segment in partition: " + partition + ", seqNum: " + seqNum);
      getControllerRequestClient().runPeriodicTask("RealtimeSegmentValidationManager");
    }
  }

  private boolean isOffline(int partition, int seqNum) {
    IdealState idealState = _helixAdmin.getResourceIdealState(getHelixClusterName(),
        TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      LLCSegmentName llcSegmentName = LLCSegmentName.of(entry.getKey());
      if (llcSegmentName != null && llcSegmentName.getPartitionGroupId() == partition
          && llcSegmentName.getSequenceNumber() == seqNum) {
        return !entry.getValue().containsValue(SegmentStateModel.CONSUMING);
      }
    }
    return false;
  }

  @Override
  protected Map<String, String> getStreamConfigMap() {
    Map<String, String> streamConfigMap = super.getStreamConfigMap();
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(
        streamConfigMap.get(StreamConfigProperties.STREAM_TYPE),
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), ExceptingKafkaConsumerFactory.class.getName());
    ExceptingKafkaConsumerFactory.init(getHelixClusterName(), _helixAdmin, getTableName());
    return streamConfigMap;
  }
  @Override
  protected IngestionConfig getIngestionConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(Collections.singletonList(getStreamConfigMap())));
    return ingestionConfig;
  }

  @Override
  protected Map<String, String> getStreamConfigs() {
    return null;
  }

  @Override
  protected void createSegmentsAndUpload(List<File> avroFiles, Schema schema, TableConfig tableConfig)
      throws Exception {
    if (!_tarDir.exists()) {
      _tarDir.mkdir();
    }
    if (!_segmentDir.exists()) {
      _segmentDir.mkdir();
    }

    // create segments out of the avro files (segments will be placed in _tarDir)
    List<File> copyOfAvroFiles = new ArrayList<>(avroFiles);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(copyOfAvroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);

    // upload segments to controller
    uploadSegmentsToController(getTableName(), _tarDir, false, false);

    // upload the first segment again to verify refresh
    uploadSegmentsToController(getTableName(), _tarDir, true, false);

    // upload the first segment again to verify refresh with different segment crc
    uploadSegmentsToController(getTableName(), _tarDir, true, true);

    // add avro files to the original list so H2 will have the uploaded data as well
    avroFiles.addAll(copyOfAvroFiles);
  }

  private void uploadSegmentsToController(String tableName, File tarDir, boolean onlyFirstSegment, boolean changeCrc)
      throws Exception {
    File[] segmentTarFiles = tarDir.listFiles();
    assertNotNull(segmentTarFiles);
    int numSegments = segmentTarFiles.length;
    assertTrue(numSegments > 0);
    if (onlyFirstSegment) {
      numSegments = 1;
    }
    URI uploadSegmentHttpURI = URI.create(getControllerRequestURLBuilder().forSegmentUpload());
    try (FileUploadDownloadClient fileUploadDownloadClient = new FileUploadDownloadClient()) {
      if (numSegments == 1) {
        File segmentTarFile = segmentTarFiles[0];
        if (changeCrc) {
          changeCrcInSegmentZKMetadata(tableName, segmentTarFile.toString());
        }
        assertEquals(
            fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(), segmentTarFile,
                tableName, TableType.REALTIME).getStatusCode(), HttpStatus.SC_OK);
      } else {
        // Upload segments in parallel
        ExecutorService executorService = Executors.newFixedThreadPool(numSegments);
        List<Future<Integer>> futures = new ArrayList<>(numSegments);
        for (File segmentTarFile : segmentTarFiles) {
          futures.add(executorService.submit(
              () -> fileUploadDownloadClient.uploadSegment(uploadSegmentHttpURI, segmentTarFile.getName(),
                  segmentTarFile, tableName, TableType.REALTIME).getStatusCode()));
        }
        executorService.shutdown();
        for (Future<Integer> future : futures) {
          assertEquals((int) future.get(), HttpStatus.SC_OK);
        }
      }
    }
  }

  private void changeCrcInSegmentZKMetadata(String tableName, String segmentFilePath) {
    int startIdx = segmentFilePath.indexOf("mytable_");
    int endIdx = segmentFilePath.indexOf(".tar.gz");
    String segmentName = segmentFilePath.substring(startIdx, endIdx);
    String tableNameWithType = TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName);
    SegmentZKMetadata segmentZKMetadata = _helixResourceManager.getSegmentZKMetadata(tableNameWithType, segmentName);
    segmentZKMetadata.setCrc(111L);
    _helixResourceManager.updateZkMetadata(tableNameWithType, segmentZKMetadata);
  }

  @Override
  protected long getCountStarResult() {
    // all the data that was ingested from Kafka also got uploaded via the controller's upload endpoint
    return super.getCountStarResult() * 2;
  }

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    System.out.println(String.format(
        "Using random seed: %s, isDirectAlloc: %s, isConsumerDirConfigured: %s, enableLeadControllerResource: %s",
        RANDOM_SEED, _isDirectAlloc, _isConsumerDirConfigured, _enableLeadControllerResource));

    // Remove the consumer directory
    FileUtils.deleteQuietly(new File(CONSUMER_DIRECTORY));

    super.setUp();
  }

  @AfterClass
  @Override
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(new File(CONSUMER_DIRECTORY));
    super.tearDown();
  }

  @Test
  public void testConsumerDirectoryExists() {
    File consumerDirectory = new File(CONSUMER_DIRECTORY, "mytable_REALTIME");
    assertEquals(consumerDirectory.exists(), _isConsumerDirConfigured,
        "The off heap consumer directory does not exist");
  }

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
    // There should be no inverted index or range index sealed because the sorted column is not configured with them
    JsonNode columnIndexSize = getColumnIndexSize(getSortedColumn());
    assertFalse(columnIndexSize.has(StandardIndexes.INVERTED_ID));
    assertFalse(columnIndexSize.has(StandardIndexes.RANGE_ID));

    // For point lookup query, there should be no scan from the committed/consuming segments, but full scan from the
    // uploaded segments:
    // - Committed segments have sorted index
    // - Consuming segments have inverted index
    // - Uploaded segments have neither of them
    String query = "SELECT COUNT(*) FROM myTable WHERE Carrier = 'DL'";
    JsonNode response = postQuery(query);
    long numEntriesScannedInFilter = response.get("numEntriesScannedInFilter").asLong();
    long numDocsInUploadedSegments = super.getCountStarResult();
    assertEquals(numEntriesScannedInFilter, numDocsInUploadedSegments);

    // For range query, there should be no scan from the committed segments, but full scan from the uploaded/consuming
    // segments:
    // - Committed segments have sorted index
    // - Consuming/Uploaded segments do not have sorted index
    query = "SELECT COUNT(*) FROM myTable WHERE Carrier > 'DL'";
    response = postQuery(query);
    numEntriesScannedInFilter = response.get("numEntriesScannedInFilter").asLong();
    // NOTE: If this test is running after force commit test, there will be no records in consuming segments
    assertTrue(numEntriesScannedInFilter >= numDocsInUploadedSegments);
    assertTrue(numEntriesScannedInFilter < 2 * numDocsInUploadedSegments);
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

    // Enable dictionary and inverted index.
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
        return numConsumingSegmentsQueried == 2 && minConsumingFreshnessTimeMs > _startTime
            && minConsumingFreshnessTimeMs < System.currentTimeMillis() && isReloadJobCompleted(enableDictReloadId);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate dictionary and inverted index");
    // numEntriesScannedInFilter should be zero with inverted index
    queryResponse = postQuery(query);
    assertEquals(queryResponse.get("numEntriesScannedInFilter").asLong(), 0L);

    // Disable dictionary and inverted index.
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
    if (RANDOM.nextBoolean()) {
      // Regular force commit without batch
      String jobId = forceCommit(realtimeTableName);
      testForceCommitInternal(realtimeTableName, jobId, consumingSegments, 60000L);
    } else {
      // Force commit with batch
      String jobId = forceCommit(realtimeTableName, 1, 1, 120);
      testForceCommitInternal(realtimeTableName, jobId, consumingSegments, 120000L);
    }
  }

  public Set<String> getConsumingSegmentsFromIdealState(String realtimeTableName) {
    IdealState idealState = _helixResourceManager.getTableIdealState(realtimeTableName);
    assertNotNull(idealState);
    Set<String> consumingSegments = new HashSet<>();
    for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
      if (entry.getValue().containsValue(SegmentStateModel.CONSUMING)) {
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

  private String forceCommit(String tableName, int batchSize, int batchIntervalSec, int batchTimeoutSec)
      throws Exception {
    String response = sendPostRequest(
        _controllerRequestURLBuilder.forTableForceCommit(tableName) + "?batchSize=" + batchSize
            + "&batchStatusCheckIntervalSec=" + batchIntervalSec + "&batchStatusCheckTimeoutSec=" + batchTimeoutSec,
        null);
    return JsonUtils.stringToJsonNode(response).get("forceCommitJobId").asText();
  }

  private void testForceCommitInternal(String realtimeTableName, String jobId, Set<String> consumingSegments,
      long timeoutMs) {
    Map<String, String> jobMetadata =
        _helixResourceManager.getControllerJobZKMetadata(jobId, ControllerJobType.FORCE_COMMIT);
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

  public boolean isForceCommitJobCompleted(String forceCommitJobId)
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

  @Test
  @Override
  public void testHardcodedServerPartitionedSqlQueries()
      throws Exception {
    super.testHardcodedServerPartitionedSqlQueries();
  }

  public static class ExceptingKafkaConsumerFactory extends KafkaConsumerFactory {

    public static final int PARTITION_FOR_EXCEPTIONS = 1; // Setting this to -1 disables all exceptions thrown.
    public static final int SEQ_NUM_FOR_CREATE_EXCEPTION = 1;
    public static final int SEQ_NUM_FOR_CONSUME_EXCEPTION = 3;

    private static HelixAdmin _helixAdmin;
    private static String _helixClusterName;
    private static String _tableName;
    public ExceptingKafkaConsumerFactory() {
      super();
    }

    public static void init(String helixClusterName, HelixAdmin helixAdmin, String tableName) {
      _helixAdmin = helixAdmin;
      _helixClusterName = helixClusterName;
      _tableName = tableName;
    }

    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
        PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
      /*
       * The segment data manager is creating a consumer to consume rows into a segment.
       * Check the partition and sequence number of the segment and decide whether it
       * qualifies for:
       * - Throwing exception during create OR
       * - Throwing exception during consumption.
       * Make sure that this still works if retries are added in RealtimeSegmentDataManager
       */
      int partition = partitionGroupConsumptionStatus.getPartitionGroupId();
      boolean exceptionDuringConsume = false;
      int seqNum = getSegmentSeqNum(partition);
      if (partition == PARTITION_FOR_EXCEPTIONS) {
        if (seqNum == SEQ_NUM_FOR_CREATE_EXCEPTION) {
          throw new RuntimeException("TestException during consumer creation");
        } else if (seqNum == SEQ_NUM_FOR_CONSUME_EXCEPTION) {
          exceptionDuringConsume = true;
        }
      }
      return new ExceptingKafkaConsumer(clientId, _streamConfig, partition, exceptionDuringConsume);
    }

    private int getSegmentSeqNum(int partition) {
      IdealState idealState =
          _helixAdmin.getResourceIdealState(_helixClusterName, TableNameBuilder.REALTIME.tableNameWithType(_tableName));
      for (Map.Entry<String, Map<String, String>> entry : idealState.getRecord().getMapFields().entrySet()) {
        LLCSegmentName llcSegmentName = LLCSegmentName.of(entry.getKey());
        if (llcSegmentName != null && llcSegmentName.getPartitionGroupId() == partition && entry.getValue()
            .containsValue(SegmentStateModel.CONSUMING)) {
          return llcSegmentName.getSequenceNumber();
        }
      }
      fail("No consuming segment found in partition: " + partition);
      return -1;
    }

    public static class ExceptingKafkaConsumer extends KafkaPartitionLevelConsumer {
      private final boolean _exceptionDuringConsume;

      public ExceptingKafkaConsumer(String clientId, StreamConfig streamConfig, int partition,
          boolean exceptionDuringConsume) {
        super(clientId, streamConfig, partition);
        _exceptionDuringConsume = exceptionDuringConsume;
      }

      @Override
      public KafkaMessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) {
        if (_exceptionDuringConsume) {
          throw new RuntimeException("TestException during consumption");
        }
        return super.fetchMessages(startOffset, timeoutMs);
      }
    }
  }
}
