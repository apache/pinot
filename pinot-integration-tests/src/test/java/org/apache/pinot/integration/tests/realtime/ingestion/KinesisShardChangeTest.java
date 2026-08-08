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
package org.apache.pinot.integration.tests.realtime.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.common.restlet.resources.TableView;
import org.apache.pinot.integration.tests.realtime.ingestion.utils.KinesisUtils;
import org.apache.pinot.plugin.stream.kinesis.KinesisConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING;
import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;


public class KinesisShardChangeTest extends BaseKinesisIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisShardChangeTest.class);

  private static final String SCHEMA_FILE_PATH = "kinesis/airlineStats_data_reduced.schema";
  private static final String DATA_FILE_PATH = "kinesis/airlineStats_data_reduced.json";
  private static final String TABLE_NAME = "kinesisShardChange";
  private static final String STREAM_NAME = "kinesis-shard-change";
  private static final Integer NUM_SHARDS = 2;
  private static final List<ShardOffsetScenario> SPLIT_OFFSET_SCENARIOS = List.of(
      new ShardOffsetScenario("split", "smallest", "lastConsumed", 100, 250, 4, 4),
      new ShardOffsetScenario("split", "smallest", null, 100, 250, 4, 4),
      new ShardOffsetScenario("split", "largest", "lastConsumed", 50, 200, 2, 4),
      new ShardOffsetScenario("split", "largest", null, 50, 200, 2, 4),
      new ShardOffsetScenario("split", "lastConsumed", "lastConsumed", 200, 200, 6, 4),
      new ShardOffsetScenario("split", "lastConsumed", "largest", 200, 200, 6, 4),
      new ShardOffsetScenario("split", "lastConsumed", null, 200, 200, 2, 4),
      new ShardOffsetScenario("split", null, null, 200, 200, 2, 4));
  private static final List<ShardOffsetScenario> MERGE_OFFSET_SCENARIOS = List.of(
      new ShardOffsetScenario("merge", "smallest", "lastConsumed", 100, 250, 4, 1),
      new ShardOffsetScenario("merge", "smallest", null, 100, 250, 4, 1),
      new ShardOffsetScenario("merge", "largest", "lastConsumed", 50, 200, 2, 1),
      new ShardOffsetScenario("merge", "largest", null, 50, 200, 2, 1),
      new ShardOffsetScenario("merge", "lastConsumed", "lastConsumed", 200, 200, 3, 1),
      new ShardOffsetScenario("merge", "lastConsumed", "largest", 200, 200, 3, 1),
      new ShardOffsetScenario("merge", "lastConsumed", null, 200, 200, 2, 1),
      new ShardOffsetScenario("merge", null, null, 200, 200, 2, 1));
  private static final List<String> INITIAL_OFFSET_CRITERIA = List.of("smallest", "largest", "lastConsumed");

  private Thread _publisherThread;
  private volatile Throwable _publisherFailure;

  @BeforeMethod
  public void beforeTest()
      throws Exception {
    cleanupKinesisResources();
    createStream(NUM_SHARDS);
  }

  @AfterMethod(alwaysRun = true)
  public void afterTest(ITestResult testResult)
      throws Exception {
    Throwable primaryFailure = testResult.getThrowable();
    boolean interrupted = Thread.interrupted();
    Throwable cleanupFailure = null;
    try {
      stopPublisherThread();
    } catch (Throwable t) {
      cleanupFailure = t;
    }
    interrupted |= Thread.interrupted();
    try {
      cleanupKinesisResources();
    } catch (Throwable t) {
      if (cleanupFailure == null) {
        cleanupFailure = t;
      } else {
        cleanupFailure.addSuppressed(t);
      }
    }
    interrupted |= Thread.interrupted();
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    if (cleanupFailure != null) {
      if (primaryFailure != null) {
        primaryFailure.addSuppressed(cleanupFailure);
        return;
      }
      if (cleanupFailure instanceof Error) {
        throw (Error) cleanupFailure;
      }
      if (cleanupFailure instanceof Exception) {
        throw (Exception) cleanupFailure;
      }
      throw new RuntimeException(cleanupFailure);
    }
  }

  /// Test case to validate shard split/merge behavior with different offset combinations.
  /// The expectation is that
  /// 1. when "smallest" offset is used, the old parent shards would be consumed first.
  ///    New shards will not be consumed until RVM is run or resume() is called with lastConsumed / largest offset
  /// 2. when "largest" offset is used, only new records would be consumed and all prior records pushed to kinesis
  ///    would be skipped.
  /// 3. when "lastConsumed" offset is used, data would be consumed based on the last consumed offset.
  /// 4. when RealtimeSegmentValidationManager is triggered, the behaviour should be same as calling resume() with
  ///    "lastConsumed" offset.
  @Test
  public void testShardSplitWithOffsets()
      throws Exception {
    runShardOperationScenarios("split", SPLIT_OFFSET_SCENARIOS);
  }

  @Test
  public void testShardMergeWithOffsets()
      throws Exception {
    runShardOperationScenarios("merge", MERGE_OFFSET_SCENARIOS);
  }

  private void runShardOperationScenarios(String operation, List<ShardOffsetScenario> scenarios)
      throws Exception {
    Map<String, Integer> expectedRecordsByTable = new LinkedHashMap<>();
    for (ShardOffsetScenario scenario : scenarios) {
      createNewSchemaAndTable(scenario._tableName, "smallest");
      expectedRecordsByTable.put(scenario._tableName, 50);
    }

    publishRecordsToKinesis(0, 50);
    waitForRecordsToBeConsumed(expectedRecordsByTable);

    if ("split".equals(operation)) {
      KinesisUtils.splitNthShard(_kinesisClient, getKinesisStreamName(), 0); // splits shard 0 into shard 2 & 3
      KinesisUtils.splitNthShard(_kinesisClient, getKinesisStreamName(), 1); // splits shard 1 into shard 4 & 5
    } else {
      KinesisUtils.mergeShards(_kinesisClient, getKinesisStreamName(), 0, 1); // merges shard 0 & 1 into shard 2
    }

    publishRecordsToKinesis(50, 200);

    for (ShardOffsetScenario scenario : scenarios) {
      applyOffsetAction(scenario, scenario._firstOffsetCriteria, "first");
      expectedRecordsByTable.put(scenario._tableName, scenario._firstExpectedRecords);
    }
    waitForRecordsToBeConsumed(expectedRecordsByTable);

    for (ShardOffsetScenario scenario : scenarios) {
      applyOffsetAction(scenario, scenario._secondOffsetCriteria, "second");
      expectedRecordsByTable.put(scenario._tableName, scenario._secondExpectedRecords);
    }
    waitForRecordsToBeConsumed(expectedRecordsByTable);

    // Publish more records after shard operation. These will go to the new shards
    publishRecordsToKinesis(100, 200);
    for (ShardOffsetScenario scenario : scenarios) {
      // TODO - Tables resumed with largest should consume the 100 new records without relying on RVM.
      int expectedRecords = "largest".equals(scenario._secondOffsetCriteria)
          ? scenario._secondExpectedRecords : scenario._secondExpectedRecords + 100;
      expectedRecordsByTable.put(scenario._tableName, expectedRecords);
    }
    waitForRecordsToBeConsumed(expectedRecordsByTable);

    waitForIdealStateToMatchExternalView(expectedRecordsByTable.keySet());
    runPeriodicTask("RealtimeSegmentValidationManager", null, TableType.REALTIME);
    for (ShardOffsetScenario scenario : scenarios) {
      expectedRecordsByTable.put(scenario._tableName, scenario._secondExpectedRecords + 100);
    }
    waitForRecordsToBeConsumed(expectedRecordsByTable);

    Map<String, SegmentStateExpectation> expectedSegmentStates = new LinkedHashMap<>();
    for (ShardOffsetScenario scenario : scenarios) {
      expectedSegmentStates.put(scenario._tableName,
          new SegmentStateExpectation(scenario._name, scenario._expectedOnlineSegments,
              scenario._expectedConsumingSegments));
    }
    waitForSegmentStates(expectedSegmentStates);
  }

  /// Test case to split shards, then create new table and check consumption
  /// For the sake of brevity, we will only test shard split and calling Realtime Validation Manager
  /// Individually, pause and resume have been verified for shard split / merge operations
  @Test
  public void testNewTablesAfterShardSplit()
      throws Exception {
    publishRecordsToKinesis(0, 50);

    KinesisUtils.splitNthShard(_kinesisClient, getKinesisStreamName(), 0); // splits shard 0 into shard 2 & 3
    KinesisUtils.splitNthShard(_kinesisClient, getKinesisStreamName(), 1); // splits shard 1 into shard 4 & 5

    Map<String, Integer> expectedRecordsByTable = new LinkedHashMap<>();
    for (String offsetCriteria : INITIAL_OFFSET_CRITERIA) {
      String tableName = TABLE_NAME + "_new_after_split_" + offsetCriteria;
      createNewSchemaAndTable(tableName, offsetCriteria);
      expectedRecordsByTable.put(tableName, 50);
    }
    waitForRecordsToBeConsumed(expectedRecordsByTable);

    // publish more records. These will go to the new shards
    publishRecordsToKinesis(50, 200);
    waitForRecordsToBeConsumed(expectedRecordsByTable); // Pinot does not listen to new shards yet.

    // Trigger RVM. This will commit the current segments and start consuming from the new shards
    waitForIdealStateToMatchExternalView(expectedRecordsByTable.keySet());
    runPeriodicTask("RealtimeSegmentValidationManager", null, TableType.REALTIME);
    for (Map.Entry<String, Integer> entry : expectedRecordsByTable.entrySet()) {
      entry.setValue(200);
    }
    waitForRecordsToBeConsumed(expectedRecordsByTable);

    Map<String, SegmentStateExpectation> expectedSegmentStates = new LinkedHashMap<>();
    for (String offsetCriteria : INITIAL_OFFSET_CRITERIA) {
      String tableName = TABLE_NAME + "_new_after_split_" + offsetCriteria;
      expectedSegmentStates.put(tableName,
          new SegmentStateExpectation("new table after split [initial=" + offsetCriteria + "]", 2, 4));
    }
    waitForSegmentStates(expectedSegmentStates);
  }

  /// Test case to first split shards, then merge some shards.
  /// For the sake of brevity, we will only test by calling Realtime Validation Manager
  /// Individually, pause and resume have been verified for shard split / merge operations
  @Test
  public void testSplitAndMergeShards()
      throws Exception {
    createDefaultSchemaAndTable();

    // Publish initial records
    publishRecordsToKinesis(0, 50);
    waitForRecordsToBeConsumed(getTableName(), 50); // pinot has created 2 segments

    // Split the shards
    KinesisUtils.splitNthShard(_kinesisClient, getKinesisStreamName(), 0); // splits shard 0 into shard 2 & 3
    KinesisUtils.splitNthShard(_kinesisClient, getKinesisStreamName(), 1); // splits shard 1 into shard 4 & 5

    // Publish more records after shard operation. These will go to the new shards
    publishRecordsToKinesis(50, 175);

    // Merge some shards
    KinesisUtils.mergeShards(_kinesisClient, getKinesisStreamName(), 2, 3); // merges shard 2 & 3 into shard 6
    KinesisUtils.mergeShards(_kinesisClient, getKinesisStreamName(), 4, 5); // merges shard 4 & 5 into shard 7

    // Publish more records after shard operation. These will go to the new shards
    publishRecordsToKinesis(175, 200);

    // Trigger RVM. This will commit segments 0 and 1 and start consuming from shards 2-5
    runRealtimeSegmentValidationTask(getTableName());
    waitForRecordsToBeConsumed(getTableName(), 175);

    // Trigger RVM. This will commit segments 2-5 and start consuming from shards 6-7
    runRealtimeSegmentValidationTask(getTableName());
    waitForRecordsToBeConsumed(getTableName(), 200);

    // Validate that 8 segments are created in total
    waitForSegmentStates(getTableName(), 6, 2);
  }

  /// Test case to continuously publish records to kinesis (in a background thread) and concurrently split shards
  /// and concurrently call pause and resume APIs or RVM and finally validate the total count of records
  @Test
  public void testConcurrentShardSplit()
      throws IOException, InterruptedException {
    createDefaultSchemaAndTable();

    // Start a background thread to continuously publish records to kinesis
    _publisherFailure = null;
    _publisherThread = new Thread(() -> {
      try {
        for (int i = 0; i < 200; i += 5) {
          publishRecordsToKinesis(i, i + 5);
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        _publisherFailure = e;
        LOGGER.error("Error while publishing records to kinesis", e);
      }
    }, "kinesis-shard-change-publisher");
    _publisherThread.start(); // This will take ~40 secs to complete with 5 records ingested per second

    Thread.sleep(5000);

    // Split the shards
    KinesisUtils.splitNthShard(_kinesisClient, getKinesisStreamName(), 0); // splits shard 0 into shard 2 & 3
    KinesisUtils.splitNthShard(_kinesisClient, getKinesisStreamName(), 1); // splits shard 1 into shard 4 & 5

    Thread.sleep(5000);

    // Trigger RVM. This will commit segments 0 and 1 and start consuming from shards 2-5
    runRealtimeSegmentValidationTask(getTableName()); // This will commit segments 0-1 and start consuming from 2-5

    // Merge some shards
    KinesisUtils.mergeShards(_kinesisClient, getKinesisStreamName(), 2, 3); // merges shard 2 & 3 into shard 6
    KinesisUtils.mergeShards(_kinesisClient, getKinesisStreamName(), 4, 5); // merges shard 4 & 5 into shard 7

    Thread.sleep(5000);

    // Call pause and resume APIs
    pauseTable(getTableName()); // This will commit segments 2-5
    resumeTable(getTableName(), "lastConsumed"); // start consuming from shards 6-7

    // Wait for the publisher thread to finish
    try {
      _publisherThread.join();
      _publisherThread = null;
    } catch (InterruptedException e) {
      LOGGER.error("Error while waiting for publisher thread to finish", e);
      Thread.currentThread().interrupt();
      throw e;
    }
    throwIfPublisherFailed();

    waitForRecordsToBeConsumed(getTableName(), 200);

    // Validate that all records are consumed
    waitForSegmentStates(getTableName(), 6, 2);
  }

  private void waitForSegmentStates(String tableName, int expectedOnlineSegments, int expectedConsumingSegments)
      throws IOException {
    waitForSegmentStates(
        Map.of(tableName, new SegmentStateExpectation(tableName, expectedOnlineSegments, expectedConsumingSegments)));
  }

  private void waitForSegmentStates(Map<String, SegmentStateExpectation> expectationsByTable)
      throws IOException {
    TestUtils.waitForCondition(aVoid -> {
      boolean allStatesMatch = true;
      for (Map.Entry<String, SegmentStateExpectation> entry : expectationsByTable.entrySet()) {
        String tableName = entry.getKey();
        SegmentStateExpectation expectation = entry.getValue();
        try {
          TableView tableView = getExternalView(tableName, TableType.REALTIME);
          int totalSegments = tableView._realtime.size();
          int onlineSegments = countSegmentsInState(tableView, ONLINE);
          int consumingSegments = countSegmentsInState(tableView, CONSUMING);
          if (totalSegments != expectation.getExpectedTotalSegments()
              || onlineSegments != expectation._expectedOnlineSegments
              || consumingSegments != expectation._expectedConsumingSegments) {
            LOGGER.warn("Scenario {} is waiting for segment states: expected total/online/consuming={}/{}/{}, "
                    + "actual={}/{}/{}", expectation._scenarioName, expectation.getExpectedTotalSegments(),
                expectation._expectedOnlineSegments, expectation._expectedConsumingSegments, totalSegments,
                onlineSegments, consumingSegments);
            allStatesMatch = false;
          }
        } catch (Exception e) {
          LOGGER.warn("Could not query segment states for scenario {} on table {}", expectation._scenarioName,
              tableName, e);
          allStatesMatch = false;
        }
      }
      return allStatesMatch;
    }, 1000L, 60_000L, "Wait for all scenario segment states: " + expectationsByTable.keySet());

    for (Map.Entry<String, SegmentStateExpectation> entry : expectationsByTable.entrySet()) {
      SegmentStateExpectation expectation = entry.getValue();
      validateSegmentStates(entry.getKey(), expectation._expectedOnlineSegments,
          expectation._expectedConsumingSegments, expectation._scenarioName);
    }
  }

  private void validateSegmentStates(String tableName, int expectedOnlineSegments, int expectedConsumingSegments,
      String scenarioName)
      throws IOException {
    TableView tableView = getExternalView(tableName, TableType.REALTIME);
    Assert.assertEquals(tableView._realtime.size(), expectedOnlineSegments + expectedConsumingSegments,
        "Unexpected total segment count for " + scenarioName);

    List<String> onlineSegments = tableView._realtime.entrySet().stream()
        .filter(x -> x.getValue().containsValue(ONLINE))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    Assert.assertEquals(onlineSegments.size(), expectedOnlineSegments,
        "Unexpected online segment count for " + scenarioName);

    List<String> consumingSegments = tableView._realtime.entrySet().stream()
        .filter(x -> x.getValue().containsValue(CONSUMING))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    Assert.assertEquals(consumingSegments.size(), expectedConsumingSegments,
        "Unexpected consuming segment count for " + scenarioName);
  }

  private static int countSegmentsInState(TableView tableView, String state) {
    return (int) tableView._realtime.values().stream().filter(instanceStates -> instanceStates.containsValue(state))
        .count();
  }

  private void waitForIdealStateToMatchExternalView(Iterable<String> tableNames) {
    TestUtils.waitForCondition(aVoid -> {
      boolean allTablesConverged = true;
      for (String tableName : tableNames) {
        try {
          TableView idealState = getOrCreateAdminClient().getTableClient().getIdealStateObject(tableName);
          TableView externalView = getOrCreateAdminClient().getTableClient().getExternalViewObject(tableName);
          if (!Objects.equals(idealState._realtime, externalView._realtime)) {
            LOGGER.warn("Waiting for IdealState and ExternalView to converge for scenario table {}: {}", tableName,
                findRealtimeStateMismatch(idealState._realtime, externalView._realtime));
            allTablesConverged = false;
          }
        } catch (Exception e) {
          LOGGER.warn("Could not query IdealState and ExternalView for scenario table {}", tableName, e);
          allTablesConverged = false;
        }
      }
      return allTablesConverged;
    }, 1000L, 60_000L, "Wait for scenario IdealState and ExternalView convergence");
  }

  private static String findRealtimeStateMismatch(Map<String, Map<String, String>> idealState,
      Map<String, Map<String, String>> externalView) {
    if (idealState == null) {
      return "realtime IdealState map is missing";
    }
    if (externalView == null) {
      return "realtime ExternalView map is missing";
    }
    for (Map.Entry<String, Map<String, String>> partitionEntry : idealState.entrySet()) {
      String partitionName = partitionEntry.getKey();
      Map<String, String> externalViewStates = externalView.get(partitionName);
      if (!Objects.equals(partitionEntry.getValue(), externalViewStates)) {
        return "partition=" + partitionName + ", ideal=" + partitionEntry.getValue() + ", external="
            + externalViewStates;
      }
    }
    for (String partitionName : externalView.keySet()) {
      if (!idealState.containsKey(partitionName)) {
        return "ExternalView-only partition=" + partitionName + ", states=" + externalView.get(partitionName);
      }
    }
    return "realtime maps differ";
  }

  /// start and end offsets are essentially the start row index and end row index of the file
  ///
  /// @param startOffset - inclusive
  /// @param endOffset   - exclusive
  private void publishRecordsToKinesis(int startOffset, int endOffset)
      throws Exception {
    InputStream inputStream = RealtimeKinesisIntegrationTest.class.getClassLoader()
        .getResourceAsStream(KinesisShardChangeTest.DATA_FILE_PATH);
    assert inputStream != null;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      String line;
      int count = 0;
      while ((line = br.readLine()) != null) {
        // Skip the first startOffset lines
        if (count < startOffset) {
          count++;
          continue;
        }
        if (count++ >= endOffset) {
          break;
        }
        JsonNode data = JsonUtils.stringToJsonNode(line);
        PutRecordResponse putRecordResponse = putRecord(line, data.get("Origin").textValue());
        if (putRecordResponse.sdkHttpResponse().statusCode() != 200) {
          throw new RuntimeException("Failed to put record " + line + " to Kinesis stream with status code: "
              + putRecordResponse.sdkHttpResponse().statusCode());
        }
      }
    }
  }

  private void waitForRecordsToBeConsumed(String tableName, int expectedNumRecords)
      throws InterruptedException {
    waitForRecordsToBeConsumed(Map.of(tableName, expectedNumRecords));
  }

  private void waitForRecordsToBeConsumed(Map<String, Integer> expectedRecordsByTable)
      throws InterruptedException {
    TestUtils.waitForCondition(aVoid -> {
      boolean allCountsMatch = true;
      for (Map.Entry<String, Integer> entry : expectedRecordsByTable.entrySet()) {
        String tableName = entry.getKey();
        int expectedNumRecords = entry.getValue();
        try {
          long count = getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getResultSet(0).getLong(0);
          if (count != expectedNumRecords) {
            LOGGER.warn("Scenario table {} expected {} records, but got {}. Retrying", tableName,
                expectedNumRecords, count);
            allCountsMatch = false;
          }
        } catch (Exception e) {
          LOGGER.warn("Could not query scenario table {} while waiting for {} records", tableName,
              expectedNumRecords, e);
          allCountsMatch = false;
        }
      }
      return allCountsMatch;
    }, 2000L, 60_000L, "Wait for all scenario records to be ingested: " + expectedRecordsByTable);

    // Use one stability window for the whole group to ensure none of the tables ingest additional records.
    Thread.sleep(2000);
    for (Map.Entry<String, Integer> entry : expectedRecordsByTable.entrySet()) {
      String tableName = entry.getKey();
      long count = getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getResultSet(0).getLong(0);
      Assert.assertEquals(count, entry.getValue().longValue(),
          "Record count changed during the stability window for scenario table " + tableName);
    }
  }

  private void applyOffsetAction(ShardOffsetScenario scenario, String offsetCriteria, String phase)
      throws Exception {
    if (offsetCriteria != null) {
      LOGGER.info("Applying {} offset during the {} phase for {}", offsetCriteria, phase, scenario._name);
      pauseTable(scenario._tableName);
      resumeTable(scenario._tableName, offsetCriteria);
    } else {
      LOGGER.info("Running realtime segment validation during the {} phase for {}", phase, scenario._name);
      runRealtimeSegmentValidationTask(scenario._tableName);
    }
  }

  private void createDefaultSchemaAndTable()
      throws IOException {
    Schema schema = createSchema(SCHEMA_FILE_PATH);
    schema.setSchemaName(getTableName());
    addTrackedSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(null);
    addTrackedTable(tableConfig);
  }

  @Override
  public Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigs = super.getStreamConfigs();
    // All scenario tables share a JVM-wide limiter for each stream/shard/request tuple. Use a consistent test-only
    // value matching the aggregate load of the eight formerly independent scenarios so the first table cannot limit
    // the whole group to the production default of one request per second.
    streamConfigs.put(KinesisConfig.RPS_LIMIT, "8.0");
    return streamConfigs;
  }

  private void createNewSchemaAndTable(String name, String offsetCriteria)
      throws IOException {
    Schema schema = createSchema(SCHEMA_FILE_PATH);
    schema.setSchemaName(name);
    addTrackedSchema(schema);

    TableConfigBuilder tableConfigBuilder = getTableConfigBuilder(TableType.REALTIME);
    tableConfigBuilder.setTableName(name);
    Map<String, String> streamConfigs = getStreamConfigs();
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), offsetCriteria);
    tableConfigBuilder.setStreamConfigs(streamConfigs);
    TableConfig tableConfig = tableConfigBuilder.build();
    addTrackedTable(tableConfig);
  }

  @Override
  public List<String> getNoDictionaryColumns() {
    return List.of();
  }

  @Override
  public String getSortedColumn() {
    return null;
  }

  @Override
  protected String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected String getKinesisStreamName() {
    return STREAM_NAME;
  }

  /// Expected ExternalView state for one independently asserted scenario table.
  private static final class SegmentStateExpectation {
    private final String _scenarioName;
    private final int _expectedOnlineSegments;
    private final int _expectedConsumingSegments;

    private SegmentStateExpectation(String scenarioName, int expectedOnlineSegments, int expectedConsumingSegments) {
      _scenarioName = scenarioName;
      _expectedOnlineSegments = expectedOnlineSegments;
      _expectedConsumingSegments = expectedConsumingSegments;
    }

    private int getExpectedTotalSegments() {
      return _expectedOnlineSegments + _expectedConsumingSegments;
    }
  }

  /// One independently asserted table observing a shared split or merge timeline.
  private static final class ShardOffsetScenario {
    private final String _name;
    private final String _tableName;
    private final String _firstOffsetCriteria;
    private final String _secondOffsetCriteria;
    private final int _firstExpectedRecords;
    private final int _secondExpectedRecords;
    private final int _expectedOnlineSegments;
    private final int _expectedConsumingSegments;

    private ShardOffsetScenario(String operation, String firstOffsetCriteria, String secondOffsetCriteria,
        int firstExpectedRecords, int secondExpectedRecords, int expectedOnlineSegments,
        int expectedConsumingSegments) {
      String firstAction = actionName(firstOffsetCriteria);
      String secondAction = actionName(secondOffsetCriteria);
      _name = operation + " [first=" + firstAction + ", second=" + secondAction + "]";
      _tableName = TABLE_NAME + "_" + operation + "_" + firstAction + "_" + secondAction;
      _firstOffsetCriteria = firstOffsetCriteria;
      _secondOffsetCriteria = secondOffsetCriteria;
      _firstExpectedRecords = firstExpectedRecords;
      _secondExpectedRecords = secondExpectedRecords;
      _expectedOnlineSegments = expectedOnlineSegments;
      _expectedConsumingSegments = expectedConsumingSegments;
    }

    private static String actionName(String offsetCriteria) {
      return offsetCriteria != null ? offsetCriteria : "rvm";
    }
  }

  private void stopPublisherThread() {
    Thread publisherThread = _publisherThread;
    if (publisherThread == null) {
      throwIfPublisherFailed();
      return;
    }
    publisherThread.interrupt();
    boolean interrupted = Thread.interrupted();
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (publisherThread.isAlive()) {
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        break;
      }
      try {
        publisherThread.join(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    if (publisherThread.isAlive()) {
      throw new IllegalStateException("Kinesis publisher thread did not stop within 10 seconds");
    }
    _publisherThread = null;
    throwIfPublisherFailed();
  }

  private void throwIfPublisherFailed() {
    Throwable publisherFailure = _publisherFailure;
    _publisherFailure = null;
    if (publisherFailure != null) {
      throw new AssertionError("Kinesis publisher thread failed", publisherFailure);
    }
  }
}
