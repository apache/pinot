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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.controller.api.resources.TableViews;
import org.apache.pinot.integration.tests.realtime.ingestion.utils.KinesisUtils;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;


public class KinesisShardChangeTest extends BaseKinesisIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisShardChangeTest.class);

  // TODO - Check with full airlineStats data with new local stack version
  private static final String SCHEMA_FILE_PATH = "kinesis/airlineStats_data_reduced.schema";
  private static final String DATA_FILE_PATH = "kinesis/airlineStats_data_reduced.json";

  @BeforeMethod
  public void beforeTest()
      throws IOException {
    createStream(2);
    addSchema(createSchema(SCHEMA_FILE_PATH));
    TableConfig tableConfig = createRealtimeTableConfig(null);
    addTableConfig(tableConfig);
  }

  @AfterMethod
  public void afterTest()
      throws IOException {
    dropRealtimeTable(getTableName());
    deleteSchema(getTableName());
    deleteStream();
  }

  /**
   * Data provider for shard split and merge tests with different offset combinations.
   */
  @DataProvider(name = "shardOffsetCombinations")
  public Object[][] shardOffsetCombinations() {
    return new Object[][]{
        {"split", "smallest", "lastConsumed", 100, 250, 4, 4},
        {"split", "smallest", null, 100, 250, 4, 4},
        {"split", "largest", "lastConsumed", 50, 200, 2, 4},
        {"split", "largest", null, 50, 200, 2, 4},
        {"split", "lastConsumed", "lastConsumed", 200, 200, 6, 4},
        {"split", "lastConsumed", "largest", 200, 200, 6, 0},
        {"split", "lastConsumed", null, 200, 200, 2, 4},
        {"split", null, null, 200, 200, 2, 4},
        {"merge", "smallest", "lastConsumed", 100, 250, 4, 1},
        {"merge", "smallest", null, 100, 250, 4, 1},
        {"merge", "largest", "lastConsumed", 50, 200, 2, 1},
        {"merge", "largest", null, 50, 200, 2, 1},
        {"merge", "lastConsumed", "lastConsumed", 200, 200, 3, 1},
        {"merge", "lastConsumed", "largest", 200, 200, 3, 0},
        {"merge", "lastConsumed", null, 200, 200, 2, 1},
        {"merge", null, null, 200, 200, 2, 1},
    };
  }

  /**
   * Test case to validate shard split/merge behavior with different offset combinations.
   * The expectation is that
   * 1. when "smallest" offset is used, the old parent shards would be consumed first.
   *    New shards will not be consumed until RVM is run or resume() is called with lastConsumed / the largest offset
   * 2. when "largest" offset is used, only new records would be consumed and all prior records pushed to kinesis
   *    would be skipped.
   * 3. when "lastConsumed" offset is used, data would be consumed based on the last consumed offset.
   * @param operation - "split" or "merge"
   * @param firstOffsetCriteria - Offset criteria for the first resume call. If its null, we will trigger Realtime
   *                              Segment Validation Manager
   * @param secondOffsetCriteria - Offset criteria for the second resume call. If its null, we will trigger Realtime
   *                               Segment Validation Manager
   * @param firstExpectedRecords - Expected records after the first resume
   * @param secondExpectedRecords - Expected records after the second resume
   * @param expectedOnlineSegments - Expected number of online segments after the second resume
   * @param expectedConsumingSegments - Expected Number of consuming segments after the second resume
   */
  @Test(dataProvider = "shardOffsetCombinations")
  public void testShardOperationsWithOffsets(String operation, String firstOffsetCriteria, String secondOffsetCriteria,
      int firstExpectedRecords, int secondExpectedRecords, int expectedOnlineSegments,
      int expectedConsumingSegments)
      throws Exception {

    // Publish initial records and wait for them to be consumed
    publishRecordsToKinesis(DATA_FILE_PATH, 0, 50);
    waitForRecordsToBeConsumed(getTableName(), 50); // pinot has created 2 segments

    // Perform shard operation (split or merge)
    if ("split".equals(operation)) {
      KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 0); // splits shard 0 into shard 2 & 3
      KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 1); // splits shard 1 into shard 4 & 5
    } else if ("merge".equals(operation)) {
      KinesisUtils.mergeShards(_kinesisClient, STREAM_NAME, 0, 1); // merges shard 0 & 1 into shard 2
    }

    // Publish more records after shard operation. These will go to the new shards
    publishRecordsToKinesis(DATA_FILE_PATH, 50, 200);

    if (firstOffsetCriteria != null) {
      // Pause and resume with the first offset criteria
      pauseTable(getTableName()); // This will commit the current segments
      resumeTable(getTableName(), firstOffsetCriteria);
    } else {
      runRealtimeSegmentValidationTask(getTableName());
    }
    waitForRecordsToBeConsumed(getTableName(), firstExpectedRecords); // Pinot has created Y new segments

    if (secondOffsetCriteria != null) {
      // Pause and resume with the second offset criteria
      pauseTable(getTableName()); // This will commit the current segments
      resumeTable(getTableName(), secondOffsetCriteria);
    } else {
      runRealtimeSegmentValidationTask(getTableName());
    }
    // Pinot will now consume X records
    waitForRecordsToBeConsumed(getTableName(), secondExpectedRecords); // Pinot has created Y new segments

    // Validate the final state of segments
    validateSegmentStates(getTableName(), expectedOnlineSegments, expectedConsumingSegments);
  }

  /**
   * Data provider for table recreation tests with different offset combinations.
   */
  @DataProvider(name = "initialOffsetCombinations")
  public Object[][] initialOffsetCombinations() {
    return new Object[][]{
        {"smallest", 50, 200},
        {"largest", 50, 200}, // TODO - Fix. Table created with largest offset should not consume old records
        {"lastConsumed", 50, 200}
    };
  }

  @Test(dataProvider = "initialOffsetCombinations")
  public void testNewTable(String offsetCriteria, int firstExpectedRecords, int secondExpectedRecords)
      throws Exception {
    // Publish initial records
    publishRecordsToKinesis(DATA_FILE_PATH, 0, 50);

    // Split the shards
    KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 0); // splits shard 0 into shard 2 & 3
    KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 1); // splits shard 1 into shard 4 & 5

    // new table is created with defined offset criteria but listening to the original stream
    String name = getTableName() + "_" + offsetCriteria;
    createNewSchemaAndTable(name, offsetCriteria);

    waitForRecordsToBeConsumed(name, firstExpectedRecords);

    // publish more records. These will go to the new shards
    publishRecordsToKinesis(DATA_FILE_PATH, 50, 200);
    waitForRecordsToBeConsumed(name, firstExpectedRecords); // pinot doesn't listen to new shards yet. 1357

    // Trigger RVM. This will commit the current segments and start consuming from the new shards
    runRealtimeSegmentValidationTask(name);
    waitForRecordsToBeConsumed(name, secondExpectedRecords);

    // Validate the final state of segments
    validateSegmentStates(name, 2, 4);

    dropNewSchemaAndTable(name, offsetCriteria);
  }

  /**
   * Test case to first split shards, then merge some shards.
   * For the sake of brevity, we will only test by calling Realtime Validation Manager
   * Individually, pause and resume have been verified for shard split / merge operations
   */
  @Test
  public void testSplitAndMergeShards()
      throws Exception {
    // Publish initial records
    publishRecordsToKinesis(DATA_FILE_PATH, 0, 50);
    waitForRecordsToBeConsumed(getTableName(), 50); // pinot has created 2 segments

    // Split the shards
    KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 0); // splits shard 0 into shard 2 & 3
    KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 1); // splits shard 1 into shard 4 & 5

    // Publish more records after shard operation. These will go to the new shards
    publishRecordsToKinesis(DATA_FILE_PATH, 50, 175);

    // Merge some shards
    KinesisUtils.mergeShards(_kinesisClient, STREAM_NAME, 2, 3); // merges shard 2 & 3 into shard 6
    KinesisUtils.mergeShards(_kinesisClient, STREAM_NAME, 4, 5); // merges shard 4 & 5 into shard 7

    // Publish more records after shard operation. These will go to the new shards
    publishRecordsToKinesis(DATA_FILE_PATH, 175, 200);

    // Trigger RVM. This will commit segments 0 and 1 and start consuming from shards 2-5
    runRealtimeSegmentValidationTask(getTableName());
    waitForRecordsToBeConsumed(getTableName(), 175);

    // Trigger RVM. This will commit segments 2-5 and start consuming from shards 6-7
    runRealtimeSegmentValidationTask(getTableName());
    waitForRecordsToBeConsumed(getTableName(), 200);

    // Validate that 8 segments are created in total
    validateSegmentStates(getTableName(), 6, 2);
  }

  private void validateSegmentStates(String tableName, int expectedOnlineSegments, int expectedConsumingSegments)
      throws IOException {
    TableViews.TableView tableView = getExternalView(tableName, TableType.REALTIME);
    Assert.assertEquals(tableView._realtime.size(), expectedOnlineSegments + expectedConsumingSegments);

    List<String> onlineSegments = tableView._realtime.entrySet().stream()
        .filter(x -> x.getValue().containsValue("ONLINE"))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    Assert.assertEquals(onlineSegments.size(), expectedOnlineSegments);

    List<String> consumingSegments = tableView._realtime.entrySet().stream()
        .filter(x -> x.getValue().containsValue("CONSUMING"))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    Assert.assertEquals(consumingSegments.size(), expectedConsumingSegments);
  }

  /**
   * start and end offsets are essentially the start row index and end row index of the file
   * @param startOffset - inclusive
   * @param endOffset - exclusive
   * @return the number of records published to Kinesis
   */
  private int publishRecordsToKinesis(String dataFilePath, int startOffset, int endOffset)
      throws Exception {
    InputStream inputStream =
        RealtimeKinesisIntegrationTest.class.getClassLoader().getResourceAsStream(dataFilePath);
    int numRecordsPushed = 0;
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
        if (putRecordResponse.sdkHttpResponse().statusCode() == 200) {
          numRecordsPushed++;
        } else {
          throw new RuntimeException("Failed to put record " + line + " to Kinesis stream with status code: "
              + putRecordResponse.sdkHttpResponse().statusCode());
        }
      }
    }
    return numRecordsPushed;
  }

  private void waitForRecordsToBeConsumed(String tableName, int expectedNumRecords)
      throws InterruptedException {
    TestUtils.waitForCondition(aVoid -> {
      try {
        long count = getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getResultSet(0).getLong(0);
        if (count != expectedNumRecords) {
          LOGGER.warn("Expected {} records, but got {} records. Retrying", expectedNumRecords, count);
        }
        return count == expectedNumRecords;
      } catch (Exception e) {
        return false;
      }
    }, 1000, 60_000L, "Wait for all records to be ingested");
    // Sleep for few secs and validate the count again (to ensure no more records are ingested)
    Thread.sleep(2000);
    long count = getPinotConnection().execute("SELECT COUNT(*) FROM " + tableName).getResultSet(0).getLong(0);
    Assert.assertEquals(count, expectedNumRecords);
  }

  private void createNewSchemaAndTable(String name, String offsetCriteria)
      throws IOException {
    Schema schema = createSchema(SCHEMA_FILE_PATH);
    schema.setSchemaName(name);
    addSchema(schema);

    TableConfigBuilder tableConfigBuilder = getTableConfigBuilder(TableType.REALTIME);
    tableConfigBuilder.setTableName(name);
    Map<String, String> streamConfigs = getStreamConfigs();
    streamConfigs.put(StreamConfigProperties.constructStreamProperty(STREAM_TYPE,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), offsetCriteria);
    tableConfigBuilder.setStreamConfigs(streamConfigs);
    TableConfig tableConfig = tableConfigBuilder.build();
    addTableConfig(tableConfig);
  }

  private void dropNewSchemaAndTable(String name, String offsetCriteria)
      throws IOException {
    dropRealtimeTable(name);
    deleteSchema(name);
  }

  @Override
  public List<String> getNoDictionaryColumns() {
    return Collections.emptyList();
  }

  @Override
  public String getSortedColumn() {
    return null;
  }
}
