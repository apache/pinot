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
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
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
  public void afterTest() {
    deleteStream();
  }

  /**
   * Test case to validate behaviour when increasing shards and consuming from the smallest offset
   * The expectation is that when a shard is split, the old shard would be consumed first.
   * New shards will not be consumed until RVM is run or resume() is called with lastConsumed / the largest offset
   */
  @Test
  public void testIncreaseShardsWithSmallestOffset_1()
      throws Exception {

    // Publish 50 records to Kinesis and wait for them to be consumed before splitting the shards
    publishRecordsToKinesis(DATA_FILE_PATH, 0, 50);
    waitForRecordsToBeConsumed(getTableName(), 50); // pinot has created 2 segments

    // Split the shards (0-1) and publish more records. These will go to the new shards (2-5)
    KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 0); // splits shard 0 into shard 2 & 3
    KinesisUtils.splitNthShard(_kinesisClient, STREAM_NAME, 1); // splits shard 1 into shard 4 & 5
    publishRecordsToKinesis(DATA_FILE_PATH, 50, 200);

    // Pause and then resume with smallest offset
    pauseTable(getTableName()); // This will commit the current segments
    resumeTable(getTableName(), "smallest");
    // Pinot will ingest the older shards first (50 records) and stop consumption until more action is taken
    waitForRecordsToBeConsumed(getTableName(), 100); // pinot has created 2 new segments

    // Pause and resume with lastConsumed offset
    pauseTable(getTableName()); // This will commit the current segments
    resumeTable(getTableName(), "lastConsumed");
    // Pinot will now consume the new shards (150 records)
    waitForRecordsToBeConsumed(getTableName(), 250); // pinot has created 4 new segments

    TableViews.TableView tableView = getExternalView(getTableName());
    Assert.assertEquals(tableView._realtime.size(), 8);

    List<String> onlineSegments = tableView._realtime.entrySet().stream().filter(x -> x.getValue().containsValue("ONLINE")).map(
        Map.Entry::getKey).collect(Collectors.toList());
    // Assert that all online segments start with getTableName()__0 or with getTableName()__1
    Assert.assertTrue(onlineSegments.stream().allMatch(x -> x.startsWith(getTableName() + "__0") || x.startsWith(getTableName() + "__1")));
    Assert.assertEquals(onlineSegments.size(), 4);

    List<String> consumingSegments = tableView._realtime.entrySet().stream().filter(x -> x.getValue().containsValue("CONSUMING")).map(
        Map.Entry::getKey).collect(Collectors.toList());
    // Assert that no consuming segments start with getTableName()__0 or with getTableName()__1
    Assert.assertTrue(consumingSegments.stream().noneMatch(x -> x.startsWith(getTableName() + "__0") || x.startsWith(getTableName() + "__1")));
    Assert.assertEquals(consumingSegments.size(), 4);

  }

  /**
   * start and end offsets are essentially the start row index and end row index of the file
   * @param startOffset - inclusive
   * @param endOffset - exclusive
   * @return the number of records published to Kinesis
   */
  private int publishRecordsToKinesis(String dataFilePath, int startOffset, int endOffset) throws Exception {
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

  private void waitForRecordsToBeConsumed(String tableName, int expectedNumRecords) throws InterruptedException {
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

  @Override
  public List<String> getNoDictionaryColumns() {
    return Collections.emptyList();
  }

  @Override
  public String getSortedColumn() {
    return null;
  }

}
