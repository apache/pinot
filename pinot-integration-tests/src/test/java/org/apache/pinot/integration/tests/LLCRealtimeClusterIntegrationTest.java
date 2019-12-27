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
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.ZNRecord;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that extends RealtimeClusterIntegrationTest but uses low-level Kafka consumer.
 * TODO: Add separate module-level tests and remove the randomness of this test
 */
public class LLCRealtimeClusterIntegrationTest extends RealtimeClusterIntegrationTest {
  private static final String CONSUMER_DIRECTORY = "/tmp/consumer-test";
  private static final String TEST_UPDATED_INVERTED_INDEX_QUERY =
      "SELECT COUNT(*) FROM mytable WHERE DivActualElapsedTime = 305";
  private static final List<String> UPDATED_INVERTED_INDEX_COLUMNS = Collections.singletonList("DivActualElapsedTime");
  private static final long RANDOM_SEED = System.currentTimeMillis();
  private static final Random RANDOM = new Random(RANDOM_SEED);

  private final boolean _isDirectAlloc = RANDOM.nextBoolean();
  private final boolean _isConsumerDirConfigured = RANDOM.nextBoolean();
  private final boolean _enableLeadControllerResource = RANDOM.nextBoolean();
  private final long _startTime = System.currentTimeMillis();

  @BeforeClass
  @Override
  public void setUp()
      throws Exception {
    System.out.println(String.format(
        "Using random seed: %s, isDirectAlloc: %s, isConsumerDirConfigured: %s, enableLeadControllerResource: %s",
        RANDOM_SEED, _isDirectAlloc, _isConsumerDirConfigured, _enableLeadControllerResource));

    // Remove the consumer directory
    File consumerDirectory = new File(CONSUMER_DIRECTORY);
    if (consumerDirectory.exists()) {
      FileUtils.deleteDirectory(consumerDirectory);
    }

    super.setUp();
  }

  @Override
  public void startController() {
    ControllerConf controllerConfig = getDefaultControllerConfiguration();
    controllerConfig.setHLCTablesAllowed(false);
    controllerConfig.setSplitCommit(true);
    startController(controllerConfig);
    enableResourceConfigForLeadControllerResource(_enableLeadControllerResource);
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Nullable
  @Override
  protected String getLoadMode() {
    return "MMAP";
  }

  @Override
  protected void overrideServerConf(Configuration configuration) {
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_ALLOCATION, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_REALTIME_OFFHEAP_DIRECT_ALLOCATION, _isDirectAlloc);
    if (_isConsumerDirConfigured) {
      configuration.setProperty(CommonConstants.Server.CONFIG_OF_CONSUMER_DIR, CONSUMER_DIRECTORY);
    }
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_SPLIT_COMMIT, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_COMMIT_END_WITH_METADATA, true);
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_INSTANCE_RELOAD_CONSUMING_SEGMENT, true);
  }

  @Test
  public void testConsumerDirectoryExists() {
    File consumerDirectory = new File(CONSUMER_DIRECTORY, "mytable_REALTIME");
    assertEquals(consumerDirectory.exists(), _isConsumerDirConfigured,
        "The off heap consumer directory does not exist");
  }

  @Test
  public void testSegmentFlushSize() {
    String zkSegmentsPath = "/SEGMENTS/" + TableNameBuilder.REALTIME.tableNameWithType(getTableName());
    List<String> segmentNames = _propertyStore.getChildNames(zkSegmentsPath, 0);
    for (String segmentName : segmentNames) {
      ZNRecord znRecord = _propertyStore.get(zkSegmentsPath + "/" + segmentName, null, 0);
      assertEquals(znRecord.getSimpleField(CommonConstants.Segment.FLUSH_THRESHOLD_SIZE),
          Integer.toString(getRealtimeSegmentFlushSize() / getNumKafkaPartitions()),
          "Segment: " + segmentName + " does not have the expected flush size");
    }
  }

  @Test
  public void testInvertedIndexTriggering()
      throws Exception {
    long numTotalDocs = getCountStarResult();

    JsonNode queryResponse = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertTrue(queryResponse.get("numEntriesScannedInFilter").asLong() > 0L);

    updateRealtimeTableConfig(getTableName(), UPDATED_INVERTED_INDEX_COLUMNS, null);
    sendPostRequest(_controllerRequestURLBuilder.forTableReload(getTableName(), "realtime"), null);

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResponse1 = postQuery(TEST_UPDATED_INVERTED_INDEX_QUERY);
        // Total docs should not change during reload
        assertEquals(queryResponse1.get("totalDocs").asLong(), numTotalDocs);
        assertEquals(queryResponse1.get("numConsumingSegmentsQueried").asLong(), 2);
        assertTrue(queryResponse1.get("minConsumingFreshnessTimeMs").asLong() > _startTime);
        assertTrue(queryResponse1.get("minConsumingFreshnessTimeMs").asLong() < System.currentTimeMillis());
        return queryResponse1.get("numEntriesScannedInFilter").asLong() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 600_000L, "Failed to generate inverted index");
  }

  @Test(expectedExceptions = IOException.class)
  public void testAddHLCTableShouldFail()
      throws IOException {
    TableConfig tableConfig = new TableConfig.Builder(TableType.REALTIME).setTableName("testTable")
        .setStreamConfigs(Collections.singletonMap("stream.kafka.consumer.type", "HIGHLEVEL")).build();
    sendPostRequest(_controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonConfigString());
  }

  @Test
  public void testReload()
      throws Exception {
    long numTotalDocs = getCountStarResult();
    JsonNode queryResponse = postQuery(String.format(TEST_SELECT_QUERY_TEMPLATE, "*", "mytable"));
    assertEquals(queryResponse.get("totalDocs").asLong(), numTotalDocs);
    assertEquals(queryResponse.get("selectionResults").get("columns").size(), 79);
    // Get the current schema
    Schema schema = Schema.fromString(sendGetRequest(_controllerRequestURLBuilder.forSchemaGet(getTableName())));
    int originalSchemaSize = schema.size();
    // Construct new columns
    Map<String, FieldSpec> newColumnsMap = new HashMap<>();
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.STRING, true);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.INT, true);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.LONG, true);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.FLOAT, true);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.DOUBLE, true);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.BOOLEAN, true);
    addNewField(newColumnsMap, FieldType.METRIC, DataType.INT, true);
    addNewField(newColumnsMap, FieldType.METRIC, DataType.LONG, true);
    addNewField(newColumnsMap, FieldType.METRIC, DataType.FLOAT, true);
    addNewField(newColumnsMap, FieldType.METRIC, DataType.DOUBLE, true);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.STRING, false);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.INT, false);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.LONG, false);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.FLOAT, false);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.DOUBLE, false);
    addNewField(newColumnsMap, FieldType.DIMENSION, DataType.BOOLEAN, false);
    // Add new columns to the schema
    newColumnsMap.values().stream().forEach(schema::addField);
    sendMultipartPutRequest(_controllerRequestURLBuilder.forSchemaUpdate(getTableName()),
        schema.toSingleLineJsonString());

    // Reload the table
    sendPostRequest(_controllerRequestURLBuilder.forTableReload(getTableName(), "realtime"), null);

    // Wait for all segments to finish reloading, and test querying the new columns
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode newQueryResponse = postQuery(String.format(TEST_SELECT_QUERY_TEMPLATE,
            newColumnsMap.keySet().isEmpty() ? schema.getTimeColumnName() : String.join(", ", newColumnsMap.keySet()),
            "mytable"));
        // When querying the new columns after the reload, total number of docs should not change.
        // Also, both consuming and committed segments should be matched in the result
        long totalDocs = newQueryResponse.get("totalDocs").asLong();
        long numSegmentsQueried = newQueryResponse.get("numSegmentsQueried").asLong();
        long numSegmentsMatched = newQueryResponse.get("numSegmentsMatched").asLong();
        if (totalDocs != numTotalDocs || numSegmentsQueried != numSegmentsMatched) {
          return false;
        }
        return true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, 300_000L, "Failed to generate default values for new columns");

    // Test select star query. All columns should be returned with no exceptions.
    queryResponse = postQuery(String.format(TEST_SELECT_QUERY_TEMPLATE, "*", "mytable"));
    assertEquals(queryResponse.get("selectionResults").get("columns").size(),
        originalSchemaSize + newColumnsMap.size());
    assertEquals(queryResponse.get("selectionResults").get("results").size(), numTotalDocs);
    assertEquals(queryResponse.get("exceptions").toString(), "[]");

    // Test more queries on new columns
    queryResponse = postQuery("SELECT MAX(NewSinglevalueIntDimension) from mytable");
    assertEquals(queryResponse.get("aggregationResults").get(0).get("value").asInt(), Integer.MIN_VALUE);

    queryResponse = postQuery("SELECT "
        + "MIN(NewSinglevalueDoubleMetric), MIN(NewSinglevalueFloatMetric), MIN(NewSinglevalueIntMetric), MIN(NewSinglevalueLongMetric) "
        + "FROM mytable GROUP BY "
        + "NewSinglevalueLongDimension, NewSinglevalueIntDimension, NewSinglevalueFloatDimension, NewSinglevalueDoubleDimension, "
        + "NewSinglevalueStringDimension, NewMultivalueIntDimension, NewMultivalueStringDimension");
    JsonNode groupByResultArray = queryResponse.get("aggregationResults");
    Assert.assertEquals(groupByResultArray.size(), 4);
    Assert.assertEquals(groupByResultArray.get(0).get("groupByResult").get(0).get("value").asDouble(), 0.0);
  }
}
