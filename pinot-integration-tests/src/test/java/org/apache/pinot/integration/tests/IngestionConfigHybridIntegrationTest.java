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
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Tests ingestion configs on a hybrid table
 */
@Test(suiteName = "integration-suite-1", groups = {"integration-suite-1"})
public class IngestionConfigHybridIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int NUM_REALTIME_SEGMENTS = 6;
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  // query result of SELECT COUNT(*) FROM mytable WHERE AirlineID != 19393 AND ArrDelayMinutes > 5 on unfiltered data
  private static final long FILTERED_COUNT_STAR_RESULT = 24047L;

  @Override
  protected String getTimeColumnName() {
    return TIME_COLUMN_NAME;
  }

  protected long getCountStarResult() {
    return FILTERED_COUNT_STAR_RESULT;
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(Collections.singletonList(getStreamConfigMap())));
    FilterConfig filterConfig =
        new FilterConfig("Groovy({AirlineID == 19393 || ArrDelayMinutes <= 5 }, AirlineID, ArrDelayMinutes)");
    ingestionConfig.setFilterConfig(filterConfig);
    List<TransformConfig> transformConfigs = Arrays.asList(
        new TransformConfig("AmPm", "Groovy({DepTime < 1200 ? \"AM\": \"PM\"}, DepTime)"),
        new TransformConfig("millisSinceEpoch", "fromEpochDays(DaysSinceEpoch)"),
        new TransformConfig("lowerCaseDestCityName", "lower(DestCityName)"));
    ingestionConfig.setTransformConfigs(transformConfigs);
    return ingestionConfig;
  }

  @Override
  protected Map<String, String> getStreamConfigs() {
    return null;
  }

  @Override
  protected Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(DEFAULT_SCHEMA_NAME)
        .addSingleValueDimension("AirlineID", FieldSpec.DataType.LONG)
        .addSingleValueDimension("DepTime", FieldSpec.DataType.INT)
        .addSingleValueDimension("AmPm", FieldSpec.DataType.STRING)
        .addSingleValueDimension("lowerCaseDestCityName", FieldSpec.DataType.STRING)
        .addMetric("ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .addDateTime("millisSinceEpoch", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:DAYS").build();
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    System.out.println("this.getClass().getName() = " + this.getClass().getName());
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    // Start Zk and Kafka
    startZk();
    startKafka();

    // Start the Pinot cluster
    startController();
    startBroker();
    startServer();

    List<File> avroFiles = getAllAvroFiles();
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles, NUM_REALTIME_SEGMENTS);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
    addTableConfig(createRealtimeTableConfig(realtimeAvroFiles.get(0)));

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _segmentDir,
        _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Push data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Select column created with transform function
    String sqlQuery = "Select millisSinceEpoch from " + DEFAULT_TABLE_NAME;
    JsonNode response = postQuery(sqlQuery);
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "millisSinceEpoch");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG");

    // Select column created with transform function
    sqlQuery = "Select AmPm, DepTime from " + DEFAULT_TABLE_NAME;
    response = postQuery(sqlQuery);
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "AmPm");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(1).asText(), "DepTime");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "STRING");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(1).asText(), "INT");
    for (int i = 0; i < response.get("resultTable").get("rows").size(); i++) {
      String amPm = response.get("resultTable").get("rows").get(i).get(0).asText();
      int depTime = response.get("resultTable").get("rows").get(i).get(1).asInt();
      Assert.assertEquals(amPm, (depTime < 1200) ? "AM" : "PM");
    }

    // Select column created with transform function - offline table
    sqlQuery = "Select AmPm, DepTime from " + DEFAULT_TABLE_NAME + "_OFFLINE";
    response = postQuery(sqlQuery);
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "AmPm");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(1).asText(), "DepTime");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "STRING");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(1).asText(), "INT");
    for (int i = 0; i < response.get("resultTable").get("rows").size(); i++) {
      String amPm = response.get("resultTable").get("rows").get(i).get(0).asText();
      int depTime = response.get("resultTable").get("rows").get(i).get(1).asInt();
      Assert.assertEquals(amPm, (depTime < 1200) ? "AM" : "PM");
    }

    // Select column created with transform - realtime table
    sqlQuery = "Select AmPm, DepTime from " + DEFAULT_TABLE_NAME + "_REALTIME";
    response = postQuery(sqlQuery);
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "AmPm");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(1).asText(), "DepTime");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "STRING");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(1).asText(), "INT");
    for (int i = 0; i < response.get("resultTable").get("rows").size(); i++) {
      String amPm = response.get("resultTable").get("rows").get(i).get(0).asText();
      int depTime = response.get("resultTable").get("rows").get(i).get(1).asInt();
      Assert.assertEquals(amPm, (depTime < 1200) ? "AM" : "PM");
    }

    // Check there's no values that should've been filtered
    sqlQuery = "Select * from " + DEFAULT_TABLE_NAME + "  where AirlineID = 19393 or ArrDelayMinutes <= 5";
    response = postQuery(sqlQuery);
    Assert.assertEquals(response.get("resultTable").get("rows").size(), 0);

    // Check there's no values that should've been filtered - realtime table
    sqlQuery =
        "Select * from " + DEFAULT_TABLE_NAME + "_REALTIME" + "  where AirlineID = 19393 or ArrDelayMinutes <= 5";
    response = postQuery(sqlQuery);
    Assert.assertEquals(response.get("resultTable").get("rows").size(), 0);

    // Check there's no values that should've been filtered - offline table
    sqlQuery = "Select * from " + DEFAULT_TABLE_NAME + "_OFFLINE" + "  where AirlineID = 19393 or ArrDelayMinutes <= 5";
    response = postQuery(sqlQuery);
    Assert.assertEquals(response.get("resultTable").get("rows").size(), 0);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());
    dropRealtimeTable(getTableName());
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
  }
}
