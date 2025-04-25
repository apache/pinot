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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.StringFunctions.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class MultiStageEngineIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String SCHEMA_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  private static final String DEFAULT_DATABASE_NAME = CommonConstants.DEFAULT_DATABASE;
  private static final String DATABASE_NAME = "db1";
  private static final String TABLE_NAME_WITH_DATABASE = DATABASE_NAME + "." + DEFAULT_TABLE_NAME;
  private String _tableName = DEFAULT_TABLE_NAME;

  private static final String DIM_TABLE_DATA_PATH = "dimDayOfWeek_data.csv";
  private static final String DIM_TABLE_SCHEMA_PATH = "dimDayOfWeek_schema.json";
  private static final String DIM_TABLE_TABLE_CONFIG_PATH = "dimDayOfWeek_config.json";
  private static final Integer DIM_NUMBER_OF_RECORDS = 7;

  @Override
  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();

    // Set the multi-stage max server query threads for the cluster, so that we can test the query queueing logic
    // in the MultiStageBrokerRequestHandler
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_MAX_SERVER_QUERY_THREADS, "30");

    startBroker();
    startServer();
    setupTenants();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);

    setupTableWithNonDefaultDatabase(avroFiles);
  }

  @Override
  protected Map<String, String> getExtraQueryProperties() {
    // Increase timeout for this test since it keeps failing in CI.
    Map<String, String> timeoutProperties = new HashMap<>();
    timeoutProperties.put("brokerReadTimeoutMs", "120000");
    timeoutProperties.put("brokerConnectTimeoutMs", "60000");
    timeoutProperties.put("brokerHandshakeTimeoutMs", "60000");
    return timeoutProperties;
  }

  private void setupTableWithNonDefaultDatabase(List<File> avroFiles)
      throws Exception {
    _tableName = TABLE_NAME_WITH_DATABASE;
    String defaultCol = "ActualElapsedTime";
    String customCol = "ActualElapsedTime_2";
    Schema schema = createSchema();
    schema.addField(new MetricFieldSpec(customCol, FieldSpec.DataType.INT));
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    assert tableConfig.getIndexingConfig().getNoDictionaryColumns() != null;
    List<String> noDicCols = new ArrayList<>(DEFAULT_NO_DICTIONARY_COLUMNS);
    noDicCols.add(customCol);
    tableConfig.getIndexingConfig().setNoDictionaryColumns(noDicCols);
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig(customCol, defaultCol)));
    tableConfig.setIngestionConfig(ingestionConfig);
    addTableConfig(tableConfig);

    // Create and upload segments to 'db1.mytable'
    TestUtils.ensureDirectoriesExistAndEmpty(_segmentDir, _tarDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
    _tableName = DEFAULT_TABLE_NAME;
  }

  protected void setupTenants()
      throws IOException {
  }

  @BeforeMethod
  @Override
  public void resetMultiStage() {
    setUseMultiStageQueryEngine(true);
  }

  @Test
  @Override
  public void testHardcodedQueries()
      throws Exception {
    super.testHardcodedQueries();
  }

  @Test
  public void testSingleValueQuery()
      throws Exception {
    String query = "select sum(ActualElapsedTime) from mytable WHERE ActualElapsedTime > "
        + "(select avg(ActualElapsedTime) as avg_profit from mytable)";
    JsonNode jsonNode = postQuery(query);
    long joinResult = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();

    // The query of `SELECT avg(ActualElapsedTime) FROM mytable` is -1412.435033969449
    query = "select sum(ActualElapsedTime) as profit from mytable WHERE ActualElapsedTime > -1412.435033969449";
    jsonNode = postQuery(query);
    long expectedResult = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(joinResult, expectedResult);
  }

  @Test
  @Override
  public void testGeneratedQueries()
      throws Exception {
    super.testGeneratedQueries(false, true);
    super.testGeneratedQueries(true, true);
  }

  // This query was failing in https://github.com/apache/pinot/issues/14375
  @Test
  public void testIssue14375()
      throws Exception {
    String query = "SELECT \"DivArrDelay\", \"Cancelled\", \"DestAirportID\" "
        + "FROM mytable "
        + "WHERE \"OriginStateName\" BETWEEN 'Montana' AND 'South Dakota' "
        + "AND \"OriginAirportID\" BETWEEN 13127 AND 12945 "
        + "OR \"DistanceGroup\" = 4 "
        + "ORDER BY \"Month\", \"LateAircraftDelay\", \"TailNum\" "
        + "LIMIT 10000";
    String h2Query = "SELECT `DivArrDelay`, `Cancelled`, `DestAirportID` "
        + "FROM mytable "
        + "WHERE `OriginStateName` BETWEEN 'Montana' AND 'South Dakota' "
        + "AND `OriginAirportID` BETWEEN 13127 AND 12945 "
        + "OR `DistanceGroup` = 4 "
        + "ORDER BY `Month`, `LateAircraftDelay`, `TailNum` "
        + "LIMIT 10000";

    testQuery(query, h2Query);
  }

  @Test
  public void testQueryOptions()
      throws Exception {
    String pinotQuery = "SET multistageLeafLimit = 1; SELECT * FROM mytable;";
    String h2Query = "SELECT * FROM mytable limit 1";
    testQueryWithMatchingRowCount(pinotQuery, h2Query);
  }

  @Test
  public void testMultiValueColumnSelectionQuery()
      throws Exception {
    String pinotQuery =
        "SELECT DivAirportIDs, DivAirports FROM mytable WHERE DATE_TIME_CONVERT(DaysSinceEpoch, '1:DAYS:EPOCH', "
            + "'1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd''T''HH:mm:ss.SSS''Z''', '1:DAYS') = '2014-09-05T00:00:00.000Z'";
    String h2Query =
        "SELECT DivAirportIDs[1], DivAirports[1] FROM mytable WHERE DaysSinceEpoch = 16318 LIMIT 10000";
    testQueryWithMatchingRowCount(pinotQuery, h2Query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctCountQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String[] numericResultFunctions = new String[]{
        "distinctCount", "distinctCountBitmap", "distinctCountHLL", "segmentPartitionedDistinctCount",
        "distinctCountSmartHLL", "distinctCountThetaSketch", "distinctSum", "distinctAvg"
    };

    double[] expectedNumericResults = new double[]{
        364, 364, 355, 364, 364, 364, 5915969, 16252.662087912087
    };
    Assert.assertEquals(numericResultFunctions.length, expectedNumericResults.length);

    for (int i = 0; i < numericResultFunctions.length; i++) {
      String pinotQuery = "SELECT " + numericResultFunctions[i] + "(DaysSinceEpoch) FROM mytable";
      JsonNode jsonNode = postQuery(pinotQuery);
      Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble(), expectedNumericResults[i]);
    }

    String[] binaryResultFunctions = new String[]{
        "distinctCountRawHLL", "distinctCountRawThetaSketch"
    };
    int[] expectedBinarySizeResults = new int[]{
        360,
        3904
    };
    for (int i = 0; i < binaryResultFunctions.length; i++) {
      String pinotQuery = "SELECT " + binaryResultFunctions[i] + "(DaysSinceEpoch) FROM mytable";
      JsonNode jsonNode = postQuery(pinotQuery);
      Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asText().length(),
          expectedBinarySizeResults[i]);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMultiValueColumnAggregationQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    String[] multiValueFunctions = new String[]{
        "sumMV", "countMV", "minMV", "maxMV", "avgMV", "minMaxRangeMV", "distinctCountMV", "distinctCountBitmapMV",
        "distinctCountHLLMV", "distinctSumMV", "distinctAvgMV"
    };
    double[] expectedResults = new double[]{
        -5.421344202E9, 577725, -9999.0, 16271.0, -9383.95292223809, 26270.0, 312, 312, 328, 3954484.0,
        12674.628205128205
    };

    Assert.assertEquals(multiValueFunctions.length, expectedResults.length);

    for (int i = 0; i < multiValueFunctions.length; i++) {
      String pinotQuery = String.format("SELECT %s(DivAirportIDs) FROM mytable", multiValueFunctions[i]);
      JsonNode jsonNode = postQuery(pinotQuery);
      Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble(), expectedResults[i]);
    }

    String pinotQuery = "SELECT percentileMV(DivAirportIDs, 99) FROM mytable";
    JsonNode jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble(), 13433.0);

    pinotQuery = "SELECT percentileEstMV(DivAirportIDs, 99) FROM mytable";
    jsonNode = postQuery(pinotQuery);
    Assert.assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble() > 13000);
    Assert.assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble() < 14000);

    pinotQuery = "SELECT percentileTDigestMV(DivAirportIDs, 99) FROM mytable";
    jsonNode = postQuery(pinotQuery);
    Assert.assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble() > 13000);
    Assert.assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble() < 14000);

    pinotQuery = "SELECT percentileKLLMV(DivAirportIDs, 99) FROM mytable";
    jsonNode = postQuery(pinotQuery);
    Assert.assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble() > 10000);
    Assert.assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble() < 17000);

    pinotQuery = "SELECT percentileKLLMV(DivAirportIDs, 99, 100) FROM mytable";
    jsonNode = postQuery(pinotQuery);
    Assert.assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble() > 10000);
    Assert.assertTrue(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble() < 17000);
  }

  @Test
  void testDual()
      throws Exception {
    setUseMultiStageQueryEngine(true);
    JsonNode queryResponse = postQuery("SELECT 1");
    Assert.assertTrue(queryResponse.get("exceptions").isEmpty());
    Assert.assertEquals(queryResponse.get("numRowsResultSet").asInt(), 1);
  }

  /**
   * This test is added because SSE engine supports it and is used in production.
   * Make sure that the difference in support is well-documented.
   */
  @Test
  void testDualWithNotExistsTableMSE()
      throws Exception {
    setUseMultiStageQueryEngine(true);

    assertQuery("SELECT 1 from notExistsTable")
        .firstException()
        .hasErrorCode(QueryErrorCode.TABLE_DOES_NOT_EXIST);
  }

  @Test
  public void testTimeFunc()
      throws Exception {
    String sqlQuery = "SELECT toDateTime(now(), 'yyyy-MM-dd z'), toDateTime(ago('PT1H'), 'yyyy-MM-dd z') FROM mytable";
    JsonNode response = postQuery(sqlQuery);
    String todayStr = response.get("resultTable").get("rows").get(0).get(0).asText();
    String expectedTodayStr =
        Instant.now().atZone(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    assertEquals(todayStr, expectedTodayStr);

    String oneHourAgoTodayStr = response.get("resultTable").get("rows").get(0).get(1).asText();
    String expectedOneHourAgoTodayStr = Instant.now().minus(Duration.parse("PT1H")).atZone(ZoneId.of("UTC"))
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    assertEquals(oneHourAgoTodayStr, expectedOneHourAgoTodayStr);
  }

  @Test
  public void testRegexpReplace()
      throws Exception {
    // Correctness tests of regexpReplace.

    // Test replace all.
    String sqlQuery = "SELECT regexpReplace('CA', 'C', 'TEST')";
    JsonNode response = postQuery(sqlQuery);
    String result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "TESTA");

    sqlQuery = "SELECT regexpReplace('foobarbaz', 'b', 'X')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXarXaz");

    sqlQuery = "SELECT regexpReplace('foobarbaz', 'b', 'XY')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXYarXYaz");

    sqlQuery = "SELECT regexpReplace('Argentina', '(.)', '$1 ')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "A r g e n t i n a ");

    sqlQuery = "SELECT regexpReplace('Pinot is  blazing  fast', '( ){2,}', ' ')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "Pinot is blazing fast");

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, and wise','\\w+thy', 'something')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, something, and wise");

    sqlQuery = "SELECT regexpReplace('11234567898','(\\d)(\\d{3})(\\d{3})(\\d{4})', '$1-($2) $3-$4')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "1-(123) 456-7898");

    // Test replace starting at index.

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 4)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, something, something and wise");

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 1)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "hsomething, something, something and wise");

    // Test occurence
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 2)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, something and wise");

    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 0)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Test flags
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 0, 0, 'i')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 21, 'i')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test - incorrect flag
    sqlQuery = "SELECT regexpReplace('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 12, 'xyz')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Test in select clause with column values
    sqlQuery = "SELECT regexpReplace(DestCityName, ' ', '', 0, -1, 'i') from mytable where OriginState = 'CA'";
    response = postQuery(sqlQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertFalse(row.get(0).asText().contains(" "));
    }

    // Test in where clause
    sqlQuery = "SELECT count(*) from mytable where regexpReplace(OriginState, '[VC]A', 'TEST') = 'TEST'";
    response = postQuery(sqlQuery);
    int count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from mytable where OriginState='CA' or OriginState='VA'";
    response = postQuery(sqlQuery);
    int count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);

    // Test nested transform
    sqlQuery =
        "SELECT count(*) from mytable where contains(regexpReplace(OriginState, '(C)(A)', '$1TEST$2'), 'CTESTA')";
    response = postQuery(sqlQuery);
    count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from mytable where OriginState='CA'";
    response = postQuery(sqlQuery);
    count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);
  }

  @Test
  public void testRegexpReplaceVar()
      throws Exception {
    // Correctness tests of regexpReplaceVar.

    // Test replace all.
    String sqlQuery = "SELECT regexpReplaceVar('CA', 'C', 'TEST')";
    JsonNode response = postQuery(sqlQuery);
    String result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "TESTA");

    sqlQuery = "SELECT regexpReplaceVar('foobarbaz', 'b', 'X')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXarXaz");

    sqlQuery = "SELECT regexpReplaceVar('foobarbaz', 'b', 'XY')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "fooXYarXYaz");

    sqlQuery = "SELECT regexpReplaceVar('Argentina', '(.)', '$1 ')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "A r g e n t i n a ");

    sqlQuery = "SELECT regexpReplaceVar('Pinot is  blazing  fast', '( ){2,}', ' ')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "Pinot is blazing fast");

    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, and wise','\\w+thy', 'something')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, something, and wise");

    sqlQuery = "SELECT regexpReplaceVar('11234567898','(\\d)(\\d{3})(\\d{3})(\\d{4})', '$1-($2) $3-$4')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "1-(123) 456-7898");

    // Test replace starting at index.

    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 4)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, something, something and wise");

    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 1)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "hsomething, something, something and wise");

    // Test occurence
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 2)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, something and wise");

    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+thy', 'something', 0, 0)";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Test flags
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 0, 0, 'i')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "something, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+tHy', 'something')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test. Pattern match not found.
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 21, 'i')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Negative test - incorrect flag
    sqlQuery = "SELECT regexpReplaceVar('healthy, wealthy, stealthy and wise','\\w+tHy', 'something', 3, 12, 'xyz')";
    response = postQuery(sqlQuery);
    result = response.get("resultTable").get("rows").get(0).get(0).asText();
    assertEquals(result, "healthy, wealthy, stealthy and wise");

    // Test in select clause with column values
    sqlQuery = "SELECT regexpReplaceVar(DestCityName, ' ', '', 0, -1, 'i') from mytable where OriginState = 'CA'";
    response = postQuery(sqlQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      JsonNode row = rows.get(i);
      assertFalse(row.get(0).asText().contains(" "));
    }

    // Test in where clause
    sqlQuery = "SELECT count(*) from mytable where regexpReplaceVar(OriginState, '[VC]A', 'TEST') = 'TEST'";
    response = postQuery(sqlQuery);
    int count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from mytable where OriginState='CA' or OriginState='VA'";
    response = postQuery(sqlQuery);
    int count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);

    // Test nested transform
    sqlQuery =
        "SELECT count(*) from mytable where contains(regexpReplaceVar(OriginState, '(C)(A)', '$1TEST$2'), 'CTESTA')";
    response = postQuery(sqlQuery);
    count1 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    sqlQuery = "SELECT count(*) from mytable where OriginState='CA'";
    response = postQuery(sqlQuery);
    count2 = response.get("resultTable").get("rows").get(0).get(0).asInt();
    assertEquals(count1, count2);
  }

  @Test
  public void testUrlFunc()
      throws Exception {
    String sqlQuery = "SELECT encodeUrl('key1=value 1&key2=value@!$2&key3=value%3'), "
        + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') FROM mytable";
    JsonNode response = postQuery(sqlQuery);
    String encodedString = response.get("resultTable").get("rows").get(0).get(0).asText();
    String expectedUrlStr = encodeUrl("key1=value 1&key2=value@!$2&key3=value%3");
    assertEquals(encodedString, expectedUrlStr);

    String decodedString = response.get("resultTable").get("rows").get(0).get(1).asText();
    expectedUrlStr = decodeUrl("key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    assertEquals(decodedString, expectedUrlStr);
  }

  @Test
  public void testBase64Func()
      throws Exception {

    // string literal
    String sqlQuery = "SELECT toBase64(toUtf8('hello!')), " + "fromUtf8(fromBase64('aGVsbG8h')) FROM mytable";
    JsonNode response = postQuery(sqlQuery);
    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"STRING\"]");
    JsonNode rows = response.get("resultTable").get("rows");

    String encodedString = rows.get(0).get(0).asText();
    String expectedEncodedStr = toBase64(toUtf8("hello!"));
    assertEquals(encodedString, expectedEncodedStr);
    String decodedString = rows.get(0).get(1).asText();
    String expectedDecodedStr = fromUtf8(fromBase64("aGVsbG8h"));
    assertEquals(decodedString, expectedDecodedStr);

    // long string literal encode
    sqlQuery =
        "SELECT toBase64(toUtf8('this is a long string that will encode to more than 76 characters using base64')) "
            + "FROM mytable";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    encodedString = rows.get(0).get(0).asText();
    assertEquals(encodedString,
        toBase64(toUtf8("this is a long string that will encode to more than 76 characters using base64")));

    // long string literal decode
    sqlQuery = "SELECT fromUtf8(fromBase64"
        + "('dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0"
        + "')) FROM mytable";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    decodedString = rows.get(0).get(0).asText();
    assertEquals(decodedString, fromUtf8(fromBase64(
        "dGhpcyBpcyBhIGxvbmcgc3RyaW5nIHRoYXQgd2lsbCBlbmNvZGUgdG8gbW9yZSB0aGFuIDc2IGNoYXJhY3RlcnMgdXNpbmcgYmFzZTY0")));

    // non-string literal
    sqlQuery = "SELECT toBase64(toUtf8(123)), fromUtf8(fromBase64(toBase64(toUtf8(123)))), 123 FROM mytable";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    encodedString = rows.get(0).get(0).asText();
    decodedString = rows.get(0).get(1).asText();
    String originalCol = rows.get(0).get(2).asText();
    assertEquals(decodedString, originalCol);
    assertEquals(encodedString, toBase64(toUtf8("123")));

    // identifier
    sqlQuery = "SELECT Carrier, toBase64(toUtf8(Carrier)), fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))), "
        + "fromBase64(toBase64(toUtf8(Carrier))) FROM mytable LIMIT 100";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"STRING\",\"STRING\",\"BYTES\"]");
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.size(), 100);
    for (int i = 0; i < 100; i++) {
      String original = rows.get(0).asText();
      String encoded = rows.get(1).asText();
      String decoded = rows.get(2).asText();
      assertEquals(original, decoded);
      assertEquals(encoded, toBase64(toUtf8(original)));
      assertEquals(decoded, fromUtf8(fromBase64(toBase64(toUtf8(original)))));
    }

    // invalid argument
    sqlQuery = "SELECT toBase64('hello!') FROM mytable";
    response = postQuery(sqlQuery);
    int expectedStatusCode = useMultiStageQueryEngine() ? QueryErrorCode.QUERY_PLANNING.getId()
        : QueryErrorCode.SQL_PARSING.getId();
    Assert.assertEquals(response.get("exceptions").get(0).get("errorCode").asInt(), expectedStatusCode);

    // invalid argument
    sqlQuery = "SELECT fromBase64('hello!') FROM mytable";
    try (QueryAssert.QueryErrorAssert.Soft assertion = assertQuery(sqlQuery).softFirstException()) {
      assertion.hasErrorCode(QueryErrorCode.QUERY_PLANNING);
      assertion.containsMessage("Illegal base64 character");
    }

    // string literal used in a filter
    sqlQuery = "SELECT * FROM mytable WHERE fromUtf8(fromBase64('aGVsbG8h')) != Carrier AND "
        + "toBase64(toUtf8('hello!')) != Carrier LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // non-string literal used in a filter
    sqlQuery = "SELECT * FROM mytable WHERE fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) != Carrier LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // string identifier used in a filter
    sqlQuery = "SELECT * FROM mytable WHERE fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))) = Carrier LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // non-string identifier used in a filter
    sqlQuery = "SELECT fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))), AirlineID FROM mytable WHERE "
        + "fromUtf8(fromBase64(toBase64(toUtf8(AirlineID)))) = AirlineID LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"LONG\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);

    // string identifier used in group by order by
    sqlQuery = "SELECT Carrier as originalCol, toBase64(toUtf8(Carrier)) as encoded, "
        + "fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))) as decoded FROM mytable "
        + "GROUP BY Carrier, toBase64(toUtf8(Carrier)), fromUtf8(fromBase64(toBase64(toUtf8(Carrier)))) "
        + "ORDER BY toBase64(toUtf8(Carrier)) LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\",\"STRING\",\"STRING\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      String original = rows.get(0).asText();
      String encoded = rows.get(1).asText();
      String decoded = rows.get(2).asText();
      assertEquals(original, decoded);
      assertEquals(encoded, toBase64(toUtf8(original)));
      assertEquals(decoded, fromUtf8(fromBase64(toBase64(toUtf8(original)))));
    }

    // non-string identifier used in group by order by
    sqlQuery = "SELECT AirlineID as originalCol, toBase64(toUtf8(CAST(AirlineID AS VARCHAR))) as encoded, "
        + "fromUtf8(fromBase64(toBase64(toUtf8(CAST(AirlineID AS VARCHAR))))) as decoded FROM mytable "
        + "GROUP BY AirlineID, toBase64(toUtf8(CAST(AirlineID AS VARCHAR))), "
        + "fromUtf8(fromBase64(toBase64(toUtf8(CAST(AirlineID AS VARCHAR))))) "
        + "ORDER BY fromUtf8(fromBase64(toBase64(toUtf8(CAST(AirlineID AS VARCHAR))))) LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"LONG\",\"STRING\",\"STRING\"]");
    rows = resultTable.get("rows");
    assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      String original = rows.get(0).asText();
      String encoded = rows.get(1).asText();
      String decoded = rows.get(2).asText();
      assertEquals(original, decoded);
      assertEquals(encoded, toBase64(toUtf8(original)));
      assertEquals(decoded, fromUtf8(fromBase64(toBase64(toUtf8(original)))));
    }

    // Test select with group by order by limit
    sqlQuery = "SELECT toBase64(toUtf8(AirlineID)) "
        + "FROM mytable "
        + "GROUP BY toBase64(toUtf8(AirlineID)) "
        + "ORDER BY toBase64(toUtf8(AirlineID)) DESC "
        + "LIMIT 10";
    response = postQuery(sqlQuery);
    resultTable = response.get("resultTable");
    dataSchema = resultTable.get("dataSchema");
    assertEquals(dataSchema.get("columnDataTypes").toString(), "[\"STRING\"]");
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.get(0).get(0).asText(), "MjExNzE=");
    assertEquals(rows.get(1).get(0).asText(), "MjAzOTg=");
    assertEquals(rows.get(2).get(0).asText(), "MjAzNjY=");
    assertEquals(rows.get(3).get(0).asText(), "MjAzNTU=");
    assertEquals(rows.get(4).get(0).asText(), "MjAzMDQ=");
    assertEquals(rows.get(5).get(0).asText(), "MjA0Mzc=");
    assertEquals(rows.get(6).get(0).asText(), "MjA0MzY=");
    assertEquals(rows.get(7).get(0).asText(), "MjA0MDk=");
    assertEquals(rows.get(8).get(0).asText(), "MTkzOTM=");
    assertEquals(rows.get(9).get(0).asText(), "MTk5Nzc=");
  }

  @Test
  public void testLiteralOnlyFunc()
      throws Exception {
    long queryStartTimeMs = System.currentTimeMillis();
    String sqlQuery =
        "SELECT 1, cast(now() as bigint) as currentTs, ago('PT1H') as oneHourAgoTs, 'abc', "
            + "toDateTime(now(), 'yyyy-MM-dd z') as today, cast(now() as bigint), ago('PT1H'), "
            + "encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') as encodedUrl, "
            + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') as decodedUrl, "
            + "toBase64(toUtf8('hello!')) as toBase64, fromUtf8(fromBase64('aGVsbG8h')) as fromBase64";
    JsonNode response = postQuery(sqlQuery);
    long queryEndTimeMs = System.currentTimeMillis();

    JsonNode resultTable = response.get("resultTable");
    JsonNode dataSchema = resultTable.get("dataSchema");
    JsonNode columnDataTypes = dataSchema.get("columnDataTypes");
    assertEquals(columnDataTypes.get(0).asText(), "INT");
    assertEquals(columnDataTypes.get(1).asText(), "LONG");
    assertEquals(columnDataTypes.get(2).asText(), "LONG");
    assertEquals(columnDataTypes.get(3).asText(), "STRING");
    assertEquals(columnDataTypes.get(4).asText(), "STRING");
    assertEquals(columnDataTypes.get(5).asText(), "LONG");
    assertEquals(columnDataTypes.get(6).asText(), "LONG");
    assertEquals(columnDataTypes.get(7).asText(), "STRING");
    assertEquals(columnDataTypes.get(8).asText(), "STRING");
    assertEquals(columnDataTypes.get(9).asText(), "STRING");
    assertEquals(columnDataTypes.get(10).asText(), "STRING");

    JsonNode results = resultTable.get("rows").get(0);
    assertEquals(results.get(0).asInt(), 1);
    long nowResult = results.get(1).asLong();
    // Timestamp granularity is seconds
    assertTrue(nowResult >= ((queryStartTimeMs / 1000) * 1000));
    assertTrue(nowResult <= ((queryEndTimeMs / 1000) * 1000));
    long oneHourAgoResult = results.get(2).asLong();
    assertTrue(oneHourAgoResult >= queryStartTimeMs - TimeUnit.HOURS.toMillis(1));
    assertTrue(oneHourAgoResult <= queryEndTimeMs - TimeUnit.HOURS.toMillis(1));
    assertEquals(results.get(3).asText(), "abc");
    String queryStartTimeDay = Instant.ofEpochMilli(queryStartTimeMs).atZone(ZoneId.of("UTC"))
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    String queryEndTimeDay = Instant.ofEpochMilli(queryEndTimeMs).atZone(ZoneId.of("UTC"))
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd z"));
    String dateTimeResult = results.get(4).asText();
    assertTrue(dateTimeResult.equals(queryStartTimeDay) || dateTimeResult.equals(queryEndTimeDay));
    nowResult = results.get(5).asLong();
    assertTrue(nowResult >= ((queryStartTimeMs / 1000) * 1000));
    assertTrue(nowResult <= ((queryEndTimeMs / 1000) * 1000));
    oneHourAgoResult = results.get(6).asLong();
    assertTrue(oneHourAgoResult >= queryStartTimeMs - TimeUnit.HOURS.toMillis(1));
    assertTrue(oneHourAgoResult <= queryEndTimeMs - TimeUnit.HOURS.toMillis(1));
    assertEquals(results.get(7).asText(), "key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253");
    assertEquals(results.get(8).asText(), "key1=value 1&key2=value@!$2&key3=value%3");
    assertEquals(results.get(9).asText(), "aGVsbG8h");
    assertEquals(results.get(10).asText(), "hello!");
  }

  @Test
  public void testMultiValueColumnGroupBy()
      throws Exception {
    String pinotQuery = "SELECT count(*), ARRAY_TO_MV(RandomAirports) FROM mytable "
        + "GROUP BY ARRAY_TO_MV(RandomAirports)";
    JsonNode jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 154);
  }

  @Test
  public void testVariadicFunction()
      throws Exception {
    String sqlQuery = "SELECT ARRAY_TO_MV(VALUE_IN(RandomAirports, 'MFR', 'SUN', 'GTR')) as airport, count(*) "
        + "FROM mytable WHERE ARRAY_TO_MV(RandomAirports) IN ('MFR', 'SUN', 'GTR') GROUP BY airport";
    JsonNode jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
    assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "STRING");
    assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(1).asText(), "LONG");
    assertEquals(jsonNode.get("numRowsResultSet").asInt(), 3);
  }

  @Test(dataProvider = "polymorphicScalarComparisonFunctionsDataProvider")
  public void testPolymorphicScalarComparisonFunctions(String type, String literal, String lesserLiteral,
      Object expectedValue)
      throws Exception {

    // Queries written this way will trigger the PinotEvaluateLiteralRule which will call the scalar comparison function
    // on the literals. Simpler queries like SELECT ... WHERE 'test' = 'test' will not trigger the optimization rule
    // because the filter will be removed by Calcite in the SQL to Rel conversion phase even before the optimization
    // rules are fired.
    String sqlQueryPrefix = "WITH data as (SELECT " + literal + " as \"foo\" FROM mytable) "
        + "SELECT * FROM data ";

    // Test equals
    JsonNode result = postQuery(sqlQueryPrefix + "WHERE \"foo\" = " + literal);
    assertNoError(result);
    checkSingleColumnSameValueResult(result, DEFAULT_COUNT_STAR_RESULT, type, expectedValue);

    // Test not equals
    result = postQuery(sqlQueryPrefix + "WHERE \"foo\" != " + lesserLiteral);
    assertNoError(result);
    checkSingleColumnSameValueResult(result, DEFAULT_COUNT_STAR_RESULT, type, expectedValue);

    // Test greater than
    result = postQuery(sqlQueryPrefix + "WHERE \"foo\" > " + lesserLiteral);
    assertNoError(result);
    checkSingleColumnSameValueResult(result, DEFAULT_COUNT_STAR_RESULT, type, expectedValue);

    // Test greater than or equals
    result = postQuery(sqlQueryPrefix + "WHERE \"foo\" >= " + lesserLiteral);
    assertNoError(result);
    checkSingleColumnSameValueResult(result, DEFAULT_COUNT_STAR_RESULT, type, expectedValue);

    // Test less than
    result = postQuery(sqlQueryPrefix + "WHERE " + lesserLiteral + " < \"foo\"");
    assertNoError(result);
    checkSingleColumnSameValueResult(result, DEFAULT_COUNT_STAR_RESULT, type, expectedValue);

    // Test less than or equals
    result = postQuery(sqlQueryPrefix + "WHERE " + lesserLiteral + " <= \"foo\"");
    assertNoError(result);
    checkSingleColumnSameValueResult(result, DEFAULT_COUNT_STAR_RESULT, type, expectedValue);
  }

  @Test
  public void testPolymorphicScalarComparisonFunctionsDifferentType()
      throws Exception {
    // Don't support comparison for literals with different types
    String sqlQueryPrefix = "WITH data as (SELECT 1 as \"foo\" FROM mytable) "
        + "SELECT * FROM data WHERE \"foo\" ";

    JsonNode jsonNode = postQuery(sqlQueryPrefix + "= 'test'");
    assertFalse(jsonNode.get("exceptions").isEmpty());

    jsonNode = postQuery(sqlQueryPrefix + "!= 'test'");
    assertFalse(jsonNode.get("exceptions").isEmpty());

    jsonNode = postQuery(sqlQueryPrefix + "> 'test'");
    assertFalse(jsonNode.get("exceptions").isEmpty());

    jsonNode = postQuery(sqlQueryPrefix + ">= 'test'");
    assertFalse(jsonNode.get("exceptions").isEmpty());

    jsonNode = postQuery(sqlQueryPrefix + "< 'test'");
    assertFalse(jsonNode.get("exceptions").isEmpty());

    jsonNode = postQuery(sqlQueryPrefix + "<= 'test'");
    assertFalse(jsonNode.get("exceptions").isEmpty());
  }

  /**
   * Helper method to verify the result of a query that is assumed to return a single column with the same value for
   * all the rows. Only the first row value is checked.
   */
  private void checkSingleColumnSameValueResult(JsonNode result, long expectedRows, String type,
      Object expectedValue) {
    assertEquals(result.get("resultTable").get("dataSchema").get("columnDataTypes").size(), 1);
    assertEquals(result.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), type);
    assertEquals(result.get("numRowsResultSet").asLong(), expectedRows);
    assertEquals(result.get("resultTable").get("rows").get(0).get(0).asText(), expectedValue);
  }

  @DataProvider(name = "polymorphicScalarComparisonFunctionsDataProvider")
  Object[][] polymorphicScalarComparisonFunctionsDataProvider() {
    List<Object[]> inputs = new ArrayList<>();

    inputs.add(new Object[]{"STRING", "'test'", "'abc'", "test"});
    inputs.add(new Object[]{"INT", "1", "0", "1"});
    inputs.add(new Object[]{"LONG", "12345678999", "12345678998", "12345678999"});
    inputs.add(new Object[]{"FLOAT", "CAST(1.234 AS FLOAT)", "CAST(1.23 AS FLOAT)", "1.234"});
    inputs.add(new Object[]{"DOUBLE", "1.234", "1.23", "1.234"});
    inputs.add(new Object[]{"BOOLEAN", "CAST(true AS BOOLEAN)", "CAST(FALSE AS BOOLEAN)", "true"});
    inputs.add(new Object[]{
        "TIMESTAMP", "CAST(1723593600000 AS TIMESTAMP)", "CAST (1623593600000 AS TIMESTAMP)",
        new DateTime(1723593600000L, DateTimeZone.getDefault()).toString("yyyy-MM-dd HH:mm:ss.S")
    });

    return inputs.toArray(new Object[0][]);
  }

  @Test
  public void skipArrayToMvOptimization()
      throws Exception {
    String sqlQuery = "SELECT 1 "
        + "FROM mytable "
        + "WHERE ARRAY_TO_MV(RandomAirports) = 'MFR' and ARRAY_TO_MV(RandomAirports) = 'GTR'";

    JsonNode jsonNode = postQuery("Explain plan for " + sqlQuery);
    JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

    Pattern pattern = Pattern.compile("LogicalValues\\(tuples=\\[\\[]]\\)");
    String planAsText = plan.asText();
    boolean matches = pattern.matcher(planAsText).find();
    Assert.assertFalse(matches, "Plan should not contain contain LogicalValues node but plan is \n"
        + planAsText);

    jsonNode = postQuery(sqlQuery);
    Assert.assertNotEquals(jsonNode.get("resultTable").get("rows").size(), 0);
  }

  @Test
  public void testMultiValueColumnGroupByOrderBy()
      throws Exception {
    String pinotQuery =
        "SELECT count(*), ARRAY_TO_MV(RandomAirports) FROM mytable GROUP BY ARRAY_TO_MV(RandomAirports) "
            + "ORDER BY ARRAY_TO_MV(RandomAirports) DESC";
    JsonNode jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 154);
  }

  @Test
  public void testMultiValueColumnTransforms()
      throws Exception {
    String pinotQuery = "SELECT arrayLength(RandomAirports) FROM mytable limit 10";
    JsonNode jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 10);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "INT");

    pinotQuery = "SELECT cardinality(DivAirportIDs) FROM mytable limit 10";
    jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 10);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "INT");

    // arrayMin dataType should be same as the column dataType
    pinotQuery = "SELECT arrayMin(DivAirports) FROM mytable limit 10";
    jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 10);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "STRING");

    pinotQuery = "SELECT arrayMin(DivAirportIDs) FROM mytable limit 10";
    jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 10);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "INT");

    // arrayMax dataType should be same as the column dataType
    pinotQuery = "SELECT arrayMax(DivAirports) FROM mytable limit 10";
    jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 10);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "STRING");

    pinotQuery = "SELECT arrayMax(DivAirportIDs) FROM mytable limit 10";
    jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 10);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "INT");

    // arraySum
    pinotQuery = "SELECT arraySum(DivAirportIDs) FROM mytable limit 1";
    jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 1);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "DOUBLE");

    // arraySum
    pinotQuery = "SELECT arrayAverage(DivAirportIDs) FROM mytable limit 1";
    jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 1);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "DOUBLE");
  }

  @Test
  public void selectStarDoesNotProjectSystemColumns()
      throws Exception {
    JsonNode jsonNode = postQuery("select * from mytable limit 0");
    List<String> columns = getColumns(jsonNode);
    for (int i = 0; i < columns.size(); i++) {
      String colName = columns.get(i);
      Assert.assertFalse(colName.startsWith("$"), "Column " + colName + " (found at index " + i + " is a system column "
          + "and shouldn't be included in select *");
    }
  }

  @Test(dataProvider = "systemColumns")
  public void systemColumnsCanBeSelected(String systemColumn)
      throws Exception {
    JsonNode jsonNode = postQuery("select " + systemColumn + " from mytable limit 0");
    assertNoError(jsonNode);
  }

  @Test(dataProvider = "systemColumns")
  public void systemColumnsCanBeUsedInWhere(String systemColumn)
      throws Exception {
    JsonNode jsonNode = postQuery("select 1 from mytable where " + systemColumn + " is not null limit 0");
    assertNoError(jsonNode);
  }

  @Test
  public void testSearch()
      throws Exception {
    String sqlQuery = "SELECT CASE WHEN ArrDel15 > 50 OR ArrDel15 < 10 THEN 10 ELSE 0 END "
        + "FROM mytable LIMIT 1000";
    JsonNode jsonNode = postQuery("Explain plan WITHOUT IMPLEMENTATION for " + sqlQuery);
    JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

    Pattern pattern = Pattern.compile("Sarg\\[\\(-∞\\.\\.10\\), \\(50\\.\\.\\+∞\\)]");
    boolean matches = pattern.matcher(plan.asText()).find();
    Assert.assertTrue(matches, "Plan doesn't contain the expected SEARCH");

    jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
  }

  @Test
  public void testLiteralFilterReduce()
      throws Exception {
    String sqlQuery = "SELECT * FROM (SELECT CASE WHEN AirTime > 0 THEN 'positive' ELSE 'negative' END AS AirTime "
        + "FROM mytable) WHERE AirTime IN ('positive', 'negative')";
    JsonNode jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
    assertEquals(jsonNode.get("resultTable").get("rows").size(), getCountStarResult());
  }

  @Test
  public void testBetween()
      throws Exception {
    String sqlQuery = "SELECT COUNT(*) FROM mytable WHERE AirTime BETWEEN 10 AND 50";
    JsonNode jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asInt(), 17293);

    String explainQuery = "EXPLAIN PLAN FOR " + sqlQuery;
    jsonNode = postQuery(explainQuery);
    assertNoError(jsonNode);
    String plan = jsonNode.get("resultTable").get("rows").get(0).get(1).asText();
    // Ensure that the BETWEEN filter predicate was converted
    Assert.assertFalse(plan.contains("BETWEEN"));
    Assert.assertTrue(plan.contains("Sarg[[10..50]]"));

    // No rows should be returned since lower bound is greater than upper bound
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE ARRAY_TO_MV(RandomAirports) BETWEEN 'SUN' AND 'GTR'";
    jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asInt(), 0);

    explainQuery = "EXPLAIN PLAN FOR " + sqlQuery;
    jsonNode = postQuery(explainQuery);
    assertNoError(jsonNode);
    plan = jsonNode.get("resultTable").get("rows").get(0).get(1).asText();
    // Ensure that the BETWEEN filter predicate was not converted
    Assert.assertTrue(plan.contains("BETWEEN"));
    Assert.assertFalse(plan.contains(">="));
    Assert.assertFalse(plan.contains("<="));
    Assert.assertFalse(plan.contains("Sarg"));

    // Expect a non-zero result this time since we're using BETWEEN SYMMETRIC
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE ARRAY_TO_MV(RandomAirports) BETWEEN SYMMETRIC 'SUN' AND 'GTR'";
    jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asInt(), 57007);

    explainQuery = "EXPLAIN PLAN FOR " + sqlQuery;
    jsonNode = postQuery(explainQuery);
    assertNoError(jsonNode);
    plan = jsonNode.get("resultTable").get("rows").get(0).get(1).asText();
    Assert.assertTrue(plan.contains("BETWEEN"));
    // Ensure that the BETWEEN filter predicate was not converted
    Assert.assertFalse(plan.contains(">="));
    Assert.assertFalse(plan.contains("<="));
    Assert.assertFalse(plan.contains("Sarg"));

    // Test NOT BETWEEN
    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE ARRAY_TO_MV(RandomAirports) NOT BETWEEN 'GTR' AND 'SUN'";
    jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asInt(), 58538);

    explainQuery =
        "SET " + CommonConstants.Broker.Request.QueryOptionKey.EXPLAIN_ASKING_SERVERS + "=true; EXPLAIN PLAN FOR "
            + sqlQuery;
    jsonNode = postQuery(explainQuery);
    assertNoError(jsonNode);
    plan = jsonNode.get("resultTable").get("rows").get(0).get(1).asText();
    // Ensure that the BETWEEN filter predicate was not converted. Also ensure that the NOT filter is added.
    Assert.assertTrue(plan.contains("BETWEEN"));
    Assert.assertTrue(plan.contains("FilterNot"));
    Assert.assertFalse(plan.contains(">="));
    Assert.assertFalse(plan.contains("<="));
    Assert.assertFalse(plan.contains("Sarg"));
  }

  @Test
  public void testCaseWhenWithLargeNumberOfWhenThenClauses()
      throws Exception {
    // This test is to verify that the case when function with a large number of when then clauses works correctly.
    // The test verifies both the scalar and transform function variants.

    // Write the query in a way that the case when will be executed in the intermediate stage and hence will have
    // to use the scalar function variant instead of the transform function variant.
    String sqlQuery =
        "SELECT CASE WHEN CRSArrTime > 2000 THEN 20 WHEN CRSArrTime > 1900 THEN 19 WHEN CRSArrTime > 1800 THEN 18 "
            + "WHEN CRSArrTime > 1700 THEN 17 WHEN CRSArrTime > 1600 THEN 16 WHEN CRSArrTime > 1500 THEN 15 WHEN "
            + "CRSArrTime > 1400 THEN 14 WHEN CRSArrTime > 1300 THEN 13 WHEN CRSArrTime > 1200 THEN 12 WHEN "
            + "CRSArrTime > 1100 THEN 11 WHEN CRSArrTime > 1000 THEN 10 WHEN CRSArrTime > 900 THEN 9 WHEN "
            + "CRSArrTime > 800 THEN 8 WHEN CRSArrTime > 700 THEN 7 WHEN CRSArrTime > 600 THEN 6 WHEN "
            + "CRSArrTime > 500 THEN 50 WHEN CRSArrTime > 400 THEN 4 WHEN CRSArrTime > 300 THEN 3 WHEN "
            + "CRSArrTime > 200 THEN 2 WHEN CRSArrTime > 100 THEN 1 ELSE 0 END FROM (SELECT * FROM mytable ORDER BY "
            + "CRSArrTime LIMIT 10)";
    JsonNode jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").size(), 1);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "INT");
    JsonNode rowsScalar = jsonNode.get("resultTable").get("rows");
    assertEquals(rowsScalar.size(), 10);

    // Rewrite the query in a way that the case when will be executed in the leaf stage projection and hence will use
    // the transform function variant
    sqlQuery =
        "SELECT CASE WHEN CRSArrTime > 2000 THEN 20 WHEN CRSArrTime > 1900 THEN 19 WHEN CRSArrTime > 1800 THEN 18 "
            + "WHEN CRSArrTime > 1700 THEN 17 WHEN CRSArrTime > 1600 THEN 16 WHEN CRSArrTime > 1500 THEN 15 WHEN "
            + "CRSArrTime > 1400 THEN 14 WHEN CRSArrTime > 1300 THEN 13 WHEN CRSArrTime > 1200 THEN 12 WHEN "
            + "CRSArrTime > 1100 THEN 11 WHEN CRSArrTime > 1000 THEN 10 WHEN CRSArrTime > 900 THEN 9 WHEN "
            + "CRSArrTime > 800 THEN 8 WHEN CRSArrTime > 700 THEN 7 WHEN CRSArrTime > 600 THEN 6 WHEN "
            + "CRSArrTime > 500 THEN 50 WHEN CRSArrTime > 400 THEN 4 WHEN CRSArrTime > 300 THEN 3 WHEN "
            + "CRSArrTime > 200 THEN 2 WHEN CRSArrTime > 100 THEN 1 ELSE 0 END FROM mytable ORDER BY "
            + "CRSArrTime LIMIT 10";
    jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").size(), 1);
    Assert.assertEquals(jsonNode.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "INT");
    JsonNode rowsTransform = jsonNode.get("resultTable").get("rows");
    assertEquals(rowsTransform.size(), 10);

    for (int i = 0; i < 10; i++) {
      assertEquals(rowsScalar.get(i).get(0).asInt(), rowsTransform.get(i).get(0).asInt());
    }
  }

  @Test
  public void testNullIf()
      throws Exception {
    // Calls to the Calcite NULLIF operator are rewritten to the equivalent CASE WHEN expressions. This test verifies
    // that the rewrite works correctly.
    String sqlQuery = "SET " + CommonConstants.Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING
        + "=true; SELECT NULLIF(ArrDelay, 0) FROM mytable";
    JsonNode result = postQuery(sqlQuery);
    assertNoError(result);

    JsonNode rows = result.get("resultTable").get("rows");
    AtomicInteger nullRows = new AtomicInteger();
    rows.elements().forEachRemaining(row -> {
      if (row.get(0).isNull()) {
        nullRows.getAndIncrement();
      }
    });

    sqlQuery = "SELECT COUNT(*) FROM mytable WHERE ArrDelay = 0";
    result = postQuery(sqlQuery);
    assertNoError(result);
    assertEquals(nullRows.get(), result.get("resultTable").get("rows").get(0).get(0).asInt());
  }

  @Test
  public void testMVNumericCastInFilter()
      throws Exception {
    String sqlQuery = "SELECT COUNT(*) FROM mytable WHERE ARRAY_TO_MV(CAST(DivAirportIDs AS BIGINT ARRAY)) > 0";
    JsonNode jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asInt(), 15482);
  }

  @Test
  public void testDirectFilteredAggregationWithNoValueMatchingAggregationFilterDefault()
      throws Exception {
    // Use a hint to ensure that the aggregation will not be pushed to the leaf stage, so that we can test the
    // MultistageGroupByExecutor. This will use a "DIRECT" aggregation.
    String sqlQuery = "SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */"
        + "AirlineID, COUNT(*) FILTER (WHERE Origin = 'garbage') FROM mytable WHERE AirlineID > 20000 GROUP BY "
        + "AirlineID";
    JsonNode result = postQuery(sqlQuery);
    assertNoError(result);
    // Ensure that result set is not empty
    assertTrue(result.get("numRowsResultSet").asInt() > 0);

    // Ensure that the count is 0 for all groups (because the aggregation filter does not match any rows)
    JsonNode rows = result.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      assertEquals(rows.get(i).get(1).asInt(), 0);
      // Ensure that the main filter was applied
      assertTrue(rows.get(i).get(0).asInt() > 20000);
    }
  }

  @Test
  public void testFilteredAggregationWithNoValueMatchingAggregationFilterDefault()
      throws Exception {
    // Query written this way with a CTE and limit will be planned such that the multi-stage group by executor will be
    // used for both leaf and final aggregation
    String aggregates1 = "COUNT(*) FILTER (WHERE Origin = 'garbage')";
    String aggregates2 = aggregates1 + ", COUNT(*)";
    String queryTemplate = "SET mseMaxInitialResultHolderCapacity = 1;\n"
        + "WITH tmp AS (SELECT * FROM mytable WHERE AirlineID > 20000 LIMIT 10000)\n"
        + "SELECT AirlineID, %s FROM tmp GROUP BY AirlineID";
    String query1 = String.format(queryTemplate, aggregates1);
    String query2 = String.format(queryTemplate, aggregates2);
    for (String query : new String[]{query1, query2}) {
      JsonNode result = postQuery(query);
      assertNoError(result);
      // Ensure that result set is not empty
      assertTrue(result.get("numRowsResultSet").asInt() > 0);

      // Ensure that the count is 0 for all groups (because the aggregation filter does not match any rows)
      JsonNode rows = result.get("resultTable").get("rows");
      for (int i = 0; i < rows.size(); i++) {
        assertEquals(rows.get(i).get(1).asInt(), 0);
        // Ensure that the main filter was applied
        assertTrue(rows.get(i).get(0).asInt() > 20000);
      }
    }
  }

  @Test
  public void testDirectFilteredAggregationWithNoValueMatchingAggregationFilterWithOption()
      throws Exception {
    // Use a hint to ensure that the aggregation will not be pushed to the leaf stage, so that we can test the
    // MultistageGroupByExecutor
    String sqlQuery = "SET " + CommonConstants.Broker.Request.QueryOptionKey.FILTERED_AGGREGATIONS_SKIP_EMPTY_GROUPS
        + "=true; SELECT /*+ aggOptions(is_skip_leaf_stage_group_by='true') */"
        + "AirlineID, COUNT(*) FILTER (WHERE Origin = 'garbage') FROM mytable WHERE AirlineID > 20000 GROUP BY "
        + "AirlineID";

    JsonNode result = postQuery(sqlQuery);
    assertNoError(result);

    // Result set will be empty since the aggregation filter does not match any rows, and we've set the query option to
    // skip empty groups
    assertEquals(result.get("numRowsResultSet").asInt(), 0);
  }

  @Test
  public void testWindowFunction()
      throws Exception {
    String query =
        "SELECT AirlineID, ArrDelay, DaysSinceEpoch, MAX(ArrDelay) OVER(PARTITION BY AirlineID ORDER BY DaysSinceEpoch "
            + "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS MaxAirlineDelaySoFar FROM mytable;";
    JsonNode jsonNode = postQuery(query);
    assertNoError(jsonNode);

    query =
        "SELECT AirlineID, ArrDelay, DaysSinceEpoch, SUM(ArrDelay) OVER(PARTITION BY AirlineID ORDER BY DaysSinceEpoch "
            + "ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS SumAirlineDelayInWindow FROM mytable;";
    jsonNode = postQuery(query);
    assertNoError(jsonNode);
  }

  @Test
  public void testBigDecimalAggregations()
      throws Exception {
    String query =
        "SELECT MIN(CAST(ArrTime AS DECIMAL)), MAX(CAST(ArrTime AS DECIMAL)), SUM(CAST(ArrTime AS DECIMAL)), AVG(CAST"
            + "(ArrTime AS DECIMAL)) FROM mytable";
    testQuery(query);
  }

  @Override
  protected String getTableName() {
    return _tableName;
  }

  @Test
  public void testWithoutDatabaseContext()
      throws Exception {
    // default database check. No database context passed
    checkQueryResultForDBTest("ActualElapsedTime", DEFAULT_TABLE_NAME);
  }

  @Test
  public void testWithDefaultDatabaseContextAsTableNamePrefix()
      throws Exception {
    // default database check. Default database context passed as table prefix
    checkQueryResultForDBTest("ActualElapsedTime", DEFAULT_DATABASE_NAME + "." + DEFAULT_TABLE_NAME);
  }

  @Test
  public void testWithDefaultDatabaseContextAsQueryOption()
      throws Exception {
    // default database check. Default database context passed as SET database='dbName'
    checkQueryResultForDBTest("ActualElapsedTime", DEFAULT_TABLE_NAME, DEFAULT_DATABASE_NAME);
  }

  @Test
  public void testWithDefaultDatabaseContextAsHttpHeader()
      throws Exception {
    // default database check. Default database context passed as "database" http header
    checkQueryResultForDBTest("ActualElapsedTime", DEFAULT_TABLE_NAME,
        Collections.singletonMap(CommonConstants.DATABASE, DEFAULT_DATABASE_NAME));
  }

  @Test
  public void testWithDefaultDatabaseContextAsTableNamePrefixAndQueryOption()
      throws Exception {
    // default database check. Default database context passed as table prefix as well as query option
    checkQueryResultForDBTest("ActualElapsedTime", DEFAULT_DATABASE_NAME + "." + DEFAULT_TABLE_NAME,
        DEFAULT_DATABASE_NAME);
  }

  @Test
  public void testWithDefaultDatabaseContextAsTableNamePrefixAndHttpHeader()
      throws Exception {
    // default database check. Default database context passed as table prefix as well as http header
    checkQueryResultForDBTest("ActualElapsedTime", DEFAULT_DATABASE_NAME + "." + DEFAULT_TABLE_NAME,
        Collections.singletonMap(CommonConstants.DATABASE, DEFAULT_DATABASE_NAME));
  }

  @Test
  public void testWithDatabaseContextAsTableNamePrefix()
      throws Exception {
    // Using renamed column "ActualElapsedTime_2" to ensure that the same table is not being queried.
    // custom database check. Database context passed only as table prefix. Will
    JsonNode result = getQueryResultForDBTest("ActualElapsedTime_2", TABLE_NAME_WITH_DATABASE, null, null);
    checkQueryPlanningErrorForDBTest(result, QueryErrorCode.TABLE_DOES_NOT_EXIST);
  }

  @Test
  public void testWithDatabaseContextAsQueryOption()
      throws Exception {
    // Using renamed column "ActualElapsedTime_2" to ensure that the same table is not being queried.
    // custom database check. Database context passed as SET database='dbName'
    checkQueryResultForDBTest("ActualElapsedTime_2", DEFAULT_TABLE_NAME, DATABASE_NAME);
  }

  @Test
  public void testWithDatabaseContextAsHttpHeader()
      throws Exception {
    // Using renamed column "ActualElapsedTime_2" to ensure that the same table is not being queried.
    // custom database check. Database context passed as "database" http header
    checkQueryResultForDBTest("ActualElapsedTime_2", DEFAULT_TABLE_NAME,
        Collections.singletonMap(CommonConstants.DATABASE, DATABASE_NAME));
  }

  @Test
  public void testWithDatabaseContextAsTableNamePrefixAndQueryOption()
      throws Exception {
    // Using renamed column "ActualElapsedTime_2" to ensure that the same table is not being queried.
    // custom database check. Database context passed as table prefix as well as query option
    checkQueryResultForDBTest("ActualElapsedTime_2", TABLE_NAME_WITH_DATABASE, DATABASE_NAME);
  }

  @Test
  public void testWithDatabaseContextAsTableNamePrefixAndHttpHeader()
      throws Exception {
    // Using renamed column "ActualElapsedTime_2" to ensure that the same table is not being queried.
    // custom database check. Database context passed as table prefix as well as http header
    checkQueryResultForDBTest("ActualElapsedTime_2", TABLE_NAME_WITH_DATABASE,
        Collections.singletonMap(CommonConstants.DATABASE, DATABASE_NAME));
  }

  @Test
  public void testWithConflictingDatabaseContextFromTableNamePrefixAndQueryOption()
      throws Exception {
    JsonNode result = getQueryResultForDBTest("ActualElapsedTime", TABLE_NAME_WITH_DATABASE, DEFAULT_DATABASE_NAME,
        null);
    checkQueryPlanningErrorForDBTest(result, QueryErrorCode.TABLE_DOES_NOT_EXIST);
  }

  @Test
  public void testWithConflictingDatabaseContextFromTableNamePrefixAndHttpHeader()
      throws Exception {
    JsonNode result = getQueryResultForDBTest("ActualElapsedTime", TABLE_NAME_WITH_DATABASE, null,
        Collections.singletonMap(CommonConstants.DATABASE, DEFAULT_DATABASE_NAME));
    checkQueryPlanningErrorForDBTest(result, QueryErrorCode.TABLE_DOES_NOT_EXIST);
  }

  @Test
  public void testWithConflictingDatabaseContextFromHttpHeaderAndQueryOption()
      throws Exception {
    JsonNode result = getQueryResultForDBTest("ActualElapsedTime", TABLE_NAME_WITH_DATABASE, DATABASE_NAME,
        Collections.singletonMap(CommonConstants.DATABASE, DEFAULT_DATABASE_NAME));
    checkQueryPlanningErrorForDBTest(result, QueryErrorCode.QUERY_VALIDATION);
  }

  @Test
  public void testCrossDatabaseQuery()
      throws Exception {
    String query = "SELECT tb1.Carrier, maxTime, distance FROM (SELECT max(AirTime) AS maxTime, Carrier FROM "
        + DEFAULT_TABLE_NAME + " GROUP BY Carrier ORDER BY maxTime DESC) AS tb1 JOIN (SELECT sum(Distance) AS distance,"
        + " Carrier FROM " + TABLE_NAME_WITH_DATABASE + " GROUP BY Carrier) AS tb2 "
        + "ON tb1.Carrier = tb2.Carrier; ";
    JsonNode result = postQuery(query);
    checkQueryPlanningErrorForDBTest(result, QueryErrorCode.TABLE_DOES_NOT_EXIST);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTablesQueriedField(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "select sum(ActualElapsedTime) from mytable;";
    JsonNode jsonNode = postQuery(query);
    JsonNode tablesQueried = jsonNode.get("tablesQueried");
    assertNotNull(tablesQueried);
    assertTrue(tablesQueried.isArray());
    assertEquals(tablesQueried.size(), 1);
    assertEquals(tablesQueried.get(0).asText(), "mytable");
  }

  @Test
  public void testTablesQueriedWithJoin()
      throws Exception {
    // Self Join
    String query = "select sum(ActualElapsedTime) from mytable WHERE ActualElapsedTime > "
        + "(select avg(ActualElapsedTime) as avg_profit from mytable)";
    JsonNode jsonNode = postQuery(query);
    JsonNode tablesQueried = jsonNode.get("tablesQueried");
    assertNotNull(tablesQueried);
    assertTrue(tablesQueried.isArray());
    assertEquals(tablesQueried.size(), 1);
    assertEquals(tablesQueried.get(0).asText(), "mytable");
  }

  @Test
  public void testConcurrentQueries() {
    QueryGenerator queryGenerator = getQueryGenerator();
    queryGenerator.setUseMultistageEngine(true);

    int numThreads = 20;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    List<Future<JsonNode>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      futures.add(executorService.submit(
          () -> postQuery(queryGenerator.generateQuery().generatePinotQuery().replace("`", "\""))));
    }

    for (Future<JsonNode> future : futures) {
      try {
        JsonNode jsonNode = future.get();
        assertNoError(jsonNode);
      } catch (Exception e) {
        Assert.fail("Caught exception while executing query", e);
      }
    }
    executorService.shutdownNow();
  }

  @Test
  public void testCaseInsensitiveNames() throws Exception {
    String query = "select ACTualELAPsedTIMe from mYtABLE where actUALelAPSedTIMe > 0 limit 1";
    JsonNode jsonNode = postQuery(query);
    long result = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();

    assertTrue(result > 0);
  }

  @Test
  public void testCaseInsensitiveNamesAgainstController() throws Exception {
    String query = "select ACTualELAPsedTIMe from mYtABLE where actUALelAPSedTIMe > 0 limit 1";
    JsonNode jsonNode = postQueryToController(query);
    long result = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();

    assertTrue(result > 0);
  }

  @Test
  public void testQueryCompileBrokerTimeout() throws Exception {
    // The sleep function is called with a literal value so it should be evaluated during the query compile phase
    String query = "SET timeoutMs=100; SELECT sleep(1000) FROM mytable";

    JsonNode result = postQuery(query);
    JsonNode exceptionsJson = result.get("exceptions");
    Iterator<JsonNode> exIterator = exceptionsJson.iterator();
    assertTrue(exIterator.hasNext(), "Expected a timeout exception but did not find one");
    ObjectNode exception = (ObjectNode) exIterator.next();

    try (QueryAssert.QueryErrorAssert.Soft assertions = QueryAssert.assertThat(result).softFirstException()) {
      // In case both error code and message are incorrect, instead of failing early, this fails with the following
      // message:
      // org.assertj.core.api.SoftAssertionError:
      // The following 2 assertions failed:
      // 1) Expected error code <BROKER_TIMEOUT (400)> but was <ACTUAL ERROR CODE>
      // at QueryAssert$QueryErrorAssert$Soft.hasErrorCode(QueryAssert$QueryErrorAssert$Soft.java:119)
      // 2) Expected message to contain <BrokerTimeoutError> but was <ACTUAL ERROR MESSAGE>
      assertions
          .hasErrorCode(QueryErrorCode.BROKER_TIMEOUT)
          .containsMessage(QueryErrorCode.BROKER_TIMEOUT.getDefaultMessage());
    }
  }

  @Test
  public void testNumServersQueried() throws Exception {
    String query = "select * from mytable limit 10";
    JsonNode jsonNode = postQuery(query);
    JsonNode numServersQueried = jsonNode.get("numServersQueried");
    assertNotNull(numServersQueried);
    assertTrue(numServersQueried.isInt());
    assertTrue(numServersQueried.asInt() > 0);
  }

  @Test
  public void testLookupJoin() throws Exception {

    Schema lookupTableSchema = createSchema(DIM_TABLE_SCHEMA_PATH);
    addSchema(lookupTableSchema);
    TableConfig tableConfig = createTableConfig(DIM_TABLE_TABLE_CONFIG_PATH);
    TenantConfig tenantConfig = new TenantConfig(getBrokerTenant(), getServerTenant(), null);
    tableConfig.setTenantConfig(tenantConfig);
    addTableConfig(tableConfig);
    createAndUploadSegmentFromFile(tableConfig, lookupTableSchema, DIM_TABLE_DATA_PATH, FileFormat.CSV,
        DIM_NUMBER_OF_RECORDS, 60_000);

    // Compare total rows in the primary table with number of rows in the result of the join with lookup table
    String query = "select count(*) from " + getTableName();
    JsonNode jsonNode = postQuery(query);
    long totalRowsInTable = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();

    query = "select /*+ joinOptions(join_strategy='lookup') */ AirlineID, DayOfWeek, dayName from " + getTableName()
        + " join daysOfWeek ON DayOfWeek = dayId where dayName in ('Monday', 'Tuesday', 'Wednesday')";
    jsonNode = postQuery(query);
    long result = jsonNode.get("resultTable").get("rows").size();
    assertTrue(result > 0);
    assertTrue(result < totalRowsInTable);

    // Verify that LOOKUP_JOIN stage is present and HASH_JOIN stage is not present in the query plan
    Set<String> stages = new HashSet<>();
    JsonNode currentNode = jsonNode.get("stageStats").get("children");
    while (currentNode != null) {
      currentNode = currentNode.get(0);
      stages.add(currentNode.get("type").asText());
      currentNode = currentNode.get("children");
    }
    assertTrue(stages.contains("LOOKUP_JOIN"), "Could not find LOOKUP_JOIN stage in the query plan");
    assertFalse(stages.contains("HASH_JOIN"), "HASH_JOIN stage should not be present in the query plan");

    dropOfflineTable(tableConfig.getTableName());
  }

  public void testSearchLiteralFilter() throws Exception {
    String sqlQuery =
        "WITH CTE_B AS (SELECT 1692057600000 AS __ts FROM mytable GROUP BY __ts) SELECT 1692057600000 AS __ts FROM "
            + "CTE_B WHERE __ts >= 1692057600000 AND __ts < 1693267200000 GROUP BY __ts";
    JsonNode explainPlan = postQuery("EXPLAIN PLAN FOR " + sqlQuery);
    assertTrue(explainPlan.get("resultTable").get("rows").get(0).get(1).asText().contains("SEARCH"));

    JsonNode result = postQuery(sqlQuery);
    assertNoError(result);
    assertEquals(result.get("resultTable").get("rows").size(), 1);
    assertEquals(result.get("resultTable").get("rows").get(0).get(0).asLong(), 1692057600000L);

    sqlQuery =
        "SELECT * FROM (SELECT CASE WHEN Carrier = 'garbage' THEN 'val1' ELSE 'val2' END as val FROM mytable) WHERE "
            + "val in ('val1', 'val2') LIMIT 1";
    explainPlan = postQuery("EXPLAIN PLAN FOR " + sqlQuery);
    assertTrue(explainPlan.get("resultTable").get("rows").get(0).get(1).asText().contains("SEARCH"));

    result = postQuery(sqlQuery);
    assertNoError(result);
    assertEquals(result.get("resultTable").get("rows").size(), 1);
    assertEquals(result.get("resultTable").get("rows").get(0).get(0).asText(), "val2");
  }

  @Test
  public void testPolymorphicScalarArrayFunctions() throws Exception {
    String query = "select ARRAY_LENGTH(ARRAY[1,2,3]);";
    JsonNode jsonNode = postQuery(query);
    assertNoError(jsonNode);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asInt(), 3);

    query = "select ARRAY_LENGTH(SPLIT('abc,xyz', ','));";
    jsonNode = postQuery(query);
    assertNoError(jsonNode);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asInt(), 2);
  }

  private void checkQueryResultForDBTest(String column, String tableName)
      throws Exception {
    checkQueryResultForDBTest(column, tableName, null, null);
  }

  private void checkQueryResultForDBTest(String column, String tableName, Map<String, String> headers)
      throws Exception {
    checkQueryResultForDBTest(column, tableName, null, headers);
  }

  private void checkQueryResultForDBTest(String column, String tableName, String database)
      throws Exception {
    checkQueryResultForDBTest(column, tableName, database, null);
  }

  private void checkQueryResultForDBTest(String column, String tableName, @Nullable String database,
      Map<String, String> headers)
      throws Exception {
    // max value of 'ActualElapsedTime'
    long expectedValue = 678;
    JsonNode jsonNode = getQueryResultForDBTest(column, tableName, database, headers);
    long result = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(result, expectedValue);
  }

  private void checkQueryPlanningErrorForDBTest(JsonNode queryResult, QueryErrorCode errorCode) {
    QueryAssert.assertThat(queryResult)
        .firstException()
        .hasErrorCode(errorCode);
  }

  private JsonNode getQueryResultForDBTest(String column, String tableName, @Nullable String database,
      Map<String, String> headers)
      throws Exception {
    String query = (StringUtils.isNotBlank(database) ? "SET database='" + database + "'; " : "")
        + "select max(" + column + ") from " + tableName + ";";
    return postQuery(query, headers);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);
    dropOfflineTable(TABLE_NAME_WITH_DATABASE);

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
