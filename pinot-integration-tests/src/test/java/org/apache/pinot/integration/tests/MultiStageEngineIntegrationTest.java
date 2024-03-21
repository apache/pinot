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
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.function.scalar.StringFunctions.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class MultiStageEngineIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String SCHEMA_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset_nonulls.schema";
  private static final String DEFAULT_DATABASE_NAME = CommonConstants.DEFAULT_DATABASE;
  private static final String DATABASE_NAME = "db1";
  private static final String TABLE_NAME_WITH_DATABASE = DATABASE_NAME + "." + DEFAULT_TABLE_NAME;
  private String _tableName = DEFAULT_TABLE_NAME;

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
    startController(getDefaultControllerConfiguration());
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

//  @Override
//  protected boolean useMultiStageQueryEngine() {
//    return true;
//  }

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
    double[] expectedNumericResultsV1 = new double[]{
        364, 364, 357, 364, 364, 364, 5915969, 16252.662087912087
    };
    Assert.assertEquals(numericResultFunctions.length, expectedNumericResults.length);

    for (int i = 0; i < numericResultFunctions.length; i++) {
      String pinotQuery = String.format("SELECT %s(DaysSinceEpoch) FROM mytable", numericResultFunctions[i]);
      JsonNode jsonNode = postQuery(pinotQuery);
      if (useMultiStageQueryEngine) {
        Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble(),
            expectedNumericResults[i]);
      } else {
        Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble(),
            expectedNumericResultsV1[i]);
      }
    }

    String[] binaryResultFunctions = new String[]{
        "distinctCountRawHLL", "distinctCountRawThetaSketch"
    };
    int[] expectedBinarySizeResults = new int[]{
        360,
        3904
    };
    int[] expectedBinarySizeResultsV1 = new int[]{
        5480,
        3904
    };
    for (int i = 0; i < binaryResultFunctions.length; i++) {
      String pinotQuery = String.format("SELECT %s(DaysSinceEpoch) FROM mytable", binaryResultFunctions[i]);
      JsonNode jsonNode = postQuery(pinotQuery);
      if (useMultiStageQueryEngine) {
        Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asText().length(),
            expectedBinarySizeResults[i]);
      } else {
        Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asText().length(),
            expectedBinarySizeResultsV1[i]);
      }
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
    double[] expectedResultsV1 = new double[]{
        -5.421344202E9, 577725, -9999.0, 16271.0, -9383.95292223809, 26270.0, 312, 312, 312, 3954484.0,
        12674.628205128205
    };

    Assert.assertEquals(multiValueFunctions.length, expectedResults.length);

    for (int i = 0; i < multiValueFunctions.length; i++) {
      String pinotQuery = String.format("SELECT %s(DivAirportIDs) FROM mytable", multiValueFunctions[i]);
      JsonNode jsonNode = postQuery(pinotQuery);
      if (useMultiStageQueryEngine) {
        Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble(), expectedResults[i]);
      } else {
        Assert.assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asDouble(), expectedResultsV1[i]);
      }
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
    int expectedStatusCode = useMultiStageQueryEngine() ? QueryException.QUERY_PLANNING_ERROR_CODE
        : QueryException.SQL_PARSING_ERROR_CODE;
    Assert.assertEquals(response.get("exceptions").get(0).get("errorCode").asInt(), expectedStatusCode);

    // invalid argument
    sqlQuery = "SELECT fromBase64('hello!') FROM mytable";
    response = postQuery(sqlQuery);
    assertTrue(response.get("exceptions").get(0).get("message").toString().contains("Illegal base64 character"));

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
        "SELECT 1, now() as currentTs, ago('PT1H') as oneHourAgoTs, 'abc', toDateTime(now(), 'yyyy-MM-dd z') as "
            + "today, now(), ago('PT1H'), encodeUrl('key1=value 1&key2=value@!$2&key3=value%3') as encodedUrl, "
            + "decodeUrl('key1%3Dvalue+1%26key2%3Dvalue%40%21%242%26key3%3Dvalue%253') as decodedUrl, toBase64"
            + "(toUtf8('hello!')) as toBase64, fromUtf8(fromBase64('aGVsbG8h')) as fromBase64";
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
    assertTrue(nowResult >= queryStartTimeMs);
    assertTrue(nowResult <= queryEndTimeMs);
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
    assertTrue(nowResult >= queryStartTimeMs);
    assertTrue(nowResult <= queryEndTimeMs);
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
    String pinotQuery = "SELECT count(*), arrayToMV(RandomAirports) FROM mytable "
        + "GROUP BY arrayToMV(RandomAirports)";
    JsonNode jsonNode = postQuery(pinotQuery);
    Assert.assertEquals(jsonNode.get("resultTable").get("rows").size(), 154);
  }

  @Test
  public void testMultiValueColumnGroupByOrderBy()
      throws Exception {
    String pinotQuery =
        "SELECT count(*), arrayToMV(RandomAirports) FROM mytable " + "GROUP BY arrayToMV(RandomAirports) "
            + "ORDER BY arrayToMV(RandomAirports) DESC";
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
    String sqlQuery = "SELECT CASE WHEN ArrDelay > 50 OR ArrDelay < 10 THEN 10 ELSE 0 END "
        + "FROM mytable LIMIT 1000";
    JsonNode jsonNode = postQuery("Explain plan for " + sqlQuery);
    JsonNode plan = jsonNode.get("resultTable").get("rows").get(0).get(1);

    Pattern pattern = Pattern.compile("SEARCH\\(\\$7, Sarg\\[\\(-∞\\.\\.10\\), \\(50\\.\\.\\+∞\\)]\\)");
    boolean matches = pattern.matcher(plan.asText()).find();
    Assert.assertTrue(matches, "Plan doesn't contain the expected SEARCH");

    jsonNode = postQuery(sqlQuery);
    assertNoError(jsonNode);
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
  public void testWithDefaultDatabaseContextAsTableNamePrefixAndQueryOption()
      throws Exception {
    // default database check. Default database context passed as table prefix as well as query option
    checkQueryResultForDBTest("ActualElapsedTime", DEFAULT_DATABASE_NAME + "." + DEFAULT_TABLE_NAME,
        DEFAULT_DATABASE_NAME);
  }

  @Test
  public void testWithDatabaseContextAsTableNamePrefix()
      throws Exception {
    // Using renamed column "ActualElapsedTime_2" to ensure that the same table is not being queried.
    // custom database check. Database context passed as table prefix
    checkQueryResultForDBTest("ActualElapsedTime_2", TABLE_NAME_WITH_DATABASE);
  }

  @Test
  public void testWithDatabaseContextAsQueryOption()
      throws Exception {
    // Using renamed column "ActualElapsedTime_2" to ensure that the same table is not being queried.
    // custom database check. Database context passed as SET database='dbName'
    checkQueryResultForDBTest("ActualElapsedTime_2", DEFAULT_TABLE_NAME, DATABASE_NAME);
  }

  @Test
  public void testWithDatabaseContextAsTableNamePrefixAndQueryOption()
      throws Exception {
    // Using renamed column "ActualElapsedTime_2" to ensure that the same table is not being queried.
    // custom database check. Database context passed as table prefix as well as query option
    checkQueryResultForDBTest("ActualElapsedTime_2", TABLE_NAME_WITH_DATABASE, DATABASE_NAME);
  }

  @Test
  public void testCrossDatabaseQuery()
      throws Exception {
    String query = "SELECT tb1.Carrier, maxTime, distance FROM (SELECT max(AirTime) AS maxTime, Carrier FROM "
        + DEFAULT_TABLE_NAME + " GROUP BY Carrier ORDER BY maxTime DESC) AS tb1 JOIN (SELECT sum(Distance) AS distance,"
        + " Carrier FROM " + TABLE_NAME_WITH_DATABASE + " GROUP BY Carrier) AS tb2 "
        + "ON tb1.Carrier = tb2.Carrier; ";
    JsonNode result = postQuery(query);
    assertEquals(result.get("exceptions").get(0).get("errorCode").asInt(), QueryException.QUERY_PLANNING_ERROR_CODE);
  }

  private void checkQueryResultForDBTest(String column, String tableName)
      throws Exception {
    checkQueryResultForDBTest(column, tableName, null);
  }

  private void checkQueryResultForDBTest(String column, String tableName, @Nullable String database)
      throws Exception {
    String query = (StringUtils.isNotBlank(database) ? "SET database='" + database + "'; " : "")
        + "select max(" + column + ") from " + tableName + ";";
    // max value of 'ActualElapsedTime'
    long expectedValue = 678;
    JsonNode jsonNode = postQuery(query);
    long result = jsonNode.get("resultTable").get("rows").get(0).get(0).asLong();
    assertEquals(result, expectedValue);
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
