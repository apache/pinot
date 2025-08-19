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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;
import org.apache.pinot.tsdb.spi.series.SimpleTimeSeriesBuilderFactory;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TimeSeriesIntegrationTest extends BaseClusterIntegrationTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(TimeSeriesIntegrationTest.class);
  private static final String TS_COLUMN = "ts";
  private static final String DAYS_SINCE_FIRST_TRIP_COLUMN = "daysSinceFirstTrip";
  private static final String DEVICE_OS_COLUMN = "deviceOs";
  private static final String REFERRAL_COLUMN = "referralCol";
  private static final String TOTAL_TRIPS_COLUMN = "totalTrips";

  private static final String[] DEVICES = new String[]{"windows", "android", "ios"};
  private static final long NUMBER_OF_ROWS = 1000L;
  private static final long VIEWS_MIN_VALUE = 20L;
  private static final long VIEWS_MAX_VALUE = 30L;
  private static final long DATA_START_TIME_SEC = 1747008000L;
  private static final long QUERY_START_TIME_SEC = DATA_START_TIME_SEC - 60; // 1 minute before start time
  private static final long QUERY_END_TIME_SEC = DATA_START_TIME_SEC + 300; // 5 minutes after start time

  @Test(dataProvider = "isBrokerResponseCompatible")
  public void testGroupByMax(boolean isBrokerResponseCompatible) {
    String query = String.format(
      "fetch{table=\"mytable_OFFLINE\",filter=\"\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"%s\"}"
        + " | max{%s} | transformNull{0} | keepLastValue{}",
      TS_COLUMN, TOTAL_TRIPS_COLUMN, DEVICE_OS_COLUMN
    );
    runGroupedTimeSeriesQuery(query, 3, isBrokerResponseCompatible, (ts, val, row) ->
      assertEquals(val, ts <= DATA_START_TIME_SEC ? 0L : VIEWS_MAX_VALUE)
    );
  }

  @Test(dataProvider = "isBrokerResponseCompatible")
  public void testGroupByMin(boolean isBrokerResponseCompatible) {
    String query = String.format(
      "fetch{table=\"mytable_OFFLINE\",filter=\"\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"%s\"}"
        + " | min{%s} | transformNull{0} | keepLastValue{}",
      TS_COLUMN, TOTAL_TRIPS_COLUMN, DAYS_SINCE_FIRST_TRIP_COLUMN
    );
    runGroupedTimeSeriesQuery(query, 5, isBrokerResponseCompatible, (ts, val, row) ->
      assertEquals(val, ts <= DATA_START_TIME_SEC ? 0L : VIEWS_MIN_VALUE)
    );
  }

  @Test(dataProvider = "isBrokerResponseCompatible")
  public void testGroupBySum(boolean isBrokerResponseCompatible) {
    String query = String.format(
      "fetch{table=\"mytable_OFFLINE\",filter=\"\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"%s\"}"
        + " | sum{%s} | transformNull{0} | keepLastValue{}",
      TS_COLUMN, TOTAL_TRIPS_COLUMN, REFERRAL_COLUMN
    );
    runGroupedTimeSeriesQuery(query, 2, isBrokerResponseCompatible, (ts, val, row) -> {
      String referral = row.get(REFERRAL_COLUMN);
      long expected = ts <= DATA_START_TIME_SEC ? 0L
        // If referral is true, views are MAX_VALUE, otherwise 20
        : "1".equals(referral) ? 30 * VIEWS_MIN_VALUE : 30 * VIEWS_MAX_VALUE;
      assertEquals(val, expected);
    });
  }

  @Test(dataProvider = "isBrokerResponseCompatible")
  public void testGroupByTwoColumnsAndExpressionValue(boolean isBrokerResponseCompatible) {
    String query = String.format(
      "fetch{table=\"mytable_OFFLINE\",filter=\"\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"%s*10\"}"
        + " | max{%s,%s} | transformNull{0} | keepLastValue{}",
      TS_COLUMN, TOTAL_TRIPS_COLUMN, DEVICE_OS_COLUMN, DAYS_SINCE_FIRST_TRIP_COLUMN
    );
    runGroupedTimeSeriesQuery(query, 15, isBrokerResponseCompatible, (ts, val, row) -> {
      long expected = ts <= DATA_START_TIME_SEC ? 0L : 10 * VIEWS_MAX_VALUE;
      assertEquals(val, expected);
    });
  }

  @Test(dataProvider = "isBrokerResponseCompatible")
  public void testGroupByThreeColumnsAndConstantValue(boolean isBrokerResponseCompatible) {
    String query = String.format(
      "fetch{table=\"mytable_OFFLINE\",filter=\"\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"1\"}"
        + " | sum{%s,%s,%s} | transformNull{0} | keepLastValue{}",
      TS_COLUMN, TOTAL_TRIPS_COLUMN, DEVICE_OS_COLUMN, DAYS_SINCE_FIRST_TRIP_COLUMN, REFERRAL_COLUMN
    );
    runGroupedTimeSeriesQuery(query, 30, isBrokerResponseCompatible, (ts, val, row) -> {
      // Since there are 30 groups, each minute will have 2 rows.
      long expected = ts <= DATA_START_TIME_SEC ? 0L : 2L;
      assertEquals(val, expected);
    });
  }

  @Test(dataProvider = "isBrokerResponseCompatible")
  public void testGroupByWithFilter(boolean isBrokerResponseCompatible) {
    String query = String.format(
      "fetch{table=\"mytable_OFFLINE\",filter=\"%s='windows'\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"1\"}"
        + " | sum{%s,%s,%s} | transformNull{0} | keepLastValue{}",
      DEVICE_OS_COLUMN, TS_COLUMN, TOTAL_TRIPS_COLUMN, DEVICE_OS_COLUMN, DAYS_SINCE_FIRST_TRIP_COLUMN, REFERRAL_COLUMN
    );
    runGroupedTimeSeriesQuery(query, 10, isBrokerResponseCompatible, (ts, val, row) ->
      assertEquals(val, ts <= DATA_START_TIME_SEC ? 0L : 2L)
    );
  }

  @Test(dataProvider = "isBrokerResponseCompatible")
  public void testTransformNull(boolean isBrokerResponseCompatible) {
    String query = String.format(
      "fetch{table=\"mytable_OFFLINE\",filter=\"\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"%s\"}"
        + " | max{%s} | transformNull{42} | keepLastValue{}",
      TS_COLUMN, TOTAL_TRIPS_COLUMN, DEVICE_OS_COLUMN
    );
    runGroupedTimeSeriesQuery(query, 3, isBrokerResponseCompatible, (ts, val, row) ->
      assertEquals(val, ts <= DATA_START_TIME_SEC ? 42L : VIEWS_MAX_VALUE)
    );
  }

  @Test(dataProvider = "isBrokerResponseCompatible")
  public void testTableWithoutType(boolean isBrokerResponseCompatible) {
    String query = String.format(
      "fetch{table=\"mytable\",filter=\"\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"%s\"}"
        + " | max{%s} | transformNull{0} | keepLastValue{}",
      TS_COLUMN, TOTAL_TRIPS_COLUMN, DEVICE_OS_COLUMN
    );
    runGroupedTimeSeriesQuery(query, 3, isBrokerResponseCompatible, (ts, val, row) ->
      assertEquals(val, ts <= DATA_START_TIME_SEC ? 0L : VIEWS_MAX_VALUE)
    );
  }

  @Test
  public void testStartTimeEqualsEndTimeQuery() {
    String query = String.format(
      "fetch{table=\"mytable_OFFLINE\",filter=\"\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"%s\"}"
        + " | max{%s} | transformNull{0} | keepLastValue{}",
      TS_COLUMN, TOTAL_TRIPS_COLUMN, DEVICE_OS_COLUMN
    );
    JsonNode result = getTimeseriesQuery(query, QUERY_START_TIME_SEC, QUERY_START_TIME_SEC, getHeaders());
    assertEquals(result.get("status").asText(), "success");
    JsonNode series = result.get("data").get("result");
    assertEquals(series.size(), 0);
  }

  @Test
  public void testControllerTimeseriesEndpoints()
      throws Exception {
    // Call /timeseries/api/v1/query_range.
    String query = String.format(
        "fetch{table=\"mytable_OFFLINE\",filter=\"\",ts_column=\"%s\",ts_unit=\"MILLISECONDS\",value=\"%s\"}"
            + " | max{%s} | transformNull{0} | keepLastValue{}",
        TS_COLUMN, TOTAL_TRIPS_COLUMN, DEVICE_OS_COLUMN
    );
    JsonNode result = getTimeseriesQuery(getControllerBaseApiUrl(), query, QUERY_START_TIME_SEC, QUERY_END_TIME_SEC,
        getHeaders());
    assertEquals(result.get("status").asText(), "success");

    // Call /timeseries/languages.
    var statusCodeAndResponse = sendGetRequestWithStatusCode(
        getControllerBaseApiUrl() + "/timeseries/languages", getHeaders());
    assertEquals(statusCodeAndResponse.getLeft(), 200);
    assertEquals(statusCodeAndResponse.getRight(), "[\"m3ql\"]");
  }

  @DataProvider(name = "isBrokerResponseCompatible")
  public Object[][] isBrokerResponseCompatible() {
    return new Object[][]{
        {false}, {true}
    };
  }

  protected Map<String, String> getHeaders() {
    return Collections.emptyMap();
  }

  private void runGroupedTimeSeriesQuery(String query, int expectedGroups, boolean isBrokerResponseCompatible,
      TimeSeriesValidator validator) {
    JsonNode result = isBrokerResponseCompatible
        ? postTimeseriesQuery(getBrokerBaseApiUrl(), query, QUERY_START_TIME_SEC, QUERY_END_TIME_SEC, getHeaders())
        : getTimeseriesQuery(query, QUERY_START_TIME_SEC, QUERY_END_TIME_SEC, getHeaders());

    if (isBrokerResponseCompatible) {
      validateBrokerResponse(result, expectedGroups, validator);
    } else {
      validatePrometheusResponse(result, expectedGroups, validator);
    }
  }

  private void validatePrometheusResponse(JsonNode result, int expectedGroups, TimeSeriesValidator validator) {
    assertEquals("success", result.path("status").asText());

    JsonNode series = result.path("data").path("result");
    assertEquals(expectedGroups, series.size());

    for (JsonNode row : series) {
      Map<String, String> metric;
      try {
        metric = JsonUtils.jsonNodeToStringMap(row.path("metric"));
      } catch (Exception e) {
        throw new RuntimeException("Failed to parse metric from row", e);
      }
      for (JsonNode point : row.path("values")) {
        long ts = point.get(0).asLong();
        long val = point.get(1).asLong();
        validator.validate(ts, val, metric);
      }
    }
  }

  private void validateBrokerResponse(JsonNode result, int expectedGroups, TimeSeriesValidator validator) {
    assertNotNull(result);
    assertEquals(expectedGroups, result.path("numRowsResultSet").asInt());

    JsonNode resultTable = result.path("resultTable");
    assertNotNull(resultTable);

    List<String> columnNames = extractStrings(resultTable.path("dataSchema").path("columnNames"));
    JsonNode rows = resultTable.path("rows");

    for (JsonNode jsonRow : rows) {
      ArrayNode row = (ArrayNode) jsonRow;
      assertEquals(columnNames.size(), row.size());

      ArrayNode tsArray = (ArrayNode) row.get(0);
      ArrayNode valArray = (ArrayNode) row.get(1);
      assertEquals(tsArray.size(), valArray.size());

      Map<String, String> metric = new HashMap<>();
      for (int i = 2; i < row.size(); i++) {
        metric.put(columnNames.get(i), row.get(i).asText());
      }

      for (int i = 0; i < tsArray.size(); i++) {
        validator.validate(tsArray.get(i).asLong(), valArray.get(i).asLong(), metric);
      }
    }
  }

  private List<String> extractStrings(JsonNode arrayNode) {
    List<String> result = new ArrayList<>();
    arrayNode.forEach(node -> result.add(node.asText()));
    return result;
  }

  @Override
  protected void overrideControllerConf(Map<String, Object> properties) {
    properties.put(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), "m3ql");
    properties.put(PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey("m3ql"),
        "org.apache.pinot.tsdb.m3ql.M3TimeSeriesPlanner");
    properties.put(PinotTimeSeriesConfiguration.getSeriesBuilderFactoryConfigKey("m3ql"),
        SimpleTimeSeriesBuilderFactory.class.getName());
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    addTimeSeriesConfigurations(brokerConf);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    addTimeSeriesConfigurations(serverConf);
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public long getCountStarResult() {
    return NUMBER_OF_ROWS;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
      .addSingleValueDimension(TS_COLUMN, FieldSpec.DataType.LONG)
      .addSingleValueDimension(DAYS_SINCE_FIRST_TRIP_COLUMN, FieldSpec.DataType.LONG)
      .addSingleValueDimension(DEVICE_OS_COLUMN, FieldSpec.DataType.STRING)
      .addSingleValueDimension(REFERRAL_COLUMN, FieldSpec.DataType.BOOLEAN)
      .addSingleValueDimension(TOTAL_TRIPS_COLUMN, FieldSpec.DataType.LONG)
      .build();
  }

  @BeforeClass
  public void setUp()
    throws Exception {
    LOGGER.info("Setting up integration test class: {}", getClass().getSimpleName());
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    if (_controllerRequestURLBuilder == null) {
      _controllerRequestURLBuilder = ControllerRequestURLBuilder.baseUrl("http://localhost:" + getControllerPort());
    }
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
    // create & upload schema AND table config
    Schema schema = createSchema();
    addSchema(schema);

    File avroFile = createAvroFile();
    // create offline table
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // create & upload segments
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    waitForAllDocsLoaded(60_000);
    LOGGER.info("Finished setting up integration test class: {}", getClass().getSimpleName());
  }

  @AfterClass
  public void tearDown()
    throws Exception {
    LOGGER.info("Tearing down integration test class: {}", getClass().getSimpleName());
    dropOfflineTable(getTableName());
    FileUtils.deleteDirectory(_tempDir);

    // Shutdown the Pinot cluster
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    LOGGER.info("Finished tearing down integration test class: {}", getClass().getSimpleName());
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }

  public File createAvroFile()
    throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
      createAvroField(TS_COLUMN, org.apache.avro.Schema.Type.LONG),
      createAvroField(DAYS_SINCE_FIRST_TRIP_COLUMN, org.apache.avro.Schema.Type.LONG),
      createAvroField(DEVICE_OS_COLUMN, org.apache.avro.Schema.Type.STRING),
      createAvroField(REFERRAL_COLUMN, org.apache.avro.Schema.Type.BOOLEAN),
      createAvroField(TOTAL_TRIPS_COLUMN, org.apache.avro.Schema.Type.LONG)
    ));

    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, avroFile);
      for (int i = 0; i < getCountStarResult(); i++) {
        writer.append(getRecord(avroSchema, i));
      }
    }
    return avroFile;
  }

  private org.apache.avro.Schema.Field createAvroField(String name, org.apache.avro.Schema.Type type) {
    return new org.apache.avro.Schema.Field(name, org.apache.avro.Schema.create(type), null, null);
  }

  private static GenericData.Record getRecord(org.apache.avro.Schema avroSchema, int i) {
    GenericData.Record record = new GenericData.Record(avroSchema);
    // Do not set DATA_START_TIME_SEC for easier assertion of values.
    record.put(TS_COLUMN, (DATA_START_TIME_SEC + 1 + i) * 1000L);
    record.put(DAYS_SINCE_FIRST_TRIP_COLUMN, i % 5);
    record.put(DEVICE_OS_COLUMN, DEVICES[i % DEVICES.length]);
    record.put(REFERRAL_COLUMN, (i % 2) == 0);
    // Alternate between VIEWS_MIN_VALUE and VIEWS_MAX_VALUE.
    record.put(TOTAL_TRIPS_COLUMN, VIEWS_MIN_VALUE + (VIEWS_MAX_VALUE - VIEWS_MIN_VALUE) * (i % 2));
    return record;
  }

  private void addTimeSeriesConfigurations(PinotConfiguration conf) {
    conf.setProperty(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), "m3ql");
    conf.setProperty(PinotTimeSeriesConfiguration.getLogicalPlannerConfigKey("m3ql"),
      "org.apache.pinot.tsdb.m3ql.M3TimeSeriesPlanner");
    conf.setProperty(PinotTimeSeriesConfiguration.getSeriesBuilderFactoryConfigKey("m3ql"),
      SimpleTimeSeriesBuilderFactory.class.getName());
  }

  @FunctionalInterface
  interface TimeSeriesValidator {
    void validate(long timestamp, long value, Map<String, String> metricMap);
  }
}
