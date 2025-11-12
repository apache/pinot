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
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * End-to-end test that enables the broker query-log system table, generates a small dataset, issues a few queries,
 * and validates that the controller query-log endpoint returns the expected rows and latency statistics.
 */
public class QueryLogSystemTableIntegrationTest extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryLogSystemTableIntegrationTest.class);
  private static final String TABLE_NAME = "queryLogTestTable";
  private static final String STRING_COLUMN = "carrier";
  private static final String METRIC_COLUMN = "latencyBucket";
  private static final int NUM_ROWS = 20;
  private static final int NUM_SAMPLE_QUERIES = 5;

  @BeforeClass
  public void setUp()
      throws Exception {
    LOGGER.warn("Setting up integration test class: {}", getClass().getSimpleName());
    ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startKafka();
    startController();
    startBroker();
    startServer();

    if (_controllerRequestURLBuilder == null) {
      _controllerRequestURLBuilder =
          ControllerRequestURLBuilder.baseUrl("http://localhost:" + getControllerPort());
    }

    Schema schema = createSchema();
    addSchema(schema);

    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    File avroFile = createAvroFile();
    ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFile, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);
    waitForAllDocsLoaded(60_000);
    LOGGER.warn("Finished setting up integration test class: {}", getClass().getSimpleName());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    LOGGER.warn("Tearing down integration test class: {}", getClass().getSimpleName());
    dropOfflineTable(getTableName());
    FileUtils.deleteDirectory(_tempDir);
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    LOGGER.warn("Finished tearing down integration test class: {}", getClass().getSimpleName());
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_ENABLED, true);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_MAX_ENTRIES, 10_000);
    brokerConf.setProperty(CommonConstants.Broker.CONFIG_OF_QUERY_LOG_SYSTEM_TABLE_FANOUT_ENABLED, true);
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_ROWS;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
        .addMetric(METRIC_COLUMN, FieldSpec.DataType.LONG)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
  }

  public File createAvroFile()
      throws Exception {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("QueryLogRecord").namespace("org.apache.pinot")
        .fields()
        .requiredString(STRING_COLUMN)
        .requiredLong(METRIC_COLUMN)
        .endRecord();
    File avroFile = new File(_tempDir, "query-log-input.avro");
    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_ROWS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(STRING_COLUMN, "carrier-" + (i % 3));
        record.put(METRIC_COLUMN, i);
        writer.append(record);
      }
    }
    return avroFile;
  }

  @Test
  public void testQueryLogSystemTableCapturesQueries()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    List<Long> reportedLatencies = new ArrayList<>();
    String baseQuery = String.format("SELECT COUNT(*) FROM %s WHERE %s >= 0", getTableName(), METRIC_COLUMN);
    LOGGER.info("Controller base API URL: {}", getControllerBaseApiUrl());

    for (int i = 0; i < NUM_SAMPLE_QUERIES; i++) {
      JsonNode response = postQuery(baseQuery);
      assertTrue(response.get("exceptions").isEmpty(), "Expected successful response");
      reportedLatencies.add(response.get("timeUsedMs").asLong());
    }

    String queryFilter = String.format("query = '%s'", baseQuery);

    JsonNode lastCountResponse = null;
    long deadline = System.currentTimeMillis() + 10_000L;
    boolean observed = false;
    while (System.currentTimeMillis() < deadline) {
      lastCountResponse =
          querySystemTable(String.format("SELECT COUNT(*) FROM system.query_log WHERE %s", queryFilter));
      JsonNode rows = getRows(lastCountResponse);
      if (rows != null && rows.size() == 1 && rows.get(0).get(0).asLong() >= NUM_SAMPLE_QUERIES) {
        observed = true;
        break;
      }
      Thread.sleep(200L);
    }
    assertTrue(observed, "Failed to observe query log entries. Last response: " + lastCountResponse);

    JsonNode detailResponse = querySystemTable(
        String.format("SELECT timeMs FROM system.query_log WHERE %s ORDER BY timestampMs DESC LIMIT %d", queryFilter,
            NUM_SAMPLE_QUERIES));
    JsonNode rows = getRows(detailResponse);
    assertEquals(rows.size(), NUM_SAMPLE_QUERIES,
        "system.query_log should contain exactly the queries issued during the test");

    List<Long> loggedLatencies = new ArrayList<>(NUM_SAMPLE_QUERIES);
    for (JsonNode row : rows) {
      loggedLatencies.add(row.get(0).asLong());
    }

    JsonNode avgResponse =
        querySystemTable(String.format("SELECT AVG(timeMs) FROM system.query_log WHERE %s", queryFilter));
    double loggedAverage = getRows(avgResponse).get(0).get(0).asDouble();
    double expectedAverage = reportedLatencies.stream().mapToLong(Long::longValue).average().orElse(0D);
    assertEquals(loggedAverage, expectedAverage, 1.0d, "Average latency recorded in system.query_log should match");
  }

  private static JsonNode getRows(JsonNode response) {
    JsonNode resultTable = response.get("resultTable");
    return resultTable != null ? resultTable.get("rows") : null;
  }

  private JsonNode querySystemTable(String sql)
      throws Exception {
    return postQueryToController(sql);
  }

  private static void ensureDirectoriesExistAndEmpty(File... dirs)
      throws IOException {
    for (File dir : dirs) {
      FileUtils.deleteDirectory(dir);
      if (!dir.mkdirs()) {
        throw new IOException("Failed to create directory: " + dir);
      }
    }
  }
}
