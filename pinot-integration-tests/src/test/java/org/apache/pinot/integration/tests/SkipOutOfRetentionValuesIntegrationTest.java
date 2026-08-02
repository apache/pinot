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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration test for the `skipOutOfRetentionValues` query option.
 *
 * Verifies that when the query option is provided, the broker dynamically injects a time
 * filter based on the table's retention configuration and pushes it down to the servers.
 */
public class SkipOutOfRetentionValuesIntegrationTest extends BaseClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "retentionTestTable";
  private static final String TIME_COLUMN = "eventTime";
  private static final int RETENTION_DAYS = 30;

  @Override
  protected String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @BeforeClass
  public void setUp() throws Exception {

    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zookeeper, Controller, Broker, Server
    startZk();
    startController();
    startBroker();
    startServer();

    // Create Schema
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(DEFAULT_TABLE_NAME)
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .addSingleValueDimension("category", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    addSchema(schema);

    // Create TableConfig with 30-day retention
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(DEFAULT_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue(String.valueOf(RETENTION_DAYS))
        .build();
    addTableConfig(tableConfig);

    // Generate test data using current time as baseline
    long now = System.currentTimeMillis();
    long daysInMs = TimeUnit.DAYS.toMillis(1);

    List<GenericRow> rows = new ArrayList<>();

    // Row 1: 40 days ago (OUTSIDE retention)
    rows.add(createRow(1, "A", now - (40 * daysInMs)));
    // Row 2: 20 days ago (INSIDE retention)
    rows.add(createRow(2, "B", now - (20 * daysInMs)));
    // Row 3: 10 days ago (INSIDE retention)
    rows.add(createRow(3, "A", now - (10 * daysInMs)));
    // Row 4: 2 days ago (INSIDE retention)
    rows.add(createRow(4, "C", now - (2 * daysInMs)));

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(_segmentDir.getPath());
    config.setTableName(DEFAULT_TABLE_NAME);
    SegmentIndexCreationDriverImpl driverImpl = new SegmentIndexCreationDriverImpl();
    driverImpl.init(config, new GenericRowRecordReader(rows));
    driverImpl.build();

    File indexDir = new File(_segmentDir, driverImpl.getSegmentName());
    File segmentTarFile = new File(_tarDir, driverImpl.getSegmentName() + ".tar.gz");
    TarCompressionUtils.createCompressedTarFile(indexDir, segmentTarFile);
    uploadSegments(DEFAULT_TABLE_NAME, _tarDir);

    waitForAllDocsLoaded(60_000L);
  }

  private GenericRow createRow(int id, String category, long eventTime) {
    GenericRow row = new GenericRow();
    row.putValue("id", id);
    row.putValue("category", category);
    row.putValue(TIME_COLUMN, eventTime);
    return row;
  }

  //should return all 4 records including the one outside the retention period.
  @Test
  public void testWithoutQueryOption() throws Exception {
    String query = "SELECT count(*) FROM " + DEFAULT_TABLE_NAME;
    JsonNode response = postQuery(query);

    long count = response.get("resultTable").get("rows").get(0).get(0).asLong();
    Assert.assertEquals(count, 4L, "Expected all 4 rows to be returned without the query option");
  }

  //should inject retention filter and return only the records within retention time bounds (3)
  @Test
  public void testWithQueryOption() throws Exception {
    String query = "SET skipOutOfRetentionValues='true'; SELECT count(*) FROM " + DEFAULT_TABLE_NAME;
    JsonNode response = postQuery(query);

    long count = response.get("resultTable").get("rows").get(0).get(0).asLong();
    Assert.assertEquals(count, 3L, "Expected exactly 3 rows (filtered by 30 day retention limit)");
  }

  @Test
  public void testWithQueryOptionAndWhereClause() throws Exception {
    // Should combine retention filter with existing filter via AND
    // Category 'A' has 2 rows: one 40 days ago (outside), one 10 days ago (inside)
    String query = "SET skipOutOfRetentionValues='true'; SELECT count(*) FROM " + DEFAULT_TABLE_NAME
        + " WHERE category = 'A'";
    JsonNode response = postQuery(query);

    long count = response.get("resultTable").get("rows").get(0).get(0).asLong();
    Assert.assertEquals(count, 1L, "Expected 1 row for category A inside the retention period");
  }

  @Override
  protected long getCountStarResult() {
    return 4;
  }

  @AfterClass
  public void tearDown() throws Exception {
    dropOfflineTable(DEFAULT_TABLE_NAME);
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
