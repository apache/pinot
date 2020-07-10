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
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.spi.config.table.IngestionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Hybrid cluster integration test that uses one of the DateTimeFieldSpec as primary time column
 */
public class Temp extends BaseClusterIntegrationTest {

  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  private static final String SCHEMA_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset_nonulls_ingestion_config.schema";

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServers(2);

    // Start Kafka
    //startKafka();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles.subList(0, avroFiles.size() - 1), offlineTableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    addTableConfig(createRealtimeTableConfig(null));

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles.subList(avroFiles.size() - 1, avroFiles.size() - 1));

    Thread.sleep(120_000);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  protected List<File> getAllAvroFiles()
      throws Exception {
    // Unpack the Avro files
    int numSegments = unpackAvroData(_tempDir).size();

    // Avro files has to be ordered as time series data
    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(_tempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }

    return avroFiles;
  }

  protected List<File> getOfflineAvroFiles(List<File> avroFiles) {
    int numOfflineSegments = 2;
    List<File> offlineAvroFiles = new ArrayList<>(numOfflineSegments);
    for (int i = 0; i < numOfflineSegments; i++) {
      offlineAvroFiles.add(avroFiles.get(i));
    }
    return offlineAvroFiles;
  }

  protected List<File> getRealtimeAvroFiles(List<File> avroFiles) {
    int numSegments = avroFiles.size();
    int numRealtimeSegments = 2;
    List<File> realtimeAvroFiles = new ArrayList<>(numRealtimeSegments);
    for (int i = numSegments - numRealtimeSegments; i < numSegments; i++) {
      realtimeAvroFiles.add(avroFiles.get(i));
    }
    return realtimeAvroFiles;
  }

  protected String getSchemaFileName() {
    return SCHEMA_FILE_NAME;
  }

  @Override
  protected String getTimeColumnName() {
    return TIME_COLUMN_NAME;
  }

  @Override
  protected boolean useLlc() {
    return true;
  }

  @Override
  protected IngestionConfig getIngestionConfig() {
    FilterConfig filterConfig = new FilterConfig("Groovy({AirlineID == 19393 || ArrDelayMinutes <= 5 }, AirlineID, ArrDelayMinutes)");
    List<TransformConfig> transformConfigs = new ArrayList<>();
    transformConfigs.add(new TransformConfig("AmPm", "Groovy({DepTime < 1200 ? \"AM\": \"PM\"}, DepTime)"));
    transformConfigs.add(new TransformConfig("millisSinceEpoch", "fromEpochDays(DaysSinceEpoch)"));
    transformConfigs.add(new TransformConfig("lowerCaseDestCityName", "lower(DestCityName)"));
    return new IngestionConfig(filterConfig, transformConfigs);
  }

  @Test
  public void testQueries()
      throws Exception {
    // Select column created with transform function
    String sqlQuery = "Select millisSinceEpoch from " + DEFAULT_TABLE_NAME;
    JsonNode response = postSqlQuery(sqlQuery);
    assertEquals(response.get("resultTable").get("dataSchema").get("columnNames").get(0).asText(), "millisSinceEpoch");
    assertEquals(response.get("resultTable").get("dataSchema").get("columnDataTypes").get(0).asText(), "LONG");

    // Select column created with transform function
    sqlQuery = "Select AmPm, DepTime from " + DEFAULT_TABLE_NAME;
    response = postSqlQuery(sqlQuery);
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
    response = postSqlQuery(sqlQuery);
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
    response = postSqlQuery(sqlQuery);
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
    sqlQuery = "Select * from " + DEFAULT_TABLE_NAME
        + "  where AirlineID = 19393 or ArrDelayMinutes <= 5";
    response = postSqlQuery(sqlQuery);
    Assert.assertEquals(response.get("resultTable").get("rows").size(), 0);

    // Check there's no values that should've been filtered - realtime table
    sqlQuery = "Select * from " + DEFAULT_TABLE_NAME + "_REALTIME"
        + "  where AirlineID = 19393 or ArrDelayMinutes <= 5";
    response = postSqlQuery(sqlQuery);
    Assert.assertEquals(response.get("resultTable").get("rows").size(), 0);

    // Check there's no values that should've been filtered - offline table
    sqlQuery = "Select * from " + DEFAULT_TABLE_NAME + "_OFFLINE"
        + "  where AirlineID = 19393 or ArrDelayMinutes <= 5";
    response = postSqlQuery(sqlQuery);
    Assert.assertEquals(response.get("resultTable").get("rows").size(), 0);

  }

  @AfterClass
  public void tearDown()
      throws Exception {
    String tableName = getTableName();
    dropOfflineTable(tableName);
    dropRealtimeTable(tableName);
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }
}
