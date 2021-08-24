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
import com.google.common.collect.Lists;
import java.io.File;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test to check aggregation functions which use {@code DictionaryBasedAggregationPlanNode} and
 * {@code MetadataBasedAggregationPlanNode}.
 */
// TODO: remove this integration test and add unit test for metadata and dictionary based aggregation operator
public class MetadataAndDictionaryAggregationPlanClusterIntegrationTest extends BaseClusterIntegrationTest {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

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

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @Test
  public void testDictionaryBasedQueries()
      throws Exception {
    String tableName = getTableName();
    String pqlQuery;
    String sqlQuery;
    String sqlQuery1;
    String sqlQuery2;
    String sqlQuery3;

    // Test queries with min, max, minmaxrange
    // Dictionary columns
    // int
    pqlQuery = "SELECT MAX(ArrTime) FROM " + tableName;
    sqlQuery = "SELECT MAX(ArrTime) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrTime) FROM " + tableName;
    sqlQuery = "SELECT MIN(ArrTime) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ArrTime) FROM " + tableName;
    sqlQuery = "SELECT MAX(ArrTime)-MIN(ArrTime) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrTime), MAX(ArrTime), MINMAXRANGE(ArrTime) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(ArrTime) FROM " + tableName;
    sqlQuery2 = "SELECT MAX(ArrTime) FROM " + tableName;
    sqlQuery3 = "SELECT MAX(ArrTime)-MIN(ArrTime) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ArrTime), COUNT(*) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(ArrTime) FROM " + tableName;
    sqlQuery2 = "SELECT COUNT(*) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));
    // float
    pqlQuery = "SELECT MAX(DepDelayMinutes) FROM " + tableName;
    sqlQuery = "SELECT MAX(DepDelayMinutes) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelayMinutes) FROM " + tableName;
    sqlQuery = "SELECT MIN(DepDelayMinutes) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(DepDelayMinutes) FROM " + tableName;
    sqlQuery = "SELECT MAX(DepDelayMinutes)-MIN(DepDelayMinutes) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelayMinutes), MAX(DepDelayMinutes), MINMAXRANGE(DepDelayMinutes) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(DepDelayMinutes) FROM " + tableName;
    sqlQuery2 = "SELECT MAX(DepDelayMinutes) FROM " + tableName;
    sqlQuery3 = "SELECT MAX(DepDelayMinutes)-MIN(DepDelayMinutes) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(DepDelayMinutes), COUNT(*) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(DepDelayMinutes) FROM " + tableName;
    sqlQuery2 = "SELECT COUNT(*) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // double
    pqlQuery = "SELECT MAX(ArrDelayMinutes) FROM " + tableName;
    sqlQuery = "SELECT MAX(ArrDelayMinutes) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelayMinutes) FROM " + tableName;
    sqlQuery = "SELECT MIN(ArrDelayMinutes) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ArrDelayMinutes) FROM " + tableName;
    sqlQuery = "SELECT MAX(ArrDelayMinutes)-MIN(ArrDelayMinutes) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelayMinutes), MAX(ArrDelayMinutes), MINMAXRANGE(ArrDelayMinutes) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(ArrDelayMinutes) FROM " + tableName;
    sqlQuery2 = "SELECT MAX(ArrDelayMinutes) FROM " + tableName;
    sqlQuery3 = "SELECT MAX(ArrDelayMinutes)-MIN(ArrDelayMinutes) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ArrDelayMinutes), COUNT(*) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(ArrDelayMinutes) FROM " + tableName;
    sqlQuery2 = "SELECT COUNT(*) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // long
    pqlQuery = "SELECT MAX(AirlineID) FROM " + tableName;
    sqlQuery = "SELECT MAX(AirlineID) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(AirlineID) FROM " + tableName;
    sqlQuery = "SELECT MIN(AirlineID) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(AirlineID) FROM " + tableName;
    sqlQuery = "SELECT MAX(AirlineID)-MIN(AirlineID) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(AirlineID), MAX(AirlineID), MINMAXRANGE(AirlineID) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(AirlineID) FROM " + tableName;
    sqlQuery2 = "SELECT MAX(AirlineID) FROM " + tableName;
    sqlQuery3 = "SELECT MAX(AirlineID)-MIN(AirlineID) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(AirlineID), COUNT(*) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(AirlineID) FROM " + tableName;
    sqlQuery2 = "SELECT COUNT(*) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // string
    // TODO: add test cases for string column when we add support for min and max on string datatype columns

    // Non dictionary columns
    // int
    pqlQuery = "SELECT MAX(ActualElapsedTime) FROM " + tableName;
    sqlQuery = "SELECT MAX(ActualElapsedTime) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ActualElapsedTime) FROM " + tableName;
    sqlQuery = "SELECT MIN(ActualElapsedTime) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ActualElapsedTime) FROM " + tableName;
    sqlQuery = "SELECT MAX(ActualElapsedTime)-MIN(ActualElapsedTime) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery =
        "SELECT MIN(ActualElapsedTime), MAX(ActualElapsedTime), MINMAXRANGE(ActualElapsedTime) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(ActualElapsedTime) FROM " + tableName;
    sqlQuery2 = "SELECT MAX(ActualElapsedTime) FROM " + tableName;
    sqlQuery3 = "SELECT MAX(ActualElapsedTime)-MIN(ActualElapsedTime) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ActualElapsedTime), COUNT(*) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(ActualElapsedTime) FROM " + tableName;
    sqlQuery2 = "SELECT COUNT(*) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // float
    pqlQuery = "SELECT MAX(ArrDelay) FROM " + tableName;
    sqlQuery = "SELECT MAX(ArrDelay) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelay) FROM " + tableName;
    sqlQuery = "SELECT MIN(ArrDelay) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(ArrDelay) FROM " + tableName;
    sqlQuery = "SELECT MAX(ArrDelay)-MIN(ArrDelay) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(ArrDelay), MAX(ArrDelay), MINMAXRANGE(ArrDelay) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(ArrDelay) FROM " + tableName;
    sqlQuery2 = "SELECT MAX(ArrDelay) FROM " + tableName;
    sqlQuery3 = "SELECT MAX(ArrDelay)-MIN(ArrDelay) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(ArrDelay), COUNT(*) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(ArrDelay) FROM " + tableName;
    sqlQuery2 = "SELECT COUNT(*) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // double
    pqlQuery = "SELECT MAX(DepDelay) FROM " + tableName;
    sqlQuery = "SELECT MAX(DepDelay) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelay) FROM " + tableName;
    sqlQuery = "SELECT MIN(DepDelay) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MINMAXRANGE(DepDelay) FROM " + tableName;
    sqlQuery = "SELECT MAX(DepDelay)-MIN(DepDelay) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));
    pqlQuery = "SELECT MIN(DepDelay), MAX(DepDelay), MINMAXRANGE(DepDelay) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(DepDelay) FROM " + tableName;
    sqlQuery2 = "SELECT MAX(DepDelay) FROM " + tableName;
    sqlQuery3 = "SELECT MAX(DepDelay)-MIN(DepDelay) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2, sqlQuery3));
    pqlQuery = "SELECT MIN(DepDelay), COUNT(*) FROM " + tableName;
    sqlQuery1 = "SELECT MIN(DepDelay) FROM " + tableName;
    sqlQuery2 = "SELECT COUNT(*) FROM " + tableName;
    testQuery(pqlQuery, Lists.newArrayList(sqlQuery1, sqlQuery2));

    // string
    // TODO: add test cases for string column when we add support for min and max on string datatype columns

    // Check execution stats
    JsonNode response;

    // Dictionary column: answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime) FROM " + tableName;
    response = postQuery(pqlQuery);
    assertEquals(response.get("numEntriesScannedPostFilter").asLong(), 0);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertEquals(response.get("totalDocs").asLong(), response.get("numDocsScanned").asLong());

    // Non dictionary column: not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(DepDelay) FROM " + tableName;
    response = postQuery(pqlQuery);
    assertEquals(response.get("numEntriesScannedPostFilter").asLong(), response.get("numDocsScanned").asLong());
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertEquals(response.get("totalDocs").asLong(), response.get("numDocsScanned").asLong());

    // multiple dictionary based aggregation functions, dictionary columns: answered by
    // DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime),MIN(ArrTime) FROM " + tableName;
    response = postQuery(pqlQuery);
    assertEquals(response.get("numEntriesScannedPostFilter").asLong(), 0);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertEquals(response.get("totalDocs").asLong(), response.get("numDocsScanned").asLong());

    // multiple aggregation functions, mix of dictionary based and non dictionary based: not answered by
    // DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime),COUNT(ArrTime) FROM " + tableName;
    response = postQuery(pqlQuery);
    assertEquals(response.get("numEntriesScannedPostFilter").asLong(), response.get("numDocsScanned").asLong());
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertEquals(response.get("totalDocs").asLong(), response.get("numDocsScanned").asLong());

    // group by in query : not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime) FROM " + tableName + "  group by DaysSinceEpoch";
    response = postQuery(pqlQuery);
    assertTrue(response.get("numEntriesScannedPostFilter").asLong() > 0);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertEquals(response.get("totalDocs").asLong(), response.get("numDocsScanned").asLong());

    // filter in query: not answered by DictionaryBasedAggregationOperator
    pqlQuery = "SELECT MAX(ArrTime) FROM " + tableName + " where DaysSinceEpoch > 16100";
    response = postQuery(pqlQuery);
    assertTrue(response.get("numEntriesScannedPostFilter").asLong() > 0);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
  }

  @Test
  public void testMetadataBasedQueries()
      throws Exception {
    String tableName = getTableName();
    String pqlQuery;
    String sqlQuery;

    // Test queries with count *
    pqlQuery = "SELECT COUNT(*) FROM " + tableName;
    sqlQuery = "SELECT COUNT(*) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));

    // Test queries with max on time column
    pqlQuery = "SELECT MAX(DaysSinceEpoch) FROM " + tableName;
    sqlQuery = "SELECT MAX(DaysSinceEpoch) FROM " + tableName;
    testQuery(pqlQuery, Collections.singletonList(sqlQuery));

    // Check execution stats
    JsonNode response;

    pqlQuery = "SELECT COUNT(*) FROM " + tableName;
    response = postQuery(pqlQuery);
    assertEquals(response.get("numEntriesScannedPostFilter").asLong(), 0);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertEquals(response.get("totalDocs").asLong(), response.get("numDocsScanned").asLong());

    // group by present in query: not answered by MetadataBasedAggregationOperator
    pqlQuery = "SELECT COUNT(*) FROM " + tableName + " GROUP BY DaysSinceEpoch";
    response = postQuery(pqlQuery);
    assertTrue(response.get("numEntriesScannedPostFilter").asLong() > 0);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertEquals(response.get("totalDocs").asLong(), response.get("numDocsScanned").asLong());

    // filter present in query: not answered by MetadataBasedAggregationOperator
    pqlQuery = "SELECT COUNT(*) FROM " + tableName + " WHERE DaysSinceEpoch > 16100";
    response = postQuery(pqlQuery);
    assertEquals(response.get("numEntriesScannedPostFilter").asLong(), 0);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);

    // mixed aggregation functions in query: not answered by MetadataBasedAggregationOperator
    pqlQuery = "SELECT COUNT(*),MAX(ArrTime) FROM " + tableName;
    response = postQuery(pqlQuery);
    assertTrue(response.get("numEntriesScannedPostFilter").asLong() > 0);
    assertEquals(response.get("numEntriesScannedInFilter").asLong(), 0);
    assertEquals(response.get("totalDocs").asLong(), response.get("numDocsScanned").asLong());
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(getTableName());

    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
