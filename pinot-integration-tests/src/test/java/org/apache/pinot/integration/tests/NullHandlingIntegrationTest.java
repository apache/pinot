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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


/**
 * Integration test that creates a Kafka broker, creates a Pinot cluster that consumes from Kafka and queries Pinot.
 * The data pushed to Kafka includes null values.
 */
public class NullHandlingIntegrationTest extends BaseClusterIntegrationTestSet {

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Start Kafka
    startKafka();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config
    addSchema(createSchema());
    addTableConfig(createRealtimeTableConfig(avroFiles.get(0)));

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(10_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());

    // Stop the Pinot cluster
    stopServer();
    stopBroker();
    stopController();
    // Stop Kafka
    stopKafka();
    // Stop Zookeeper
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Override
  protected String getAvroTarFileName() {
    return "avro_data_with_nulls.tar.gz";
  }

  @Override
  protected String getSchemaFileName() {
    return "test_null_handling.schema";
  }

  @Override
  @Nullable
  protected String getSortedColumn() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @Override
  protected boolean getNullHandlingEnabled() {
    return true;
  }

  @Override
  protected long getCountStarResult() {
    return 100;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTotalCount(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT COUNT(*) FROM " + getTableName();
    testQuery(query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCountWithNullDescription(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE description IS NOT NULL";
    testQuery(query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCountWithNullDescriptionAndSalary(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT COUNT(*) FROM " + getTableName() + " WHERE description IS NOT NULL AND salary IS NOT NULL";
    testQuery(query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseWithNullSalary(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT CASE WHEN salary IS NULL THEN 1 ELSE 0 END FROM " + getTableName();
    testQuery(query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseWithNotNullDescription(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END FROM " + getTableName();
    testQuery(query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseWithIsDistinctFrom(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT salary IS DISTINCT FROM salary FROM " + getTableName();
    testQuery(query);
    query = "SELECT salary FROM " + getTableName() + " where salary IS DISTINCT FROM salary";
    testQuery(query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseWithIsNotDistinctFrom(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = "SELECT description IS NOT DISTINCT FROM description FROM " + getTableName();
    testQuery(query);
    query = "SELECT description FROM " + getTableName() + " where description IS NOT DISTINCT FROM description";
    testQuery(query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTotalCountWithNullHandlingQueryOptionEnabled(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String pinotQuery = "SELECT COUNT(*) FROM " + getTableName() + " option(enableNullHandling=true)";
    String h2Query = "SELECT COUNT(*) FROM " + getTableName();
    testQuery(pinotQuery, h2Query);

    pinotQuery = "SELECT COUNT(1) FROM " + getTableName() + " option(enableNullHandling=true)";
    h2Query = "SELECT COUNT(1) FROM " + getTableName();
    testQuery(pinotQuery, h2Query);
    DataTableBuilderFactory.setDataTableVersion(DataTableBuilderFactory.DEFAULT_VERSION);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testNullLiteralSelectionOnlyBroker(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Null literal only
    String sqlQuery = "SELECT null FROM mytable OPTION(enableNullHandling=true)";
    JsonNode response = postQuery(sqlQuery);
    JsonNode rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asText(), "null");

    // Null related functions
    sqlQuery = "SELECT isNull(null) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asBoolean(), true);

    sqlQuery = "SELECT isNotNull(null) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asBoolean(), false);


    sqlQuery = "SELECT coalesce(null, 1) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asInt(), 1);

    sqlQuery = "SELECT coalesce(null, null) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asText(), "null");

    sqlQuery = "SELECT isDistinctFrom(null, null) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asBoolean(), false);

    sqlQuery = "SELECT isNotDistinctFrom(null, null) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asBoolean(), true);


    sqlQuery = "SELECT isDistinctFrom(null, 1) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asBoolean(), true);

    sqlQuery = "SELECT isNotDistinctFrom(null, 1) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asBoolean(), false);

    sqlQuery = "SELECT case when true then null end FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asText(), "null");


    sqlQuery = "SELECT case when false then 1 end FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asText(), "null");


    // Null intolerant functions
    sqlQuery = "SELECT add(null, 1) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asText(), "null");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testOrderByNullsFirst(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String h2Query = "SELECT salary FROM " + getTableName() + " ORDER BY salary NULLS FIRST";
    String pinotQuery = h2Query + " option(enableNullHandling=true)";

    testQuery(pinotQuery, h2Query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testOrderByNullsLast(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String h2Query = "SELECT salary FROM " + getTableName() + " ORDER BY salary DESC NULLS LAST";
    String pinotQuery = h2Query + " option(enableNullHandling=true)";

    testQuery(pinotQuery, h2Query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctOrderByNullsLast(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String h2Query = "SELECT distinct salary FROM " + getTableName() + " ORDER BY salary DESC NULLS LAST";
    String pinotQuery = h2Query + " option(enableNullHandling=true)";

    testQuery(pinotQuery, h2Query);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectNullLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Need to also select an identifier column to skip the all literal query optimization which returns without
    // querying the segment.
    String sqlQuery = "SELECT NULL, salary FROM mytable OPTION(enableNullHandling=true)";

    JsonNode response = postQuery(sqlQuery);

    JsonNode rows = response.get("resultTable").get("rows");
    assertEquals(rows.get(0).get(0).asText(), "null");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCaseWhenAllLiteral(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String sqlQuery =
        "SELECT CASE WHEN true THEN 1 WHEN NOT true THEN 0 ELSE NULL END FROM mytable OPTION(enableNullHandling=true)";

    JsonNode response = postQuery(sqlQuery);

    assertEquals(response.get("resultTable").get("rows").get(0).get(0).asInt(), 1);
  }
}
