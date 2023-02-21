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
import org.apache.pinot.common.datatable.DataTableFactory;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.util.TestUtils;
import org.roaringbitmap.RoaringBitmap;
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

    // Setting data table version to 4
    DataTableBuilderFactory.setDataTableVersion(DataTableFactory.VERSION_4);
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

  @Test
  public void testIsNull()
      throws Exception {
    String sqlQuery = "SELECT null FROM mytable OPTION(enableNullHandling=true)";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
    JsonNode rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asText(), "null");

    sqlQuery = "SELECT isNull(null) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asBoolean(), true);

    sqlQuery = "SELECT isNotNull(null) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0).get(0).asBoolean(), false);

    sqlQuery = "SELECT isNull(add(null, salary)) FROM " + getTableName() + "  OPTION (enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    assertTrue(response.get("exceptions").isEmpty());
    assertEquals(rows.size(), 10);
    for (int i = 0; i < 10; i++) {
      assertEquals(rows.get(i).get(0).asBoolean(), true);
    }
    RoaringBitmap nullSalary = new RoaringBitmap();
    sqlQuery = "SELECT salary FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (rows.get(i).get(0).asText().equals("null")) {
        nullSalary.add(i);
      }
    }
    sqlQuery = "SELECT isNull(salary) FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asBoolean(), true);
      } else {
        assertEquals(rows.get(i).get(0).asBoolean(), false);
      }
    }

    sqlQuery = "SELECT isNotNull(salary) FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asBoolean(), false);
      } else {
        assertEquals(rows.get(i).get(0).asBoolean(), true);
      }
    }

    sqlQuery = "SELECT CASE WHEN salary IS NULL THEN null ELSE 1 END FROM " + getTableName()
        + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asText(), "null");
      } else {
        assertEquals(rows.get(i).get(0).asInt(), 1);
      }
    }
    sqlQuery = "SELECT CASE WHEN salary IS NOT NULL THEN null ELSE 6 END FROM " + getTableName()
        + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asInt(), 6);
      } else {
        assertEquals(rows.get(i).get(0).asText(), "null");
      }
    }
  }

  @Test
  public void testCoalesce()
      throws Exception {
    RoaringBitmap nullSalary = new RoaringBitmap();
    String sqlQuery = "SELECT salary FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (rows.get(i).get(0).asText().equals("null")) {
        nullSalary.add(i);
      }
    }
    sqlQuery = "SELECT COALESCE(salary, null) FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asText(), "null");
      } else {
        assertTrue(!rows.get(i).get(0).asText().equals("null"));
      }
    }

    sqlQuery = "SELECT salary IS DISTINCT FROM salary FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      assertEquals(rows.get(i).get(0).asBoolean(), false);
    }

    sqlQuery = "SELECT salary IS DISTINCT FROM null FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asBoolean(), false);
      } else {
        assertEquals(rows.get(i).get(0).asBoolean(), true);
      }
    }

    sqlQuery = "SELECT salary IS NOT DISTINCT FROM null FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asBoolean(), true);
      } else {
        assertEquals(rows.get(i).get(0).asBoolean(), false);
      }
    }
  }

  @Test
  public void testNullIntolerant()
      throws Exception {
    RoaringBitmap nullSalary = new RoaringBitmap();
    String sqlQuery = "SELECT salary FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    JsonNode response = postQuery(sqlQuery, _brokerBaseApiUrl);
    JsonNode rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (rows.get(i).get(0).asText().equals("null")) {
        nullSalary.add(i);
      }
    }
    // AdditionTransformFunction
    sqlQuery = "SELECT add(salary, null) FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asText(), "null");
      }
    }
    sqlQuery = "SELECT add(salary, '1.23456789101112') FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asText(), "null");
      } else {
        assertTrue(!rows.get(i).get(0).asText().equals("null"));
      }
    }
    // Cast
    sqlQuery = "SELECT cast(salary AS INT) FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    for (int i = 0; i < 10; i++) {
      if (nullSalary.contains(i)) {
        assertEquals(rows.get(i).get(0).asText(), "null");
      } else {
        assertTrue(!rows.get(i).get(0).asText().equals("null"));
      }
    }

    sqlQuery = "SELECT cast(null AS INT) FROM " + getTableName() + " OPTION(enableNullHandling=true);";
    response = postQuery(sqlQuery, _brokerBaseApiUrl);
    rows = response.get("resultTable").get("rows");
    assertEquals(rows.get(0).get(0).asText(), "null");
  }
}
