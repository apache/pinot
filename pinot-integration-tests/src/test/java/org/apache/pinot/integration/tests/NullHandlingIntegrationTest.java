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

import java.io.File;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


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

  @Test
  public void testTotalCount()
      throws Exception {
    String query = "SELECT count(*) FROM " + getTableName();
    testQuery(query, Collections.singletonList(query));
  }

  @Test
  public void testCountWithNullDescription()
          throws Exception {
    String query = "SELECT count(*) FROM " + getTableName() + " where description IS NOT NULL";
    testQuery(query, Collections.singletonList(query));
  }

  @Test
  public void testCountWithNullDescriptionAndSalary()
          throws Exception {
    String query = "SELECT count(*) FROM " + getTableName() + " where description IS NOT NULL AND salary IS NOT NULL";
    testQuery(query, Collections.singletonList(query));
  }
}
