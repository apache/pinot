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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.client.Connection;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.dataset.DatasetMetadata;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DeepStorageSharingIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final String BROKER_TENANT_NAME = "brokerTenant";

  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 6;

  protected int getNumBrokers() {
    return NUM_BROKERS;
  }

  protected int getNumServers() {
    return NUM_SERVERS;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _avroDir, _segmentDir, _tarDir);
    ControllerConf controllerConf = getDefaultControllerConfiguration();
    controllerConf.setTenantIsolationEnabled(false);

    // Start the Pinot cluster
    startZk();
    startController(controllerConf);
    startBrokers(getNumBrokers());
    startServers(getNumServers());

    createBrokerTenant(BROKER_TENANT_NAME, 1);
    createServerTenant("mytable", 2, 0);
    createServerTenant("mytable2", 2, 0);
    createServerTenant("mytable3", 2, 0);

    // Load Schema
    _schema = Schema.fromFile(getSchemaFile());

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_avroDir);

    ExecutorService executor = Executors.newCachedThreadPool();

    ClusterIntegrationTestUtils
        .buildSegmentsFromAvro(avroFiles, 0, _segmentDir, _tarDir, getTableName(), false, null, getRawIndexColumns(),
            null, executor);

    // Load data into H2
    setUpH2Connection(avroFiles, executor);

    // Initialize query generator
    setUpQueryGenerator(avroFiles, executor);

    // Shut down the executor
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Add Schema
    addSchema(getSchemaFile(), _schema.getSchemaName());

    // Create the table
    addOfflineTable(getTableName(), _schema.getTimeColumnName(), _schema.getOutgoingTimeUnit().toString(), "mytable",
        BROKER_TENANT_NAME, "mytable", getLoadMode(), SegmentVersion.v3, getInvertedIndexColumns(), getBloomFilterIndexColumns(),
        getTaskConfig());
    completeTableConfiguration();

    // Create another table pointing to mytable dataset
    addOfflineTable("mytable2", _schema.getTimeColumnName(), _schema.getOutgoingTimeUnit().toString(), "mytable",
        BROKER_TENANT_NAME, "mytable2", getLoadMode(), SegmentVersion.v3, getInvertedIndexColumns(), getBloomFilterIndexColumns(),
        getTaskConfig());
    completeTableConfiguration();

    // Upload all segments
    uploadSegments(_tarDir);

    // Wait for all documents loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void test()
      throws Exception {
    // Create a second query
    addOfflineTable("mytable3", _schema.getTimeColumnName(), _schema.getOutgoingTimeUnit().toString(), "mytable",
        BROKER_TENANT_NAME, "mytable3", getLoadMode(), SegmentVersion.v3, getInvertedIndexColumns(), getBloomFilterIndexColumns(),
        getTaskConfig());
    completeTableConfiguration();

//    Connection pinotConnection = getPinotConnection();
//    ResultSetGroup result1 = pinotConnection.execute("select count(*) from mytable");
//    System.out.println(result1);
//
//    ResultSetGroup result2 =  pinotConnection.execute("select count(*) form mytable2");
//    System.out.println(result2);
//
//    ResultSetGroup result3 =  pinotConnection.execute("select count(*) form mytable3");
//    System.out.println(result3);
    Thread.sleep(100000000000L);
  }

  public static void main(String[] args) {
  }
}
