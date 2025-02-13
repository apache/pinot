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
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class OOMProtectionEnabledIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_OFFLINE_SEGMENTS = 8;
  private static final int NUM_REALTIME_SEGMENTS = 6;

  @Override
  protected void overrideBrokerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configuration.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    configuration.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_INSTANCE_TYPE,
        InstanceType.BROKER.toString());
    configuration.setProperty(CommonConstants.Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration configuration) {
    configuration.setProperty(CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "."
        + CommonConstants.Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    configuration.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME,
        "org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory");
    configuration.setProperty(
        CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_INSTANCE_TYPE,
        InstanceType.SERVER.toString());
    configuration.setProperty(CommonConstants.Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start Zk, Kafka and Pinot
    startZk();
    startController();
    startBroker();
    startServer();
    startKafka();

    List<File> avroFiles = getAllAvroFiles();
    List<File> offlineAvroFiles = getOfflineAvroFiles(avroFiles, NUM_OFFLINE_SEGMENTS);
    List<File> realtimeAvroFiles = getRealtimeAvroFiles(avroFiles, NUM_REALTIME_SEGMENTS);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);
    addTableConfig(createRealtimeTableConfig(realtimeAvroFiles.get(0)));

    // Create and upload segments
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(offlineAvroFiles, offlineTableConfig, schema, 0, _segmentDir,
        _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Push data into Kafka
    pushAvroIntoKafka(realtimeAvroFiles);

    // Set up the H2 connection
    setUpH2Connection(avroFiles);

    // Initialize the query generator
    setUpQueryGenerator(avroFiles);

    // Wait for all documents loaded
    waitForAllDocsLoaded(100_000L);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testHardcodedQueries(boolean useMultiStageEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageEngine);
    super.testHardcodedQueries();
  }
}
