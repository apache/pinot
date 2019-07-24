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
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.hadoop.job.JobConfigConstants;
import org.apache.pinot.hadoop.job.SegmentCreationJob;
import org.apache.pinot.hadoop.job.SegmentTarPushJob;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class HadoopSegmentBuildPushOfflineClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;

  private MiniMRYarnCluster _mrCluster;
  private Schema _schema;

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

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(getNumBrokers());
    startServers(getNumServers());

    // Start the MR Yarn cluster
    final Configuration conf = new Configuration();
    _mrCluster = new MiniMRYarnCluster(getClass().getName(), 1);
    _mrCluster.init(conf);
    _mrCluster.start();

    // Load Schema
    _schema = Schema.fromFile(getSchemaFile());

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_avroDir);

    ExecutorService executor = Executors.newCachedThreadPool();

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
    addOfflineTable(getTableName(), _schema.getTimeColumnName(), _schema.getOutgoingTimeUnit().toString(), null, null,
        getLoadMode(), SegmentVersion.v3, getInvertedIndexColumns(), getBloomFilterIndexColumns(), getTaskConfig(), null, null);

    // Generate and push Pinot segments from Hadoop
    generateAndPushSegmentsFromHadoop();

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
    _mrCluster.stop();
    FileUtils.deleteDirectory(_mrCluster.getTestWorkDir());
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  @Override
  public void testQueriesFromQueryFile()
      throws Exception {
    super.testQueriesFromQueryFile();
  }

  @Test
  @Override
  public void testSqlQueriesFromQueryFile()
      throws Exception {
    super.testQueriesFromQueryFile();
  }

  @Test
  @Override
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    super.testGeneratedQueriesWithMultiValues();
  }

  @Test
  @Override
  public void testQueryExceptions()
      throws Exception {
    super.testQueryExceptions();
  }

  @Test
  @Override
  public void testInstanceShutdown()
      throws Exception {
    super.testInstanceShutdown();
  }

  private void generateAndPushSegmentsFromHadoop()
      throws Exception {
    // Configure Hadoop segment generate and push job
    Properties properties = new Properties();
    properties.setProperty(JobConfigConstants.PATH_TO_INPUT, _avroDir.getPath());
    properties.setProperty(JobConfigConstants.PATH_TO_OUTPUT, _segmentDir.getPath());
    properties.setProperty(JobConfigConstants.SEGMENT_TABLE_NAME, getTableName());

    // Setting this will fetch the schema & table config from the controller
    properties.setProperty(JobConfigConstants.PUSH_TO_HOSTS, getDefaultControllerConfiguration().getControllerHost());
    properties.setProperty(JobConfigConstants.PUSH_TO_PORT, getDefaultControllerConfiguration().getControllerPort());

    // Run segment creation job
    SegmentCreationJob creationJob = new SegmentCreationJob(properties);
    Configuration config = _mrCluster.getConfig();
    creationJob.setConf(config);
    creationJob.run();

    // Run segment push job
    SegmentTarPushJob pushJob = new SegmentTarPushJob(properties);
    pushJob.setConf(_mrCluster.getConfig());
    pushJob.run();
  }
}
