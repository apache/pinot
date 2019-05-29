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
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroKeyComparator;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.avro.AvroRecord;
import org.apache.hadoop.mapreduce.v2.MiniMRYarnCluster;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.partition.PartitionFunction;
import org.apache.pinot.core.data.partition.PartitionFunctionFactory;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.hadoop.job.JobConfigConstants;
import org.apache.pinot.hadoop.job.SegmentPreprocessingJob;
import org.apache.pinot.hadoop.job.SegmentCreationJob;
import org.apache.pinot.hadoop.job.SegmentTarPushJob;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.hadoop.job.JobConfigConstants.*;


public class HadoopSegmentBuildPushOfflineClusterIntegrationTest extends BaseClusterIntegrationTestSet {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopSegmentBuildPushOfflineClusterIntegrationTest.class);
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
    _mrCluster = new MiniMRYarnCluster(getClass().getName(), 2);
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
        getLoadMode(), SegmentVersion.v3, getInvertedIndexColumns(), getBloomFilterIndexColumns(), getTaskConfig(), getSegmentPartitionConfig(), getSortedColumn());

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

    Properties preComputeProperties = new Properties();
    preComputeProperties.putAll(properties);
    preComputeProperties.setProperty(ENABLE_PARTITIONING, Boolean.TRUE.toString());
    preComputeProperties.setProperty(ENABLE_SORTING, Boolean.TRUE.toString());

    preComputeProperties.setProperty(JobConfigConstants.PATH_TO_INPUT, _avroDir.getPath());
    preComputeProperties.setProperty(JobConfigConstants.PATH_TO_OUTPUT, _preprocessingDir.getPath());
    properties.setProperty(JobConfigConstants.PATH_TO_INPUT, _preprocessingDir.getPath());

    // Run segment pre-processing job
    SegmentPreprocessingJob segmentPreprocessingJob = new SegmentPreprocessingJob(preComputeProperties);
    Configuration preComputeConfig = _mrCluster.getConfig();
    segmentPreprocessingJob.setConf(preComputeConfig);
    segmentPreprocessingJob.run();
    LOGGER.info("Segment preprocessing job finished.");

    // Verify partitioning and sorting.
    verifyPreprocessingJob(preComputeConfig);

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

  private void verifyPreprocessingJob(Configuration preComputeConfig) throws IOException {
    // Fetch partitioning config and sorting config.
    SegmentPartitionConfig segmentPartitionConfig = getSegmentPartitionConfig();
    Map.Entry<String, ColumnPartitionConfig>
        entry = segmentPartitionConfig.getColumnPartitionMap().entrySet().iterator().next();
    String partitionColumn = entry.getKey();
    String partitionFunctionString = entry.getValue().getFunctionName();
    int numPartitions = entry.getValue().getNumPartitions();
    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(partitionFunctionString, numPartitions);
    String sortedColumn = getSortedColumn();

    // Get output files.
    FileSystem fileSystem = FileSystem.get(preComputeConfig);
    FileStatus[] fileStatuses = fileSystem.listStatus(new Path(_preprocessingDir.getPath()));
    Assert.assertEquals(fileStatuses.length, numPartitions, "Number of output file should be the same as the number of partitions.");

    Set<Integer> partitionIdSet = new HashSet<>();
    Object previousObject;
    for (FileStatus fileStatus : fileStatuses) {
      Path avroFile = fileStatus.getPath();
      DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(fileSystem.open(avroFile), new GenericDatumReader<>());

      // Reset hash set and previous object
      partitionIdSet.clear();
      previousObject = null;
      while (dataFileStream.hasNext()) {
        GenericRecord genericRecord = dataFileStream.next();
        partitionIdSet.add(partitionFunction.getPartition(genericRecord.get(partitionColumn)));
        Assert.assertEquals(partitionIdSet.size(), 1, "Partition Id should be the same within a file.");
        org.apache.avro.Schema.Type type = genericRecord.getSchema().getField(sortedColumn).schema().getType();
        if (type.equals(org.apache.avro.Schema.Type.UNION)) {
          type = genericRecord.getSchema().getField(sortedColumn).schema().getTypes().get(0).getType();
        }
        Object currentObject = genericRecord.get(sortedColumn);
        if (previousObject == null) {
          previousObject = currentObject;
          continue;
        }
        // The values of sorted column should be sorted in ascending order.
        switch (type) {
          case STRING:
            Utf8 u1 = previousObject instanceof Utf8 ? (Utf8)previousObject : new Utf8(previousObject.toString());
            Utf8 u2 = currentObject instanceof Utf8 ? (Utf8)currentObject : new Utf8(currentObject.toString());
            Assert.assertTrue(u1.compareTo(u2) <= 0);
            break;
          case INT:
            Assert.assertTrue(((Integer)previousObject).compareTo(((Integer)currentObject)) <= 0);
            break;
          case LONG:
            Assert.assertTrue(((Long)previousObject).compareTo(((Long)currentObject)) <= 0);
            break;
          case BOOLEAN:
            Assert.assertTrue(((Boolean)previousObject).compareTo(((Boolean)currentObject)) <= 0);
            break;
          default:
            break;
        }
        previousObject = currentObject;
      }
    }
  }
}
