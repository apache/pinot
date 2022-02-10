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
package org.apache.pinot.hadoop.job;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pinot.hadoop.job.preprocess.HadoopDataPreprocessingHelper;
import org.apache.pinot.hadoop.job.preprocess.HadoopDataPreprocessingHelperFactory;
import org.apache.pinot.hadoop.utils.PinotHadoopJobPreparationHelper;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentPreprocessingJob;
import org.apache.pinot.ingestion.utils.preprocess.HadoopUtils;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Hadoop job which provides partitioning, sorting, and resizing against the input files, which is raw data in
 * either Avro or Orc format.
 * Thus, the output files are partitioned, sorted, resized after this job.
 * In order to run this job, the following configs need to be specified in job properties:
 * * enable.preprocessing: false by default. Enables preprocessing job.
 */
public class HadoopSegmentPreprocessingJob extends SegmentPreprocessingJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopSegmentPreprocessingJob.class);

  public HadoopSegmentPreprocessingJob(final Properties properties) {
    super(properties);
  }

  public void run()
      throws Exception {
    if (!_enablePreprocessing) {
      LOGGER.info("Pre-processing job is disabled.");
      return;
    } else {
      LOGGER.info("Starting {}", getClass().getSimpleName());
    }

    setTableConfigAndSchema();
    fetchPreProcessingOperations();
    fetchPartitioningConfig();
    fetchSortingConfig();
    fetchResizingConfig();

    // Cleans up preprocessed output dir if exists
    cleanUpPreprocessedOutputs(_preprocessedOutputDir);

    HadoopDataPreprocessingHelper dataPreprocessingHelper =
        HadoopDataPreprocessingHelperFactory.generateDataPreprocessingHelper(_inputSegmentDir, _preprocessedOutputDir);
    dataPreprocessingHelper
        .registerConfigs(_tableConfig, _pinotTableSchema, _partitionColumn, _numPartitions, _partitionFunction,
            _partitionColumnDefaultNullValue, _sortingColumn, _sortingColumnType, _sortingColumnDefaultNullValue,
            _numOutputFiles, _maxNumRecordsPerFile);

    Job job = dataPreprocessingHelper.setUpJob();

    // Since we aren't extending AbstractHadoopJob, we need to add the jars for the job to
    // distributed cache ourselves. Take a look at how the addFilesToDistributedCache is
    // implemented so that you know what it does.
    LOGGER.info("HDFS class path: " + _pathToDependencyJar);
    if (_pathToDependencyJar != null) {
      LOGGER.info("Copying jars locally.");
      PinotHadoopJobPreparationHelper
          .addDepsJarToDistributedCacheHelper(HadoopUtils.DEFAULT_FILE_SYSTEM, job, _pathToDependencyJar);
    } else {
      LOGGER.info("Property '{}' not specified.", JobConfigConstants.PATH_TO_DEPS_JAR);
    }

    long startTime = System.currentTimeMillis();
    // Submit the job for execution.
    job.waitForCompletion(true);
    if (!job.isSuccessful()) {
      throw new RuntimeException("Job failed : " + job);
    }

    LOGGER.info("Finished pre-processing job in {}ms", (System.currentTimeMillis() - startTime));
  }

  @Override
  protected Schema getSchema()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      if (controllerRestApi != null) {
        return controllerRestApi.getSchema();
      } else {
        try (InputStream inputStream = FileSystem.get(_schemaFile.toUri(), getConf()).open(_schemaFile)) {
          return org.apache.pinot.spi.data.Schema.fromInputSteam(inputStream);
        }
      }
    }
  }

  /**
   * Can be overridden to set additional job properties.
   */
  @SuppressWarnings("unused")
  protected void addAdditionalJobProperties(Job job) {
  }

  /**
   * Cleans up outputs in preprocessed output directory.
   */
  public static void cleanUpPreprocessedOutputs(Path preprocessedOutputDir)
      throws IOException {
    if (HadoopUtils.DEFAULT_FILE_SYSTEM.exists(preprocessedOutputDir)) {
      LOGGER.warn("Found output folder {}, deleting", preprocessedOutputDir);
      HadoopUtils.DEFAULT_FILE_SYSTEM.delete(preprocessedOutputDir, true);
    }
  }
}
