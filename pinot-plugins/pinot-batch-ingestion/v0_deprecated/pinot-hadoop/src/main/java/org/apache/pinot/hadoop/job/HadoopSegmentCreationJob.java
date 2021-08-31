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

import com.google.common.base.Preconditions;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.hadoop.job.mappers.SegmentCreationMapper;
import org.apache.pinot.hadoop.utils.PinotHadoopJobPreparationHelper;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentCreationJob;
import org.apache.pinot.ingestion.utils.JobPreparationHelper;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


public class HadoopSegmentCreationJob extends SegmentCreationJob {
  // Output Directory FileSystem
  protected FileSystem _outputDirFileSystem;

  public HadoopSegmentCreationJob(Properties properties) {
    super(properties);
    getConf().set("mapreduce.job.user.classpath.first", "true");
  }

  public void run()
      throws Exception {
    _logger.info("Starting {}", getClass().getSimpleName());

    // Initialize all directories
    _outputDirFileSystem = FileSystem.get(new Path(_outputDir).toUri(), getConf());
    JobPreparationHelper.mkdirs(_outputDirFileSystem, new Path(_outputDir), _defaultPermissionsMask);
    JobPreparationHelper.mkdirs(_outputDirFileSystem, new Path(_stagingDir), _defaultPermissionsMask);
    Path stagingInputDir = new Path(_stagingDir, "input");
    JobPreparationHelper.mkdirs(_outputDirFileSystem, stagingInputDir, _defaultPermissionsMask);

    // Gather all data files
    List<Path> dataFilePaths = getDataFilePaths(_inputPattern);
    int numDataFiles = dataFilePaths.size();
    if (numDataFiles == 0) {
      String errorMessage = "No data file founded with pattern: " + _inputPattern;
      _logger.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      _logger.info("Creating segments with data files: {}", dataFilePaths);
      for (int i = 0; i < numDataFiles; i++) {
        Path dataFilePath = dataFilePaths.get(i);
        try (DataOutputStream dataOutputStream = _outputDirFileSystem
            .create(new Path(stagingInputDir, Integer.toString(i)))) {
          dataOutputStream.write(StringUtil.encodeUtf8(dataFilePath.toString() + " " + i));
          dataOutputStream.flush();
        }
      }
    }

    // Set up the job
    Job job = Job.getInstance(getConf());
    job.setJarByClass(getClass());
    job.setJobName(getClass().getName());

    Configuration jobConf = job.getConfiguration();
    String hadoopTokenFileLocation = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
    if (hadoopTokenFileLocation != null) {
      jobConf.set("mapreduce.job.credentials.binary", hadoopTokenFileLocation);
    }
    jobConf.setInt(JobContext.NUM_MAPS, numDataFiles);

    // Set table config and schema
    TableConfig tableConfig = getTableConfig();
    if (tableConfig != null) {
      validateTableConfig(tableConfig);
      jobConf.set(JobConfigConstants.TABLE_CONFIG, tableConfig.toJsonString());
    }
    jobConf.set(JobConfigConstants.SCHEMA, getSchema().toSingleLineJsonString());

    // Set additional configurations
    for (Map.Entry<Object, Object> entry : _properties.entrySet()) {
      jobConf.set(entry.getKey().toString(), entry.getValue().toString());
    }

    job.setMapperClass(getMapperClass());
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, stagingInputDir);
    FileOutputFormat.setOutputPath(job, new Path(_stagingDir, "output"));

    addDepsJarToDistributedCache(job);
    addAdditionalJobProperties(job);

    // Submit the job
    job.waitForCompletion(true);
    if (!job.isSuccessful()) {
      throw new RuntimeException("Job failed: " + job);
    }

    moveSegmentsToOutputDir();

    cleanup(job);
  }

  protected void validateTableConfig(TableConfig tableConfig) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();

    // For APPEND use case, timeColumnName and timeType must be set
    if (APPEND.equalsIgnoreCase(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig))) {
      Preconditions.checkState(validationConfig.getTimeColumnName() != null && validationConfig.getTimeType() != null,
          "For APPEND use case, time column and type must be set");
    }
  }

  /**
   * Can be overridden to plug in custom mapper.
   */
  protected Class<? extends Mapper<LongWritable, Text, LongWritable, Text>> getMapperClass() {
    return SegmentCreationMapper.class;
  }

  protected void addDepsJarToDistributedCache(Job job)
      throws IOException {
    if (_depsJarDir != null) {
      PinotHadoopJobPreparationHelper
          .addDepsJarToDistributedCacheHelper(FileSystem.get(new Path(_depsJarDir).toUri(), getConf()), job,
              new Path(_depsJarDir));
    }
  }

  /**
   * Can be overridden to set additional job properties.
   */
  @SuppressWarnings("unused")
  protected void addAdditionalJobProperties(Job job) {
  }

  protected void moveSegmentsToOutputDir()
      throws IOException {
    Path segmentTarDir = new Path(new Path(_stagingDir, "output"), JobConfigConstants.SEGMENT_TAR_DIR);
    movePath(_outputDirFileSystem, segmentTarDir.toString(), _outputDir, true);
  }

  /**
   * Cleans up after the job completes.
   */
  protected void cleanup(Job job)
      throws Exception {
    // Delete the staging directory
    _logger.info("Deleting the staging directory: {}", _stagingDir);
    _outputDirFileSystem.delete(new Path(_stagingDir), true);
  }
}
