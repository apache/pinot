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
package org.apache.pinot.hadoop.job.preprocess;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pinot.hadoop.job.HadoopSegmentPreprocessingJob;
import org.apache.pinot.ingestion.preprocess.DataPreprocessingHelper;
import org.apache.pinot.ingestion.preprocess.partitioners.GenericPartitioner;
import org.apache.pinot.ingestion.utils.InternalConfigConstants;
import org.apache.pinot.ingestion.utils.preprocess.HadoopUtils;
import org.apache.pinot.ingestion.utils.preprocess.TextComparator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class HadoopDataPreprocessingHelper implements HadoopJobPreparer {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopDataPreprocessingHelper.class);

  protected DataPreprocessingHelper _dataPreprocessingHelper;

  public HadoopDataPreprocessingHelper(DataPreprocessingHelper dataPreprocessingHelper) {
    _dataPreprocessingHelper = dataPreprocessingHelper;
  }

  public Job setUpJob()
      throws IOException {
    LOGGER.info("Initializing a pre-processing job");
    Job job = Job.getInstance(HadoopUtils.DEFAULT_CONFIGURATION);
    Configuration jobConf = job.getConfiguration();
    // Input and output paths.
    int numInputPaths = _dataPreprocessingHelper._inputDataPaths.size();
    jobConf.setInt(JobContext.NUM_MAPS, numInputPaths);
    _dataPreprocessingHelper.setValidationConfigs(job, _dataPreprocessingHelper._sampleRawDataPath);
    for (Path inputFile : _dataPreprocessingHelper._inputDataPaths) {
      FileInputFormat.addInputPath(job, inputFile);
    }
    setHadoopJobConfigs(job);

    // Sorting column
    if (_dataPreprocessingHelper._sortingColumn != null) {
      LOGGER.info("Adding sorting column: {} to job config", _dataPreprocessingHelper._sortingColumn);
      jobConf.set(InternalConfigConstants.SORTING_COLUMN_CONFIG, _dataPreprocessingHelper._sortingColumn);
      jobConf.set(InternalConfigConstants.SORTING_COLUMN_TYPE, _dataPreprocessingHelper._sortingColumnType.name());
      jobConf.set(InternalConfigConstants.SORTING_COLUMN_DEFAULT_NULL_VALUE,
          _dataPreprocessingHelper._sortingColumnDefaultNullValue);

      switch (_dataPreprocessingHelper._sortingColumnType) {
        case INT:
          job.setMapOutputKeyClass(IntWritable.class);
          break;
        case LONG:
          job.setMapOutputKeyClass(LongWritable.class);
          break;
        case FLOAT:
          job.setMapOutputKeyClass(FloatWritable.class);
          break;
        case DOUBLE:
          job.setMapOutputKeyClass(DoubleWritable.class);
          break;
        case STRING:
          job.setMapOutputKeyClass(Text.class);
          job.setSortComparatorClass(TextComparator.class);
          break;
        default:
          throw new IllegalStateException();
      }
    } else {
      job.setMapOutputKeyClass(NullWritable.class);
    }

    // Partition column
    int numReduceTasks = 0;
    if (_dataPreprocessingHelper._partitionColumn != null) {
      numReduceTasks = _dataPreprocessingHelper._numPartitions;
      jobConf.set(InternalConfigConstants.ENABLE_PARTITIONING, "true");
      job.setPartitionerClass(GenericPartitioner.class);
      jobConf.set(InternalConfigConstants.PARTITION_COLUMN_CONFIG, _dataPreprocessingHelper._partitionColumn);
      if (_dataPreprocessingHelper._partitionFunction != null) {
        jobConf.set(InternalConfigConstants.PARTITION_FUNCTION_CONFIG, _dataPreprocessingHelper._partitionFunction);
      }
      jobConf.set(InternalConfigConstants.PARTITION_COLUMN_DEFAULT_NULL_VALUE,
          _dataPreprocessingHelper._partitionColumnDefaultNullValue);
      jobConf.setInt(InternalConfigConstants.NUM_PARTITIONS_CONFIG, numReduceTasks);
    } else {
      if (_dataPreprocessingHelper._numOutputFiles > 0) {
        numReduceTasks = _dataPreprocessingHelper._numOutputFiles;
      } else {
        // default number of input paths
        numReduceTasks = _dataPreprocessingHelper._inputDataPaths.size();
      }
    }
    job.setPartitionerClass(_dataPreprocessingHelper.getPartitioner());
    // Maximum number of records per output file
    jobConf.set(InternalConfigConstants.PREPROCESSING_MAX_NUM_RECORDS_PER_FILE,
        Integer.toString(_dataPreprocessingHelper._maxNumRecordsPerFile));
    // Number of reducers
    LOGGER.info("Number of reduce tasks for pre-processing job: {}", numReduceTasks);
    job.setNumReduceTasks(numReduceTasks);

    setUpMapperReducerConfigs(job);

    return job;
  }

  private void setHadoopJobConfigs(Job job) {
    job.setJarByClass(HadoopSegmentPreprocessingJob.class);
    job.setJobName(getClass().getName());
    FileOutputFormat.setOutputPath(job, _dataPreprocessingHelper._outputPath);
    job.getConfiguration().set(JobContext.JOB_NAME, this.getClass().getName());
    // Turn this on to always firstly use class paths that user specifies.
    job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, "true");
    // Turn this off since we don't need an empty file in the output directory
    job.getConfiguration().set(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, "false");

    String hadoopTokenFileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (hadoopTokenFileLocation != null) {
      job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY, hadoopTokenFileLocation);
    }
  }

  public Object getSchema(Path inputPathDir)
      throws IOException {
    return _dataPreprocessingHelper.getSchema(inputPathDir);
  }

  public void validateConfigsAgainstSchema(Object schema) {
    _dataPreprocessingHelper.validateConfigsAgainstSchema(schema);
  }

  public void registerConfigs(TableConfig tableConfig, Schema tableSchema, String partitionColumn, int numPartitions,
      String partitionFunction, String partitionColumnDefaultNullValue, String sortingColumn,
      FieldSpec.DataType sortingColumnType, String sortingColumnDefaultNullValue, int numOutputFiles,
      int maxNumRecordsPerFile) {
    _dataPreprocessingHelper
        .registerConfigs(tableConfig, tableSchema, partitionColumn, numPartitions, partitionFunction,
            partitionColumnDefaultNullValue,
            sortingColumn, sortingColumnType, sortingColumnDefaultNullValue, numOutputFiles, maxNumRecordsPerFile);
  }
}
