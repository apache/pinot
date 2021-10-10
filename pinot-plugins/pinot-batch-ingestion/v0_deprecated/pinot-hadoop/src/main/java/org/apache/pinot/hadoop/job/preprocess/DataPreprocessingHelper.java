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
import java.util.List;
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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pinot.hadoop.job.HadoopSegmentPreprocessingJob;
import org.apache.pinot.hadoop.job.InternalConfigConstants;
import org.apache.pinot.hadoop.job.partitioners.GenericPartitioner;
import org.apache.pinot.hadoop.utils.preprocess.HadoopUtils;
import org.apache.pinot.hadoop.utils.preprocess.TextComparator;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class DataPreprocessingHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataPreprocessingHelper.class);

  String _partitionColumn;
  int _numPartitions;
  String _partitionFunction;

  String _sortingColumn;
  private FieldSpec.DataType _sortingColumnType;

  private int _numOutputFiles;
  private int _maxNumRecordsPerFile;

  private TableConfig _tableConfig;
  private Schema _pinotTableSchema;

  List<Path> _inputDataPaths;
  Path _sampleRawDataPath;
  Path _outputPath;

  public DataPreprocessingHelper(List<Path> inputDataPaths, Path outputPath) {
    _inputDataPaths = inputDataPaths;
    _sampleRawDataPath = inputDataPaths.get(0);
    _outputPath = outputPath;
  }

  public void registerConfigs(TableConfig tableConfig, Schema tableSchema, String partitionColumn, int numPartitions,
      String partitionFunction, String sortingColumn, FieldSpec.DataType sortingColumnType, int numOutputFiles,
      int maxNumRecordsPerFile) {
    _tableConfig = tableConfig;
    _pinotTableSchema = tableSchema;
    _partitionColumn = partitionColumn;
    _numPartitions = numPartitions;
    _partitionFunction = partitionFunction;

    _sortingColumn = sortingColumn;
    _sortingColumnType = sortingColumnType;

    _numOutputFiles = numOutputFiles;
    _maxNumRecordsPerFile = maxNumRecordsPerFile;
  }

  public Job setUpJob()
      throws IOException {
    LOGGER.info("Initializing a pre-processing job");
    Job job = Job.getInstance(HadoopUtils.DEFAULT_CONFIGURATION);
    Configuration jobConf = job.getConfiguration();
    // Input and output paths.
    int numInputPaths = _inputDataPaths.size();
    jobConf.setInt(JobContext.NUM_MAPS, numInputPaths);
    setValidationConfigs(job, _sampleRawDataPath);
    for (Path inputFile : _inputDataPaths) {
      FileInputFormat.addInputPath(job, inputFile);
    }
    setHadoopJobConfigs(job);

    // Sorting column
    if (_sortingColumn != null) {
      LOGGER.info("Adding sorting column: {} to job config", _sortingColumn);
      jobConf.set(InternalConfigConstants.SORTING_COLUMN_CONFIG, _sortingColumn);
      jobConf.set(InternalConfigConstants.SORTING_COLUMN_TYPE, _sortingColumnType.name());

      switch (_sortingColumnType) {
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
    if (_partitionColumn != null) {
      numReduceTasks = _numPartitions;
      jobConf.set(InternalConfigConstants.ENABLE_PARTITIONING, "true");
      job.setPartitionerClass(GenericPartitioner.class);
      jobConf.set(InternalConfigConstants.PARTITION_COLUMN_CONFIG, _partitionColumn);
      if (_partitionFunction != null) {
        jobConf.set(InternalConfigConstants.PARTITION_FUNCTION_CONFIG, _partitionFunction);
      }
      jobConf.setInt(InternalConfigConstants.NUM_PARTITIONS_CONFIG, numReduceTasks);
      job.setPartitionerClass(getPartitioner());
    } else {
      if (_numOutputFiles > 0) {
        numReduceTasks = _numOutputFiles;
      } else {
        // default number of input paths
        numReduceTasks = _inputDataPaths.size();
      }
    }
    // Maximum number of records per output file
    jobConf
        .set(InternalConfigConstants.PREPROCESSING_MAX_NUM_RECORDS_PER_FILE, Integer.toString(_maxNumRecordsPerFile));
    // Number of reducers
    LOGGER.info("Number of reduce tasks for pre-processing job: {}", numReduceTasks);
    job.setNumReduceTasks(numReduceTasks);

    setUpMapperReducerConfigs(job);

    return job;
  }

  abstract Class<? extends Partitioner> getPartitioner();

  abstract void setUpMapperReducerConfigs(Job job)
      throws IOException;

  abstract String getSampleTimeColumnValue(String timeColumnName)
      throws IOException;

  private void setValidationConfigs(Job job, Path path)
      throws IOException {
    SegmentsValidationAndRetentionConfig validationConfig = _tableConfig.getValidationConfig();

    // TODO: Serialize and deserialize validation config by creating toJson and fromJson
    // If the use case is an append use case, check that one time unit is contained in one file. If there is more
    // than one,
    // the job should be disabled, as we should not resize for these use cases. Therefore, setting the time column name
    // and value
    if (IngestionConfigUtils.getBatchSegmentIngestionType(_tableConfig).equalsIgnoreCase("APPEND")) {
      job.getConfiguration().set(InternalConfigConstants.IS_APPEND, "true");
      String timeColumnName = validationConfig.getTimeColumnName();
      job.getConfiguration().set(InternalConfigConstants.TIME_COLUMN_CONFIG, timeColumnName);
      if (timeColumnName != null) {
        DateTimeFieldSpec dateTimeFieldSpec = _pinotTableSchema.getSpecForTimeColumn(timeColumnName);
        if (dateTimeFieldSpec != null) {
          DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat());
          job.getConfiguration().set(InternalConfigConstants.SEGMENT_TIME_TYPE, formatSpec.getColumnUnit().toString());
          job.getConfiguration()
              .set(InternalConfigConstants.SEGMENT_TIME_FORMAT, formatSpec.getTimeFormat().toString());
          job.getConfiguration().set(InternalConfigConstants.SEGMENT_TIME_SDF_PATTERN, formatSpec.getSDFPattern());
        }
      }
      job.getConfiguration().set(InternalConfigConstants.SEGMENT_PUSH_FREQUENCY,
          IngestionConfigUtils.getBatchSegmentIngestionFrequency(_tableConfig));

      String sampleTimeColumnValue = getSampleTimeColumnValue(timeColumnName);
      if (sampleTimeColumnValue != null) {
        job.getConfiguration().set(InternalConfigConstants.TIME_COLUMN_VALUE, sampleTimeColumnValue);
      }
    }
  }

  private void setHadoopJobConfigs(Job job) {
    job.setJarByClass(HadoopSegmentPreprocessingJob.class);
    job.setJobName(getClass().getName());
    FileOutputFormat.setOutputPath(job, _outputPath);
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
}
