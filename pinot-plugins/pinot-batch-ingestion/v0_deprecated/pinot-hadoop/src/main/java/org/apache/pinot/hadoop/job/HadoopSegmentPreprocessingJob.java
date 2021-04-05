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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pinot.hadoop.io.CombineAvroKeyInputFormat;
import org.apache.pinot.hadoop.job.mappers.SegmentPreprocessingMapper;
import org.apache.pinot.hadoop.job.partitioners.GenericPartitioner;
import org.apache.pinot.hadoop.job.reducers.SegmentPreprocessingReducer;
import org.apache.pinot.hadoop.utils.PinotHadoopJobPreparationHelper;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentPreprocessingJob;
import org.apache.pinot.segment.local.partition.PartitionFunctionFactory;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Hadoop job which provides partitioning, sorting, and resizing against the input files, which is raw data in Avro format.
 * Thus, the output files are partitioned, sorted, resized after this job.
 * In order to run this job, the following configs need to be specified in job properties:
 * * enable.preprocessing: false by default. Enables preprocessing job.
 */
public class HadoopSegmentPreprocessingJob extends SegmentPreprocessingJob {
  private static final Logger _logger = LoggerFactory.getLogger(HadoopSegmentPreprocessingJob.class);
  protected FileSystem _fileSystem;
  private String _partitionColumn;
  private int _numPartitions;
  private String _partitionFunction;
  private String _sortedColumn;
  private int _numOutputFiles;
  private TableConfig _tableConfig;
  private org.apache.pinot.spi.data.Schema _pinotTableSchema;

  public HadoopSegmentPreprocessingJob(final Properties properties) {
    super(properties);
  }

  public void run()
      throws Exception {
    if (!_enablePreprocessing) {
      _logger.info("Pre-processing job is disabled.");
      return;
    } else {
      _logger.info("Starting {}", getClass().getSimpleName());
    }

    _fileSystem = FileSystem.get(_inputSegmentDir.toUri(), getConf());
    final List<Path> inputDataPaths = getDataFilePaths(_inputSegmentDir);
    Preconditions.checkState(inputDataPaths.size() != 0, "No files in the input directory.");

    if (_fileSystem.exists(_preprocessedOutputDir)) {
      _logger.warn("Found output folder {}, deleting", _preprocessedOutputDir);
      _fileSystem.delete(_preprocessedOutputDir, true);
    }
    setTableConfigAndSchema();

    _logger.info("Initializing a pre-processing job");
    Job job = Job.getInstance(getConf());

    Path sampleAvroPath = inputDataPaths.get(0);
    int numInputPaths = inputDataPaths.size();

    setValidationConfigs(job, sampleAvroPath);
    setHadoopJobConfigs(job, numInputPaths);

    // Avro Schema configs.
    Schema avroSchema = getSchema(sampleAvroPath);
    _logger.info("Schema is: {}", avroSchema.toString(true));
    setSchemaParams(job, avroSchema);

    // Set up mapper output key
    Set<Schema.Field> fieldSet = new HashSet<>();

    fetchPartitioningConfig();
    fetchSortingConfig();
    fetchResizingConfig();
    validateConfigsAgainstSchema(avroSchema);

    // Partition configs.
    int numReduceTasks = 0;
    if (_partitionColumn != null) {
      numReduceTasks = _numPartitions;
      job.getConfiguration().set(InternalConfigConstants.ENABLE_PARTITIONING, "true");
      job.setPartitionerClass(GenericPartitioner.class);
      job.getConfiguration().set(InternalConfigConstants.PARTITION_COLUMN_CONFIG, _partitionColumn);
      if (_partitionFunction != null) {
        job.getConfiguration().set(InternalConfigConstants.PARTITION_FUNCTION_CONFIG, _partitionFunction);
      }
      job.getConfiguration().set(InternalConfigConstants.NUM_PARTITIONS_CONFIG, Integer.toString(numReduceTasks));
      setMaxNumRecordsConfigIfSpecified(job);
    } else {
      if (_numOutputFiles > 0) {
        numReduceTasks = _numOutputFiles;
      } else {
        // default number of input paths
        numReduceTasks = inputDataPaths.size();
      }
      // Partitioning is disabled. Adding hashcode as one of the fields to mapper output key.
      // so that all the rows can be spread evenly.
      addHashCodeField(fieldSet);
    }
    job.setInputFormatClass(CombineAvroKeyInputFormat.class);

    _logger.info("Number of reduce tasks for pre-processing job: {}", numReduceTasks);
    job.setNumReduceTasks(numReduceTasks);

    // Sort config.
    if (_sortedColumn != null) {
      _logger.info("Adding sorted column: {} to job config", _sortedColumn);
      job.getConfiguration().set(InternalConfigConstants.SORTED_COLUMN_CONFIG, _sortedColumn);

      addSortedColumnField(avroSchema, fieldSet);
    } else {
      // If sorting is disabled, hashcode will be the only factor for sort/group comparator.
      addHashCodeField(fieldSet);
    }

    // Creates a wrapper for the schema of output key in mapper.
    Schema mapperOutputKeySchema = Schema.createRecord(/*name*/"record", /*doc*/"", /*namespace*/"", false);
    mapperOutputKeySchema.setFields(new ArrayList<>(fieldSet));
    _logger.info("Mapper output schema: {}", mapperOutputKeySchema);

    AvroJob.setInputKeySchema(job, avroSchema);
    AvroJob.setMapOutputKeySchema(job, mapperOutputKeySchema);
    AvroJob.setMapOutputValueSchema(job, avroSchema);
    AvroJob.setOutputKeySchema(job, avroSchema);

    // Since we aren't extending AbstractHadoopJob, we need to add the jars for the job to
    // distributed cache ourselves. Take a look at how the addFilesToDistributedCache is
    // implemented so that you know what it does.
    _logger.info("HDFS class path: " + _pathToDependencyJar);
    if (_pathToDependencyJar != null) {
      _logger.info("Copying jars locally.");
      PinotHadoopJobPreparationHelper.addDepsJarToDistributedCacheHelper(_fileSystem, job, _pathToDependencyJar);
    } else {
      _logger.info("Property '{}' not specified.", JobConfigConstants.PATH_TO_DEPS_JAR);
    }

    long startTime = System.currentTimeMillis();
    // Submit the job for execution.
    job.waitForCompletion(true);
    if (!job.isSuccessful()) {
      throw new RuntimeException("Job failed : " + job);
    }

    _logger.info("Finished pre-processing job in {}ms", (System.currentTimeMillis() - startTime));
  }

  private void fetchPartitioningConfig() {
    // Fetch partition info from table config.
    SegmentPartitionConfig segmentPartitionConfig = _tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      Preconditions
          .checkArgument(columnPartitionMap.size() <= 1, "There should be at most 1 partition setting in the table.");
      if (columnPartitionMap.size() == 1) {
        _partitionColumn = columnPartitionMap.keySet().iterator().next();
        _numPartitions = segmentPartitionConfig.getNumPartitions(_partitionColumn);
        _partitionFunction = segmentPartitionConfig.getFunctionName(_partitionColumn);
      }
    } else {
      _logger.info("Segment partition config is null for table: {}", _tableConfig.getTableName());
    }
  }

  private void fetchSortingConfig() {
    // Fetch sorting info from table config.
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (sortedColumns != null) {
      Preconditions.checkArgument(sortedColumns.size() <= 1, "There should be at most 1 sorted column in the table.");
      if (sortedColumns.size() == 1) {
        _sortedColumn = sortedColumns.get(0);
      }
    }
  }

  private void fetchResizingConfig() {
    TableCustomConfig tableCustomConfig = _tableConfig.getCustomConfig();
    if (tableCustomConfig == null) {
      _numOutputFiles = 0;
      return;
    }
    Map<String, String> customConfigsMap = tableCustomConfig.getCustomConfigs();
    if (customConfigsMap != null && customConfigsMap.containsKey(InternalConfigConstants.PREPROCESS_NUM_FILES)) {
      _numOutputFiles = Integer.parseInt(customConfigsMap.get(InternalConfigConstants.PREPROCESS_NUM_FILES));
      Preconditions.checkState(_numOutputFiles > 0, String
          .format("The value of %s should be positive! Current value: %s", InternalConfigConstants.PREPROCESS_NUM_FILES,
              _numOutputFiles));
    } else {
      _numOutputFiles = 0;
    }
  }

  private void setMaxNumRecordsConfigIfSpecified(Job job) {
    TableCustomConfig tableCustomConfig = _tableConfig.getCustomConfig();
    if (tableCustomConfig == null) {
      return;
    }
    Map<String, String> customConfigsMap = tableCustomConfig.getCustomConfigs();
    if (customConfigsMap != null && customConfigsMap
        .containsKey(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE)) {
      int maxNumRecords =
          Integer.parseInt(customConfigsMap.get(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE));
      Preconditions.checkArgument(maxNumRecords > 0,
          "The value of " + InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE
              + " should be positive. Current value: " + maxNumRecords);
      _logger.info("Setting {} to {}", InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE, maxNumRecords);
      job.getConfiguration()
          .set(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE, Integer.toString(maxNumRecords));
    }
  }

  /**
   * Finds the avro file in the input folder, and returns its avro schema
   * @param inputPathDir Path to input directory
   * @return Input schema
   * @throws IOException exception when accessing to IO
   */
  private Schema getSchema(Path inputPathDir)
      throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Schema avroSchema = null;
    for (FileStatus fileStatus : fs.listStatus(inputPathDir)) {
      if (fileStatus.isFile() && fileStatus.getPath().getName().endsWith(".avro")) {
        _logger.info("Extracting schema from " + fileStatus.getPath());
        try (DataFileStream<GenericRecord> dataStreamReader = getAvroReader(inputPathDir)) {
          avroSchema = dataStreamReader.getSchema();
        }
        break;
      }
    }
    return avroSchema;
  }

  private void addSortedColumnField(Schema schema, Set<Schema.Field> fieldSet) {
    // Sorting is enabled. Adding sorted column value to mapper output key.
    Schema sortedColumnSchema = schema.getField(_sortedColumn).schema();
    Schema sortedColumnAsKeySchema;
    if (sortedColumnSchema.getType().equals(Schema.Type.UNION)) {
      sortedColumnAsKeySchema = Schema.createUnion(sortedColumnSchema.getTypes());
    } else if (sortedColumnSchema.getType().equals(Schema.Type.ARRAY)) {
      sortedColumnAsKeySchema = Schema.createArray(sortedColumnSchema.getElementType());
    } else {
      sortedColumnAsKeySchema = Schema.create(sortedColumnSchema.getType());
    }
    Schema.Field columnField = new Schema.Field(_sortedColumn, sortedColumnAsKeySchema, "sortedColumn", null);
    fieldSet.add(columnField);
  }

  private void validateConfigsAgainstSchema(Schema schema) {
    if (_partitionColumn != null) {
      Preconditions.checkArgument(schema.getField(_partitionColumn) != null,
          String.format("Partition column: %s is not found from the schema of input files.", _partitionColumn));
      Preconditions.checkArgument(_numPartitions > 0,
          String.format("Number of partitions should be positive. Current value: %s", _numPartitions));
      Preconditions.checkArgument(_partitionFunction != null, "Partition function should not be null!");
      try {
        PartitionFunctionFactory.PartitionFunctionType.fromString(_partitionFunction);
      } catch (IllegalArgumentException e) {
        _logger.error("Partition function needs to be one of Modulo, Murmur, ByteArray, HashCode, it is currently {}",
            _partitionColumn);
        throw new IllegalArgumentException(e);
      }
    }
    if (_sortedColumn != null) {
      Preconditions.checkArgument(schema.getField(_sortedColumn) != null,
          String.format("Sorted column: %s is not found from the schema of input files.", _sortedColumn));
    }
  }

  private void addHashCodeField(Set<Schema.Field> fieldSet) {
    Schema.Field hashCodeField = new Schema.Field("hashcode", Schema.create(Schema.Type.INT), "hashcode", null);
    fieldSet.add(hashCodeField);
  }

  @Override
  protected org.apache.pinot.spi.data.Schema getSchema()
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

  private void setTableConfigAndSchema()
      throws IOException {
    _tableConfig = getTableConfig();
    _pinotTableSchema = getSchema();

    Preconditions.checkState(_tableConfig != null, "Table config cannot be null.");
    Preconditions.checkState(_pinotTableSchema != null, "Schema cannot be null");
  }

  private void setValidationConfigs(Job job, Path path)
      throws IOException {
    SegmentsValidationAndRetentionConfig validationConfig = _tableConfig.getValidationConfig();

    // TODO: Serialize and deserialize validation config by creating toJson and fromJson
    // If the use case is an append use case, check that one time unit is contained in one file. If there is more than one,
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
          job.getConfiguration()
              .set(InternalConfigConstants.SEGMENT_TIME_TYPE, formatSpec.getColumnUnit().toString());
          job.getConfiguration()
              .set(InternalConfigConstants.SEGMENT_TIME_FORMAT, formatSpec.getTimeFormat().toString());
          job.getConfiguration()
              .set(InternalConfigConstants.SEGMENT_TIME_SDF_PATTERN, formatSpec.getSDFPattern());
        }
      }
      job.getConfiguration().set(InternalConfigConstants.SEGMENT_PUSH_FREQUENCY,
          IngestionConfigUtils.getBatchSegmentIngestionFrequency(_tableConfig));
      try (DataFileStream<GenericRecord> dataStreamReader = getAvroReader(path)) {
        job.getConfiguration()
            .set(InternalConfigConstants.TIME_COLUMN_VALUE, dataStreamReader.next().get(timeColumnName).toString());
      }
    }
  }

  private void setHadoopJobConfigs(Job job, int numInputPaths) {
    job.getConfiguration().set(JobContext.JOB_NAME, this.getClass().getName());
    // Turn this on to always firstly use class paths that user specifies.
    job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, "true");
    // Turn this off since we don't need an empty file in the output directory
    job.getConfiguration().set(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, "false");

    job.setJarByClass(HadoopSegmentPreprocessingJob.class);

    String hadoopTokenFileLocation = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (hadoopTokenFileLocation != null) {
      job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY, hadoopTokenFileLocation);
    }

    // Mapper configs.
    job.setMapperClass(SegmentPreprocessingMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
    job.getConfiguration().setInt(JobContext.NUM_MAPS, numInputPaths);

    // Reducer configs.
    job.setReducerClass(SegmentPreprocessingReducer.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
  }

  private void setSchemaParams(Job job, Schema avroSchema)
      throws IOException {
    AvroMultipleOutputs.addNamedOutput(job, "avro", AvroKeyOutputFormat.class, avroSchema);
    AvroMultipleOutputs.setCountersEnabled(job, true);
    // Use LazyOutputFormat to avoid creating empty files.
    LazyOutputFormat.setOutputFormatClass(job, AvroKeyOutputFormat.class);

    // Input and output paths.
    FileInputFormat.setInputPaths(job, _inputSegmentDir);
    FileOutputFormat.setOutputPath(job, _preprocessedOutputDir);
  }
}
