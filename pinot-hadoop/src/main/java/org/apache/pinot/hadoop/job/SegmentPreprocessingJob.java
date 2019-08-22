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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableCustomConfig;
import org.apache.pinot.core.data.partition.PartitionFunctionFactory;
import org.apache.pinot.hadoop.io.CombineAvroKeyInputFormat;
import org.apache.pinot.hadoop.job.mappers.SegmentPreprocessingMapper;
import org.apache.pinot.hadoop.job.partitioners.GenericPartitioner;
import org.apache.pinot.hadoop.job.reducers.SegmentPreprocessingReducer;
import org.apache.pinot.hadoop.utils.JobPreparationHelper;
import org.apache.pinot.hadoop.utils.PushLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.mapreduce.MRJobConfig.*;
import static org.apache.hadoop.security.UserGroupInformation.*;


/**
 * A Hadoop job which provides partitioning, sorting, and resizing against the input files, which is raw data in Avro format.
 * Thus, the output files are partitioned, sorted, resized after this job.
 * In order to run this job, the following configs need to be specified in job properties:
 * * enable.preprocessing: false by default. Enables preprocessing job.
 * * preprocess.num.files: null by default. Number of files output.
 * * partition.max.records.per.file: null by default. The output file will be split into multiple files if the number of records exceeds this number.
 */
public class SegmentPreprocessingJob extends BaseSegmentJob {
  private static final Logger _logger = LoggerFactory.getLogger(SegmentPreprocessingJob.class);

  private boolean _enablePreprocessing;

  private String _partitionColumn;
  private int _numberOfPartitions;
  private String _partitionFunction;
  private String _sortedColumn;
  private int _numberOfOutputFiles;

  private final Path _inputSegmentDir;
  private final Path _preprocessedOutputDir;
  protected final String _rawTableName;
  protected final List<PushLocation> _pushLocations;
  private DataFileStream<GenericRecord> _dataStreamReader;

  // Optional.
  private final Path _pathToDependencyJar;
  private final String _defaultPermissionsMask;

  private TableConfig _tableConfig;
  private org.apache.pinot.common.data.Schema _pinotTableSchema;
  protected FileSystem _fileSystem;

  public SegmentPreprocessingJob(final Properties properties) {
    super(properties);

    _enablePreprocessing = Boolean.parseBoolean(_properties.getProperty(JobConfigConstants.ENABLE_PREPROCESSING, "false"));

    // get input/output paths.
    _inputSegmentDir = Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PATH_TO_INPUT));
    _preprocessedOutputDir = getPathFromProperty(_properties.getProperty(JobConfigConstants.PREPROCESS_PATH_TO_OUTPUT, getDefaultPreprocessPath()));
    _rawTableName = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME));

    _pathToDependencyJar = getPathFromProperty(JobConfigConstants.PATH_TO_DEPS_JAR);
    _defaultPermissionsMask = _properties.getProperty(JobConfigConstants.DEFAULT_PERMISSIONS_MASK, null);

    // Optional push location and table parameters. If set, will use the table config and schema from the push hosts.
    String pushHostsString = _properties.getProperty(JobConfigConstants.PUSH_TO_HOSTS);
    String pushPortString = _properties.getProperty(JobConfigConstants.PUSH_TO_PORT);
    if (pushHostsString != null && pushPortString != null) {
      _pushLocations =
          PushLocation.getPushLocations(StringUtils.split(pushHostsString, ','), Integer.parseInt(pushPortString));
    } else {
      throw new RuntimeException(String
          .format("Push location is mis-configured! %s: %s, %s: %s", JobConfigConstants.PUSH_TO_HOSTS, pushHostsString,
              JobConfigConstants.PUSH_TO_PORT, pushPortString));
    }

    _logger.info("*********************************************************************");
    _logger.info("enable.preprocessing: {}", _enablePreprocessing);
    _logger.info("path.to.input: {}", _inputSegmentDir);
    _logger.info("preprocess.path.to.output: {}", _preprocessedOutputDir);
    _logger.info("path.to.deps.jar: {}", _pathToDependencyJar);
    _logger.info("push.locations: {}", _pushLocations);
    _logger.info("*********************************************************************");
  }

  private String getDefaultPreprocessPath() {
    return _inputSegmentDir + "/" + "preprocess";
  }

  public void run()
      throws Exception {
    if (!_enablePreprocessing) {
      _logger.info("Pre-processing job is disabled.");
      return;
    } else {
      _logger.info("Starting {}", getClass().getSimpleName());
    }

    _fileSystem = FileSystem.get(_conf);
    final List<Path> inputDataPaths = getDataFilePaths(_inputSegmentDir);

    if (_fileSystem.exists(_preprocessedOutputDir)) {
      _logger.warn("Found the output folder {}, deleting it", _preprocessedOutputDir);
      _fileSystem.delete(_preprocessedOutputDir, true);
    }
    JobPreparationHelper.setDirPermission(_fileSystem, _preprocessedOutputDir, _defaultPermissionsMask);

    setTableConfigAndSchema();

    _logger.info("Initializing a pre-processing job");
    Job job = Job.getInstance(_conf);

    Path sampleAvroPath = inputDataPaths.get(0);
    int numInputPaths = inputDataPaths.size();

    setValidationConfigs(job, sampleAvroPath);
    setHadoopJobConfigs(job, numInputPaths);

    // Avro Schema configs.
    Schema avroSchema = getSchema(sampleAvroPath);
    _logger.info("Schema is: {}", avroSchema.toString(true));
    setSchemaParams(job, avroSchema, numInputPaths);

    // Set up mapper output key
    Set<Schema.Field> fieldSet = new HashSet<>();

    fetchPartitioningConfig();
    fetchSortingConfig();
    fetchResizingConfig();
    validateConfigsAgainstSchema(avroSchema);

    // Partition configs.
    int numReduceTasks = (_numberOfPartitions != 0) ? _numberOfPartitions : inputDataPaths.size();
    if (_partitionColumn != null) {
      job.getConfiguration().set(InternalConfigConstants.ENABLE_PARTITIONING, "true");
      job.setPartitionerClass(GenericPartitioner.class);
      job.getConfiguration().set(InternalConfigConstants.PARTITION_COLUMN_CONFIG, _partitionColumn);
      if (_partitionFunction != null) {
        job.getConfiguration().set(InternalConfigConstants.PARTITION_FUNCTION_CONFIG, _partitionFunction);
      }
      job.getConfiguration().set(InternalConfigConstants.NUM_PARTITIONS_CONFIG, Integer.toString(numReduceTasks));
      setMaxNumRecordsConfigIfSpecified(job);
    } else {
      if (_numberOfOutputFiles > 0) {
        numReduceTasks = _numberOfOutputFiles;
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
      JobPreparationHelper.addDepsJarToDistributedCacheHelper(_fileSystem, job, _pathToDependencyJar);
    } else {
      _logger.info("Property '{}' not specified.", JobConfigConstants.PATH_TO_DEPS_JAR);
    }

    long startTime = System.currentTimeMillis();
    // Submit the job for execution.
    job.waitForCompletion(true);
    if (!job.isSuccessful()) {
      throw new RuntimeException("Job failed : " + job);
    }

    cleanup();
    _logger.info("Finished pre-processing job in {}ms", (System.currentTimeMillis() - startTime));
  }

  private void fetchPartitioningConfig() {
    // Fetch partition info from table config.
    SegmentPartitionConfig segmentPartitionConfig = _tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      Preconditions.checkArgument(columnPartitionMap.size() <= 1, "There should be at most 1 partition setting in the table.");
      if (columnPartitionMap.size() == 1) {
        _partitionColumn = columnPartitionMap.keySet().iterator().next();
        _numberOfPartitions = segmentPartitionConfig.getNumPartitions(_partitionColumn);
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
    Preconditions.checkArgument(sortedColumns.size() <= 1, "There should be at most 1 sorted column in the table.");
    if (sortedColumns.size() == 1) {
      _sortedColumn = sortedColumns.get(0);
    }
  }

  private void fetchResizingConfig() {
    TableCustomConfig tableCustomConfig = _tableConfig.getCustomConfig();
    if (tableCustomConfig == null) {
      _numberOfOutputFiles = 0;
      return;
    }
    Map<String, String> customConfigsMap = tableCustomConfig.getCustomConfigs();
    if (customConfigsMap != null && customConfigsMap.containsKey(InternalConfigConstants.PREPROCESS_NUM_FILES)) {
      _numberOfOutputFiles = Integer.parseInt(customConfigsMap.get(InternalConfigConstants.PREPROCESS_NUM_FILES));
      Preconditions.checkArgument(_numberOfOutputFiles > 0, String.format("The value of %s should be positive! Current value: %s", InternalConfigConstants.PREPROCESS_NUM_FILES, customConfigsMap.get(InternalConfigConstants.PREPROCESS_NUM_FILES)));
    } else {
      _numberOfOutputFiles = 0;
    }
  }

  private void setMaxNumRecordsConfigIfSpecified(Job job) {
    TableCustomConfig tableCustomConfig = _tableConfig.getCustomConfig();
    if (tableCustomConfig == null) {
      return;
    }
    Map<String, String> customConfigsMap = tableCustomConfig.getCustomConfigs();
    if (customConfigsMap != null && customConfigsMap.containsKey(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE)) {
      int maxNumRecords = Integer.parseInt(customConfigsMap.get(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE));
      Preconditions.checkArgument(maxNumRecords > 0, "The value of " + InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE + " should be positive. Current value: " + customConfigsMap.get(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE));
      _logger.info("Setting {} to {}", InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE, maxNumRecords);
      job.getConfiguration().set(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE, Integer.toString(maxNumRecords));
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
        avroSchema = _dataStreamReader.getSchema();
        break;
      }
    }
    return avroSchema;
  }

  /**
   * Helper method that returns avro reader for the given avro file.
   * If file name ends in 'gz' then returns the GZIP version, otherwise gives the regular reader.
   *
   * @param avroFile File to read
   * @return Avro reader for the file.
   * @throws IOException exception when accessing to IO
   */
  private DataFileStream<GenericRecord> getAvroReader(Path avroFile)
      throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    if (avroFile.getName().endsWith("gz")) {
      return new DataFileStream<>(new GZIPInputStream(fs.open(avroFile)), new GenericDatumReader<>());
    } else {
      return new DataFileStream<>(fs.open(avroFile), new GenericDatumReader<>());
    }
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
      Preconditions.checkArgument(schema.getField(_partitionColumn) != null, String
          .format("Partition column: %s is not found from the schema of input files.", _partitionColumn));
      Preconditions.checkArgument(_numberOfPartitions > 0, String.format("Number of partitions should be positive. Current value: %s", _numberOfPartitions));
      Preconditions.checkArgument(_partitionFunction != null, "Partition function should not be null!");
      try {
        PartitionFunctionFactory.PartitionFunctionType.fromString(_partitionFunction);
      } catch (IllegalArgumentException e) {
        _logger.error("Partition function needs to be one of Modulo, Murmur, ByteArray, HashCode, it is currently {}", _partitionColumn);
        throw new IllegalArgumentException(e);
      }
    }
    if (_sortedColumn != null) {
      Preconditions.checkArgument(schema.getField(_sortedColumn) != null, String
          .format("Sorted column: %s is not found from the schema of input files.", _sortedColumn));
    }
  }

  private void addHashCodeField(Set<Schema.Field> fieldSet) {
    Schema.Field hashCodeField = new Schema.Field("hashcode", Schema.create(Schema.Type.INT), "hashcode", null);
    fieldSet.add(hashCodeField);
  }

  private void setTableConfigAndSchema() {
    // If push locations, table config, and schema are not configured, this does not necessarily mean that segments
    // cannot be created. We should allow the user to go to the next step rather than failing the job.
    if (_pushLocations.isEmpty()) {
      _logger.error("Push locations cannot be empty. "
          + "They are needed to get the table config and schema needed for this step. Skipping pre-processing");
      return;
    }
    ControllerRestApi controllerRestApi = new DefaultControllerRestApi(_pushLocations, _rawTableName);
    _tableConfig = controllerRestApi.getTableConfig();
    _pinotTableSchema = controllerRestApi.getSchema();

    if (_tableConfig == null) {
      _logger.error("Table config cannot be null. Skipping pre-processing");
      return;
    }

    if (_pinotTableSchema == null) {
      _logger.error("Schema cannot be null. Skipping pre-processing");
    }
  }

  private void setValidationConfigs(Job job, Path path) throws IOException {
    SegmentsValidationAndRetentionConfig validationConfig = _tableConfig.getValidationConfig();

    // TODO: Serialize and deserialize validation config by creating toJson and fromJson
    // If the use case is an append use case, check that one time unit is contained in one file. If there is more than one,
    // the job should be disabled, as we should not resize for these use cases. Therefore, setting the time column name
    // and value
    if (validationConfig.getSegmentPushType().equalsIgnoreCase("APPEND")) {
      job.getConfiguration().set(InternalConfigConstants.IS_APPEND, "true");
      String timeColumnName = _pinotTableSchema.getTimeFieldSpec().getName();
      job.getConfiguration().set(InternalConfigConstants.TIME_COLUMN_CONFIG, timeColumnName);
      job.getConfiguration().set(InternalConfigConstants.SEGMENT_TIME_TYPE, validationConfig.getTimeType().toString());
      job.getConfiguration().set(InternalConfigConstants.SEGMENT_TIME_FORMAT, _pinotTableSchema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeFormat());
      job.getConfiguration().set(InternalConfigConstants.SEGMENT_PUSH_FREQUENCY, validationConfig.getSegmentPushFrequency());
      _dataStreamReader = getAvroReader(path);
      job.getConfiguration().set(InternalConfigConstants.TIME_COLUMN_VALUE, (String) _dataStreamReader.next().get(timeColumnName));
    }
  }

  private void setHadoopJobConfigs(Job job, int numInputPaths) {
    job.getConfiguration().set(JobContext.JOB_NAME, this.getClass().getName());
    // Turn this on to always firstly use class paths that user specifies.
    job.getConfiguration().set(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, "true");
    // Turn this off since we don't need an empty file in the output directory
    job.getConfiguration().set(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, "false");

    job.setJarByClass(SegmentPreprocessingJob.class);

    String hadoopTokenFileLocation = System.getenv(HADOOP_TOKEN_FILE_LOCATION);
    if (hadoopTokenFileLocation != null) {
      job.getConfiguration().set(MAPREDUCE_JOB_CREDENTIALS_BINARY, hadoopTokenFileLocation);
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

  private void setSchemaParams(Job job, Schema avroSchema, int numInputPaths) throws IOException {
    AvroMultipleOutputs.addNamedOutput(job, "avro", AvroKeyOutputFormat.class, avroSchema);
    AvroMultipleOutputs.setCountersEnabled(job, true);
    // Use LazyOutputFormat to avoid creating empty files.
    LazyOutputFormat.setOutputFormatClass(job, AvroKeyOutputFormat.class);

    // Input and output paths.
    FileInputFormat.setInputPaths(job, _inputSegmentDir);
    FileOutputFormat.setOutputPath(job, _preprocessedOutputDir);
    _logger.info("Total number of files to be pre-processed: {}", numInputPaths);
  }

  @Override
  protected boolean isDataFile(String fileName) {
    // TODO: support orc format in the future.
    return fileName.endsWith(".avro");
  }

  void cleanup() throws IOException {
    _dataStreamReader.close();
    // TODO: Move this to post-segment creation
//    _logger.info("Clean up pre-processing output path: {}", _preprocessedOutputDir);
//    try {
//      _fileSystem.delete(_preprocessedOutputDir, true);
//    } catch (IOException e) {
//      _logger.error("Failed to clean up pre-processing output path: {}", _preprocessedOutputDir);
//    }
  }
}
