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
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
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
import static org.apache.pinot.hadoop.job.InternalConfigConstants.*;


/**
 * A Hadoop job which provides partitioning, sorting, and resizing against the input files, which is raw data in Avro format.
 * Thus, the output files are partitioned, sorted, resized after this job.
 * In order to run this job, the following configs need to be specified in job properties:
 * * enable.partitioning: false by default. If enabled, this job will fetch the partition config from table config.
 * * enable.sorting: false by default. If enabled, the job will fetch sorted column from table config.
 * * enable.resizing: false by default. If partitioning is enabled, this is force to be disabled. If enabled, min.num.output.files is required.
 * * min.num.output.files: null by default. It's required if resizing is enabled.
 * * max.num.records: null by default. The output file will be split into multiple files if the number of records exceeded max.num.records.
 */
public class SegmentPreprocessingJob extends BaseSegmentJob {
  private static final Logger _logger = LoggerFactory.getLogger(SegmentPreprocessingJob.class);

  private boolean _enablePartitioning = false;
  private boolean _enableSorting = false;
  private boolean _enableResizing = false;

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

    if (_properties.getProperty(JobConfigConstants.ENABLE_PARTITIONING) != null) {
      _enablePartitioning = Boolean.parseBoolean(_properties.getProperty(JobConfigConstants.ENABLE_PARTITIONING));
    }

    if (_properties.getProperty(JobConfigConstants.ENABLE_SORTING) != null) {
      _enableSorting = Boolean.parseBoolean(_properties.getProperty(JobConfigConstants.ENABLE_SORTING));
    }

    boolean resizingForcedDisabled = false;
    if (_properties.getProperty(JobConfigConstants.ENABLE_RESIZING) != null) {
      if (!_enablePartitioning) {
        _enableResizing = Boolean.parseBoolean(_properties.getProperty(JobConfigConstants.ENABLE_RESIZING));
      } else {
        resizingForcedDisabled = true;
        _logger.warn("Resizing cannot be enabled since partitioning has already been enabled!");
      }
    }

    // get input/output paths.
    _inputSegmentDir = Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PATH_TO_INPUT));
    _preprocessedOutputDir =
        Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PREPROCESS_PATH_TO_OUTPUT));
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
    _logger.info("enable.partitioning: {}", _enablePartitioning);
    _logger.info("enable.sorting: {}", _enableSorting);
    _logger.info("enable.resizing: {} {}", _enableResizing,
        (resizingForcedDisabled ? "(forced to be disabled since partitioning is enabled)" : ""));
    _logger.info("path.to.input: {}", _inputSegmentDir);
    _logger.info("preprocess.path.to.output: {}", _preprocessedOutputDir);
    _logger.info("path.to.deps.jar: {}", _pathToDependencyJar);
    _logger.info("push.locations: {}", _pushLocations);
    _logger.info("*********************************************************************");
  }

  public void run()
      throws Exception {
    // TODO: Remove once the job is ready
    _enablePartitioning = false;
    _enableSorting = false;
    _enableResizing = false;

    if (!_enablePartitioning && !_enableSorting && !_enableResizing) {
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

    // If push locations, table config, and schema are not configured, this does not necessarily mean that segments
    // cannot be created. We should allow the user to go to the next step rather than failing the job.
    if (_pushLocations.isEmpty()) {
      _logger.error("Push locations cannot be empty. "
          + "They are needed to get the table config and schema needed for this step. Skipping pre-processing");
      return;
    }

    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      _tableConfig = controllerRestApi.getTableConfig();
      _pinotTableSchema = controllerRestApi.getSchema();
    }

    if (_tableConfig == null) {
      _logger.error("Table config cannot be null. Skipping pre-processing");
      return;
    }

    if (_pinotTableSchema == null) {
      _logger.error("Schema cannot be null. Skipping pre-processing");
    }

    SegmentsValidationAndRetentionConfig validationConfig = _tableConfig.getValidationConfig();

    _logger.info("Initializing a pre-processing job");
    Job job = Job.getInstance(_conf);

    long totalAvroRecords = countAvroRecords(inputDataPaths);

    // TODO: Serialize and deserialize validation config by creating toJson and fromJson
    // If the use case is an append use case, check that one time unit is contained in one file. If there is more than one,
    // the job should be disabled, as we should not resize for these use cases. Therefore, setting the time column name
    // and value
    if (validationConfig.getSegmentPushType().equalsIgnoreCase("APPEND")) {
      job.getConfiguration().set(IS_APPEND, "true");
      String timeColumnName = _pinotTableSchema.getTimeFieldSpec().getName();
      job.getConfiguration().set(TIME_COLUMN_CONFIG, timeColumnName);
      job.getConfiguration().set(SEGMENT_TIME_TYPE, validationConfig.getTimeType().toString());
      job.getConfiguration().set(SEGMENT_TIME_FORMAT, _pinotTableSchema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeFormat());
      job.getConfiguration().set(SEGMENT_PUSH_FREQUENCY, validationConfig.getSegmentPushFrequency());
      _dataStreamReader = getAvroReader(inputDataPaths.get(0));
      job.getConfiguration().set(TIME_COLUMN_VALUE, (String) _dataStreamReader.next().get(timeColumnName));
    }

    if (_enablePartitioning) {
      fetchPartitioningConfig();
      _logger.info("{}: {}", PARTITION_COLUMN_CONFIG, _partitionColumn);
      _logger.info("{}: {}", NUM_PARTITIONS_CONFIG, _numberOfPartitions);
      _logger.info("{}: {}", PARTITION_FUNCTION_CONFIG, _partitionColumn);
    }

    if (_enableSorting) {
      fetchSortingConfig();
      _logger.info("{}: {}", SORTED_COLUMN_CONFIG, _sortedColumn);
    }

    if (_enableResizing) {
      fetchResizingConfig();
      _logger.info("minimum number of output files: {}", _numberOfOutputFiles);
    }

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

    // Avro Schema configs.
    Schema avroSchema = getSchema(inputDataPaths.get(0));
    _logger.info("Schema is: {}", avroSchema.toString(true));

    // Validates configs against schema.
    validateConfigsAgainstSchema(avroSchema);

    // Mapper configs.
    job.setMapperClass(SegmentPreprocessingMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
    job.getConfiguration().setInt(JobContext.NUM_MAPS, inputDataPaths.size());

    // Reducer configs.
    job.setReducerClass(SegmentPreprocessingReducer.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);

    AvroMultipleOutputs.addNamedOutput(job, "avro", AvroKeyOutputFormat.class, avroSchema);
    AvroMultipleOutputs.setCountersEnabled(job, true);
    // Use LazyOutputFormat to avoid creating empty files.
    LazyOutputFormat.setOutputFormatClass(job, AvroKeyOutputFormat.class);

    // Input and output paths.
    FileInputFormat.setInputPaths(job, _inputSegmentDir);
    FileOutputFormat.setOutputPath(job, _preprocessedOutputDir);
    _logger.info("Total number of files to be pre-processed: {}", inputDataPaths.size());

    // Set up mapper output key
    Set<Schema.Field> fieldSet = new HashSet<>();

    // Partition configs.
    int numReduceTasks = (_numberOfPartitions != 0) ? _numberOfPartitions : inputDataPaths.size();
    if (_partitionColumn != null) {
      job.getConfiguration().set(JobConfigConstants.ENABLE_PARTITIONING, "true");
      job.setPartitionerClass(GenericPartitioner.class);
      job.getConfiguration().set(PARTITION_COLUMN_CONFIG, _partitionColumn);
      if (_partitionFunction != null) {
        job.getConfiguration().set(PARTITION_FUNCTION_CONFIG, _partitionFunction);
      }
      job.getConfiguration().set(NUM_PARTITIONS_CONFIG, Integer.toString(numReduceTasks));
    } else {
      if (_numberOfOutputFiles > 0) {
        numReduceTasks = _numberOfOutputFiles;
      }
      // Partitioning is disabled. Adding hashcode as one of the fields to mapper output key.
      // so that all the rows can be spread evenly.
      addHashCodeField(fieldSet);
    }
    setMaxNumRecordsConfigIfSpecified(job, totalAvroRecords);
    job.setInputFormatClass(CombineAvroKeyInputFormat.class);

    _logger.info("Number of reduce tasks for pre-processing job: {}", numReduceTasks);
    job.setNumReduceTasks(numReduceTasks);

    // Sort config.
    if (_sortedColumn != null) {
      _logger.info("Adding sorted column: {} to job config", _sortedColumn);
      job.getConfiguration().set(SORTED_COLUMN_CONFIG, _sortedColumn);

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

  @Nullable
  private TableConfig getTableConfig()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      return controllerRestApi != null ? controllerRestApi.getTableConfig() : null;
    }
  }

  /**
   * Can be overridden to provide custom controller Rest API.
   */
  @Nullable
  private ControllerRestApi getControllerRestApi() {
    return _pushLocations != null ? new DefaultControllerRestApi(_pushLocations, _rawTableName) : null;
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
    if (customConfigsMap != null && customConfigsMap.containsKey("min.num.output.files")) {
      _numberOfOutputFiles = Integer.parseInt(customConfigsMap.get("min.num.output.files"));
      Preconditions.checkArgument(_numberOfOutputFiles > 0, String.format("The value of min.num.output.files should be positive! Current value: %s", customConfigsMap.get("min.num.output.files")));
    } else {
      _numberOfOutputFiles = 0;
    }
  }

  // TODO: Handle case where #records exceed long max
  private void setMaxNumRecordsConfigIfSpecified(Job job, long totalAvroRecords) {
    TableCustomConfig tableCustomConfig = _tableConfig.getCustomConfig();
    if (tableCustomConfig == null) {
      return;
    }
    Map<String, String> customConfigsMap = tableCustomConfig.getCustomConfigs();
    if (customConfigsMap != null && customConfigsMap.containsKey("max.num.records")) {
      long maxNumRecords = Long.getLong(customConfigsMap.get("max.num.records"));
      Preconditions.checkArgument(maxNumRecords > 0, "The value of max.num.records should be positive. Current value: " + customConfigsMap.get("max.num.records"));
      _logger.info("Received max.num.records as {}", maxNumRecords);
      long approxMaxRecords = getAverageMaxNumRecords(maxNumRecords, totalAvroRecords);
      _logger.info("After approximation to even out file size, setting max.num.records to {}", approxMaxRecords);
      job.getConfiguration().set("max.num.records", Long.toString(maxNumRecords));
    }
  }

  /**
   * Guarantees that max.num.records is an upper bound. This method is to ensure that segments are evenly distributed.
   * @param maxNumRecords what client has set as max number of records
   * @param totalAvroRecords total avro records in all input paths
   * @return suggested approximate max num records so # records in files are around even
   */
  private long getAverageMaxNumRecords(long maxNumRecords, long totalAvroRecords) {
    long approxNumFiles = totalAvroRecords / maxNumRecords;
    // There is some remainder, means we will redivide max num records
    if (approxNumFiles * maxNumRecords != totalAvroRecords) {
      return (totalAvroRecords / (approxNumFiles + 1)) + 1;
    } else {
      return maxNumRecords;
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

  private long countAvroRecords(List<Path> inputPaths) throws IOException {
    long count = 0;

    for (Path path : inputPaths) {
      DataFileStream<GenericRecord> dataFileStream = getAvroReader(path);
      GenericRecord record=new GenericData.Record(dataFileStream.getSchema());
      while (dataFileStream.hasNext()) {
        count ++;
        dataFileStream.next(record);
      }
      dataFileStream.close();
    }
    return count;
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
    if (_enablePartitioning) {
      Preconditions.checkArgument(_partitionColumn != null, "Partition column should not be null!");
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
    if (_enableSorting) {
      Preconditions.checkArgument(_sortedColumn != null, "Sorted column should not be null!");
      Preconditions.checkArgument(schema.getField(_sortedColumn) != null, String
          .format("Sorted column: %s is not found from the schema of input files.", _sortedColumn));
    }
  }

  private void addHashCodeField(Set<Schema.Field> fieldSet) {
    Schema.Field hashCodeField = new Schema.Field("hashcode", Schema.create(Schema.Type.INT), "hashcode", null);
    fieldSet.add(hashCodeField);
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
