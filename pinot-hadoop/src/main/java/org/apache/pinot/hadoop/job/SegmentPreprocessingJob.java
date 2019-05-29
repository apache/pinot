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
import org.apache.pinot.common.config.TableConfig;
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
import static org.apache.pinot.hadoop.job.JobConfigConstants.*;


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

  // Optional.
  private final Path _pathToDependencyJar;
  private final String _defaultPermissionsMask;
  protected final List<PushLocation> _pushLocations;

  private TableConfig _tableConfig;
  protected FileSystem _fileSystem;

  public SegmentPreprocessingJob(final Properties properties) {
    super(properties);

    if (_properties.getProperty(ENABLE_PARTITIONING) != null) {
      _enablePartitioning = Boolean.parseBoolean(_properties.getProperty(ENABLE_PARTITIONING));
    }

    if (_properties.getProperty(ENABLE_SORTING) != null) {
      _enableSorting = Boolean.parseBoolean(_properties.getProperty(ENABLE_SORTING));
    }

    if (!_enablePartitioning && _properties.getProperty(ENABLE_RESIZING) != null) {
      _enableResizing = Boolean.parseBoolean(_properties.getProperty(ENABLE_RESIZING));
    }

    // get input/output paths.
    _inputSegmentDir = Preconditions.checkNotNull(getPathFromProperty(PATH_TO_INPUT));
    _preprocessedOutputDir = Preconditions.checkNotNull(getPathFromProperty(PATH_TO_OUTPUT));
    _rawTableName = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME));

    _pathToDependencyJar = getPathFromProperty(PATH_TO_DEPS_JAR);
    _defaultPermissionsMask = _properties.getProperty(JobConfigConstants.DEFAULT_PERMISSIONS_MASK, null);

    // Optional push location and table parameters. If set, will use the table config and schema from the push hosts.
    String pushHostsString = _properties.getProperty(JobConfigConstants.PUSH_TO_HOSTS);
    String pushPortString = _properties.getProperty(JobConfigConstants.PUSH_TO_PORT);
    if (pushHostsString != null && pushPortString != null) {
      _pushLocations =
          PushLocation.getPushLocations(StringUtils.split(pushHostsString, ','), Integer.parseInt(pushPortString));
    } else {
      _pushLocations = null;
    }

    _logger.info("*********************************************************************");
    _logger.info("enable.partitioning: {}", _enablePartitioning);
    _logger.info("enable.sorting: {}", _enableSorting);
    _logger.info("enable.resizing: {}", _enableResizing);
    _logger.info("path.to.input: {}", _inputSegmentDir);
    _logger.info("path.to.output: {}", _preprocessedOutputDir);
    _logger.info("path.to.deps.jar: {}", _pathToDependencyJar);
    _logger.info("push.locations: {}", _pushLocations);
    _logger.info("*********************************************************************");
  }

  public void run() throws Exception {
    if (!_enablePartitioning && !_enableSorting && !_enableResizing) {
      _logger.info("Pre-processing job is disabled.");
      return;
    } else {
      _logger.info("Starting {}", getClass().getSimpleName());
    }

    _fileSystem = FileSystem.get(_conf);
    final List<Path> inputDataPath = getDataFilePaths(_inputSegmentDir);

    if (_fileSystem.exists(_preprocessedOutputDir)) {
      _logger.warn("Found the output folder {}, deleting it", _preprocessedOutputDir);
      _fileSystem.delete(_preprocessedOutputDir, true);
    }
    JobPreparationHelper.setDirPermission(_fileSystem, _preprocessedOutputDir, _defaultPermissionsMask);

    _tableConfig = getTableConfig();
    assert _tableConfig != null : "Table config shouldn't be null";

    if (_enablePartitioning) {
      fetchPartitioningConfig();
      _logger.info("{}: {}", PARTITION_COLUMN, _partitionColumn);
      _logger.info("{}: {}", NUMBER_OF_PARTITIONS, _numberOfPartitions);
      _logger.info("{}: {}", PARTITION_FUNCTION, _partitionColumn);
    }

    if (_enableSorting) {
      fetchSortingConfig();
      _logger.info("{}: {}", SORTED_COLUMN, _sortedColumn);
    }

    if (_enableResizing) {
      fetchResizingConfig();
      _logger.info("minimum number of output files: {}", _numberOfOutputFiles);
    }

    _logger.info("Initializing a pre-processing job");
    Job job = Job.getInstance(_conf);

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

    // Schema configs.
    Schema schema = getSchema(inputDataPath.get(0));
    _logger.info("Schema is: {}", schema.toString(true));

    // Mapper configs.
    job.setMapperClass(SegmentPreprocessingMapper.class);
    job.setMapOutputKeyClass(AvroKey.class);
    job.setMapOutputValueClass(AvroValue.class);
    job.getConfiguration().setInt(JobContext.NUM_MAPS, inputDataPath.size());

    // Reducer configs.
    job.setReducerClass(SegmentPreprocessingReducer.class);
    job.setOutputKeyClass(AvroKey.class);
    job.setOutputValueClass(NullWritable.class);
    if (_properties.getProperty(MAXIMUM_NUMBER_OF_RECORDS) != null) {
      _logger.info("Setting {} to {}", MAXIMUM_NUMBER_OF_RECORDS, _properties.getProperty(MAXIMUM_NUMBER_OF_RECORDS));
      job.getConfiguration().set(MAXIMUM_NUMBER_OF_RECORDS, _properties.getProperty(MAXIMUM_NUMBER_OF_RECORDS));
    }
    AvroMultipleOutputs.addNamedOutput(job, "avro", AvroKeyOutputFormat.class, schema);
    AvroMultipleOutputs.setCountersEnabled(job, true);
    LazyOutputFormat.setOutputFormatClass(job, AvroKeyOutputFormat.class);

    // Input and output paths.
    FileInputFormat.setInputPaths(job, _inputSegmentDir);
    FileOutputFormat.setOutputPath(job, _preprocessedOutputDir);
    _logger.info("Total number of files to be pre-processed: {}", inputDataPath.size());

    // Set up mapper output key
    Set<Schema.Field> fieldSet = new HashSet<>();

    // Partition configs.
    int numReduceTasks = (_numberOfPartitions != 0) ? _numberOfPartitions : inputDataPath.size();
    if (_partitionColumn != null) {
      // If partition column is specified, the other 2 param have to be specified as well.
      assert _numberOfPartitions != 0 : "Number of partitions should not be 0!";
      assert _partitionFunction != null : "Partition function should be specified in table config!";
      job.getConfiguration().set(ENABLE_PARTITIONING, "true");
      job.setPartitionerClass(GenericPartitioner.class);
      job.getConfiguration().set(PARTITION_COLUMN, _partitionColumn);
      if (_partitionFunction != null) {
        job.getConfiguration().set(PARTITION_FUNCTION, _partitionFunction);
      }
      job.getConfiguration().set(NUMBER_OF_PARTITIONS, Integer.toString(numReduceTasks));
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
      job.getConfiguration().set(SORTED_COLUMN, _sortedColumn);

      addSortedColumnField(schema, fieldSet);
    } else {
      // If sorting is disabled, hashcode will be the only factor for sort/group comparator.
      addHashCodeField(fieldSet);
    }

    // Creates a wrapper for the schema of output key in mapper.
    Schema mapperOutputKeySchema = Schema.createRecord(/*name*/"record", /*doc*/"", /*namespace*/"", false);
    mapperOutputKeySchema.setFields(new ArrayList<>(fieldSet));
    _logger.info("Mapper output schema: {}", mapperOutputKeySchema);

    AvroJob.setInputKeySchema(job, schema);
    AvroJob.setMapOutputKeySchema(job, mapperOutputKeySchema);
    AvroJob.setMapOutputValueSchema(job, schema);
    AvroJob.setOutputKeySchema(job, schema);

    // Since we aren't extending AbstractHadoopJob, we need to add the jars for the job to
    // distributed cache ourselves. Take a look at how the addFilesToDistributedCache is
    // implemented so that you know what it does.
    _logger.info("HDFS class path: " + _pathToDependencyJar);
    if (_pathToDependencyJar != null) {
      _logger.info("Copying jars locally.");
      JobPreparationHelper.addDepsJarToDistributedCacheHelper(_fileSystem, job, _pathToDependencyJar);
    } else {
      _logger.info("Property '{}' not specified.", PATH_TO_DEPS_JAR);
    }

    long startTime = System.currentTimeMillis();
    // Submit the job for execution.
    job.waitForCompletion(true);
    if (!job.isSuccessful()) {
      throw new RuntimeException("Job failed : " + job);
    }

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
      assert columnPartitionMap.size() <= 1 : "There should be at most 1 partition setting in the table.";
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
    assert sortedColumns.size() <= 1 : "There should be at most 1 sorted column in the table.";
    if (sortedColumns.size() == 1) {
      _sortedColumn = sortedColumns.get(0);
    }
  }

  private void fetchResizingConfig() {
    if (_properties.getProperty(MINIMUM_NUMBER_OF_OUTPUT_FILES) != null) {
      _numberOfOutputFiles = Integer.parseInt(_properties.getProperty(MINIMUM_NUMBER_OF_OUTPUT_FILES));
    } else {
      _numberOfOutputFiles = 0;
    }
  }

  /**
   * Finds the avro file in the input folder, and returns its avro schema
   * @param inputPathDir Path to input directory
   * @return Input schema
   * @throws IOException exception when accessing to IO
   */
  private static Schema getSchema(Path inputPathDir) throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Schema avroSchema = null;
    for (FileStatus fileStatus : fs.listStatus(inputPathDir)) {
      if (fileStatus.isFile() && fileStatus.getPath().getName().endsWith(".avro")) {
        _logger.info("Extracting schema from " + fileStatus.getPath());
        avroSchema = extractSchemaFromAvro(fileStatus.getPath());
        break;
      }
    }
    return avroSchema;
  }

  /**
   * Extracts avro schema from avro file
   * @param avroFile Input avro file
   * @return Schema in avro file
   * @throws IOException exception when accessing to IO
   */
  private static Schema extractSchemaFromAvro(Path avroFile) throws IOException {
    DataFileStream<GenericRecord> dataStreamReader = getAvroReader(avroFile);
    Schema avroSchema = dataStreamReader.getSchema();
    dataStreamReader.close();
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
  private static DataFileStream<GenericRecord> getAvroReader(Path avroFile) throws IOException {
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
    } else {
      sortedColumnAsKeySchema = Schema.create(sortedColumnSchema.getType());
    }
    Schema.Field columnField = new Schema.Field(_sortedColumn, sortedColumnAsKeySchema, "sortedColumn", null);
    fieldSet.add(columnField);
  }

  private void addHashCodeField(Set<Schema.Field> fieldSet) {
    Schema.Field hashCodeField = new Schema.Field("hashcode", Schema.create(Schema.Type.INT), "hashcode", null);
    fieldSet.add(hashCodeField);
  }

  @Override
  protected boolean isDataFile(String fileName) {
    return fileName.endsWith(".avro");
  }

  void cleanup() {
    _logger.info("Clean up pre-processing output path: {}", _preprocessedOutputDir);
    try {
      _fileSystem.delete(_preprocessedOutputDir, true);
    } catch (IOException e) {
      _logger.error("Failed to clean up pre-processing output path: {}", _preprocessedOutputDir);
    }
  }
}
