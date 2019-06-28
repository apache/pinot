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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.IndexingConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableCustomConfig;
import org.apache.pinot.hadoop.utils.PushLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  // Optional.
  private final Path _pathToDependencyJar;
  private final String _defaultPermissionsMask;

  private TableConfig _tableConfig;
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
    _logger.info("Pre-processing job is disabled.");
    return;
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

  private void setMaxNumRecordsConfigIfSpecified(Job job) {
    TableCustomConfig tableCustomConfig = _tableConfig.getCustomConfig();
    if (tableCustomConfig == null) {
      return;
    }
    Map<String, String> customConfigsMap = tableCustomConfig.getCustomConfigs();
    if (customConfigsMap != null && customConfigsMap.containsKey("max.num.records")) {
      int maxNumRecords = Integer.parseInt(customConfigsMap.get("max.num.records"));
      Preconditions.checkArgument(maxNumRecords > 0, "The value of max.num.records should be positive. Current value: " + customConfigsMap.get("max.num.records"));
      _logger.info("Setting max.num.records to {}", maxNumRecords);
      job.getConfiguration().set("max.num.records", Integer.toString(maxNumRecords));
    }
  }

  /**
   * Finds the avro file in the input folder, and returns its avro schema
   * @param inputPathDir Path to input directory
   * @return Input schema
   * @throws IOException exception when accessing to IO
   */
  private static Schema getSchema(Path inputPathDir)
      throws IOException {
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
  private static Schema extractSchemaFromAvro(Path avroFile)
      throws IOException {
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
  private static DataFileStream<GenericRecord> getAvroReader(Path avroFile)
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

  void cleanup() {
    _logger.info("Clean up pre-processing output path: {}", _preprocessedOutputDir);
    try {
      _fileSystem.delete(_preprocessedOutputDir, true);
    } catch (IOException e) {
      _logger.error("Failed to clean up pre-processing output path: {}", _preprocessedOutputDir);
    }
  }
}
