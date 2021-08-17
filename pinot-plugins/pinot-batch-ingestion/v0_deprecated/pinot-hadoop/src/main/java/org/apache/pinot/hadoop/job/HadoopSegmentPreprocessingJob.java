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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pinot.hadoop.job.preprocess.DataPreprocessingHelper;
import org.apache.pinot.hadoop.job.preprocess.DataPreprocessingHelperFactory;
import org.apache.pinot.hadoop.utils.PinotHadoopJobPreparationHelper;
import org.apache.pinot.hadoop.utils.preprocess.DataPreprocessingUtils;
import org.apache.pinot.hadoop.utils.preprocess.HadoopUtils;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentPreprocessingJob;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A Hadoop job which provides partitioning, sorting, and resizing against the input files, which is raw data in either Avro or Orc format.
 * Thus, the output files are partitioned, sorted, resized after this job.
 * In order to run this job, the following configs need to be specified in job properties:
 * * enable.preprocessing: false by default. Enables preprocessing job.
 */
public class HadoopSegmentPreprocessingJob extends SegmentPreprocessingJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(HadoopSegmentPreprocessingJob.class);

  private String _partitionColumn;
  private int _numPartitions;
  private String _partitionFunction;

  private String _sortingColumn;
  private FieldSpec.DataType _sortingColumnType;

  private int _numOutputFiles;
  private int _maxNumRecordsPerFile;

  private TableConfig _tableConfig;
  private org.apache.pinot.spi.data.Schema _pinotTableSchema;

  private Set<DataPreprocessingUtils.Operation> _preprocessingOperations;

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

    DataPreprocessingHelper dataPreprocessingHelper = DataPreprocessingHelperFactory.generateDataPreprocessingHelper(_inputSegmentDir, _preprocessedOutputDir);
    dataPreprocessingHelper
        .registerConfigs(_tableConfig, _pinotTableSchema, _partitionColumn, _numPartitions, _partitionFunction, _sortingColumn, _sortingColumnType,
            _numOutputFiles, _maxNumRecordsPerFile);

    Job job = dataPreprocessingHelper.setUpJob();

    // Since we aren't extending AbstractHadoopJob, we need to add the jars for the job to
    // distributed cache ourselves. Take a look at how the addFilesToDistributedCache is
    // implemented so that you know what it does.
    LOGGER.info("HDFS class path: " + _pathToDependencyJar);
    if (_pathToDependencyJar != null) {
      LOGGER.info("Copying jars locally.");
      PinotHadoopJobPreparationHelper.addDepsJarToDistributedCacheHelper(HadoopUtils.DEFAULT_FILE_SYSTEM, job, _pathToDependencyJar);
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

  private void fetchPreProcessingOperations() {
    _preprocessingOperations = new HashSet<>();
    TableCustomConfig customConfig = _tableConfig.getCustomConfig();
    if (customConfig != null) {
      Map<String, String> customConfigMap = customConfig.getCustomConfigs();
      if (customConfigMap != null && !customConfigMap.isEmpty()) {
        String preprocessingOperationsString = customConfigMap.getOrDefault(InternalConfigConstants.PREPROCESS_OPERATIONS, "");
        DataPreprocessingUtils.getOperations(_preprocessingOperations, preprocessingOperationsString);
      }
    }
  }

  private void fetchPartitioningConfig() {
    // Fetch partition info from table config.
    if (!_preprocessingOperations.contains(DataPreprocessingUtils.Operation.PARTITION)) {
      LOGGER.info("Partitioning is disabled.");
      return;
    }
    SegmentPartitionConfig segmentPartitionConfig = _tableConfig.getIndexingConfig().getSegmentPartitionConfig();
    if (segmentPartitionConfig != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap = segmentPartitionConfig.getColumnPartitionMap();
      Preconditions.checkArgument(columnPartitionMap.size() <= 1, "There should be at most 1 partition setting in the table.");
      if (columnPartitionMap.size() == 1) {
        _partitionColumn = columnPartitionMap.keySet().iterator().next();
        _numPartitions = segmentPartitionConfig.getNumPartitions(_partitionColumn);
        _partitionFunction = segmentPartitionConfig.getFunctionName(_partitionColumn);
      }
    } else {
      LOGGER.info("Segment partition config is null for table: {}", _tableConfig.getTableName());
    }
  }

  private void fetchSortingConfig() {
    if (!_preprocessingOperations.contains(DataPreprocessingUtils.Operation.SORT)) {
      LOGGER.info("Sorting is disabled.");
      return;
    }
    // Fetch sorting info from table config first.
    List<String> sortingColumns = new ArrayList<>();
    List<FieldConfig> fieldConfigs = _tableConfig.getFieldConfigList();
    if (fieldConfigs != null && !fieldConfigs.isEmpty()) {
      for (FieldConfig fieldConfig : fieldConfigs) {
        if (fieldConfig.getIndexType() == FieldConfig.IndexType.SORTED) {
          sortingColumns.add(fieldConfig.getName());
        }
      }
    }
    if (!sortingColumns.isEmpty()) {
      Preconditions.checkArgument(sortingColumns.size() == 1, "There should be at most 1 sorted column in the table.");
      _sortingColumn = sortingColumns.get(0);
      return;
    }

    // There is no sorted column specified in field configs, try to find sorted column from indexing config.
    IndexingConfig indexingConfig = _tableConfig.getIndexingConfig();
    List<String> sortedColumns = indexingConfig.getSortedColumn();
    if (sortedColumns != null) {
      Preconditions.checkArgument(sortedColumns.size() <= 1, "There should be at most 1 sorted column in the table.");
      if (sortedColumns.size() == 1) {
        _sortingColumn = sortedColumns.get(0);
        FieldSpec fieldSpec = _pinotTableSchema.getFieldSpecFor(_sortingColumn);
        Preconditions.checkState(fieldSpec != null, "Failed to find sorting column: {} in the schema", _sortingColumn);
        Preconditions.checkState(fieldSpec.isSingleValueField(), "Cannot sort on multi-value column: %s", _sortingColumn);
        _sortingColumnType = fieldSpec.getDataType();
        Preconditions.checkState(_sortingColumnType.canBeASortedColumn(), "Cannot sort on %s column: %s", _sortingColumnType, _sortingColumn);
        LOGGER.info("Sorting the data with column: {} of type: {}", _sortingColumn, _sortingColumnType);
      }
    }
  }

  private void fetchResizingConfig() {
    if (!_preprocessingOperations.contains(DataPreprocessingUtils.Operation.RESIZE)) {
      LOGGER.info("Resizing is disabled.");
      return;
    }
    TableCustomConfig tableCustomConfig = _tableConfig.getCustomConfig();
    if (tableCustomConfig == null) {
      _numOutputFiles = 0;
      return;
    }
    Map<String, String> customConfigsMap = tableCustomConfig.getCustomConfigs();
    if (customConfigsMap != null && customConfigsMap.containsKey(InternalConfigConstants.PREPROCESSING_NUM_REDUCERS)) {
      _numOutputFiles = Integer.parseInt(customConfigsMap.get(InternalConfigConstants.PREPROCESSING_NUM_REDUCERS));
      Preconditions.checkState(_numOutputFiles > 0,
          String.format("The value of %s should be positive! Current value: %s", InternalConfigConstants.PREPROCESSING_NUM_REDUCERS, _numOutputFiles));
    } else {
      _numOutputFiles = 0;
    }

    if (customConfigsMap != null) {
      int maxNumRecords;
      if (customConfigsMap.containsKey(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE)) {
        LOGGER.warn("The config: {} from custom config is deprecated. Use {} instead.", InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE,
            InternalConfigConstants.PREPROCESSING_MAX_NUM_RECORDS_PER_FILE);
        maxNumRecords = Integer.parseInt(customConfigsMap.get(InternalConfigConstants.PARTITION_MAX_RECORDS_PER_FILE));
      } else if (customConfigsMap.containsKey(InternalConfigConstants.PREPROCESSING_MAX_NUM_RECORDS_PER_FILE)) {
        maxNumRecords = Integer.parseInt(customConfigsMap.get(InternalConfigConstants.PREPROCESSING_MAX_NUM_RECORDS_PER_FILE));
      } else {
        return;
      }
      // TODO: add a in-built maximum value for this config to avoid having too many small files.
      // E.g. if the config is set to 1 which is smaller than this in-built value, the job should be abort from generating too many small files.
      Preconditions.checkArgument(maxNumRecords > 0,
          "The value of " + InternalConfigConstants.PREPROCESSING_MAX_NUM_RECORDS_PER_FILE + " should be positive. Current value: " + maxNumRecords);
      LOGGER.info("Setting {} to {}", InternalConfigConstants.PREPROCESSING_MAX_NUM_RECORDS_PER_FILE, maxNumRecords);
      _maxNumRecordsPerFile = maxNumRecords;
    }
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

  private void setTableConfigAndSchema()
      throws IOException {
    _tableConfig = getTableConfig();
    _pinotTableSchema = getSchema();

    Preconditions.checkState(_tableConfig != null, "Table config cannot be null.");
    Preconditions.checkState(_pinotTableSchema != null, "Schema cannot be null");
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
