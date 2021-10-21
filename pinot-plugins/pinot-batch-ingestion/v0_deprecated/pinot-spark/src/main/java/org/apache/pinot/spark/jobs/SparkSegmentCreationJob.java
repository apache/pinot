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
package org.apache.pinot.spark.jobs;

import com.google.common.base.Preconditions;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentCreationJob;
import org.apache.pinot.ingestion.utils.JobPreparationHelper;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spark.utils.PinotSparkJobPreparationHelper;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;


public class SparkSegmentCreationJob extends SegmentCreationJob {
  private static final Logger _logger = LoggerFactory.getLogger(SparkSegmentCreationJob.class);

  public SparkSegmentCreationJob(Properties properties) {
    super(properties);
  }

  /**
   * Can be overridden to set additional segment generator configs.
   */
  @SuppressWarnings("unused")
  protected static void addAdditionalSegmentGeneratorConfigs(SegmentGeneratorConfig segmentGeneratorConfig,
      Path hdfsInputFile, int sequenceId) {
  }

  public void run()
      throws Exception {
    _logger.info("Starting {}", getClass().getSimpleName());

    Path inputPattern = new Path(_inputPattern);
    Path outputDir = new Path(_stagingDir);
    Path stagingDir = new Path(_stagingDir);

    // Initialize all directories
    FileSystem outputDirFileSystem = FileSystem.get(outputDir.toUri(), new Configuration());
    JobPreparationHelper.mkdirs(outputDirFileSystem, outputDir, _defaultPermissionsMask);
    JobPreparationHelper.mkdirs(outputDirFileSystem, stagingDir, _defaultPermissionsMask);
    Path stagingInputDir = new Path(stagingDir, "input");
    JobPreparationHelper.mkdirs(outputDirFileSystem, stagingInputDir, _defaultPermissionsMask);

    // Gather all data files
    List<Path> dataFilePaths = getDataFilePaths(inputPattern);
    int originalNumDataFiles = dataFilePaths.size();
    retainRecentFiles(dataFilePaths, _lookBackPeriod);
    int numDataFiles = dataFilePaths.size();
    if (numDataFiles == 0) {
      if (originalNumDataFiles > 0) {
        _logger.info("No input files within {} days to be processed.", _lookBackPeriod);
        return;
      }
      String errorMessage = "No data file founded with pattern: " + inputPattern;
      _logger.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      _logger.info("Creating segments with data files: {}", dataFilePaths);
      for (int i = 0; i < numDataFiles; i++) {
        Path dataFilePath = dataFilePaths.get(i);
        try (DataOutputStream dataOutputStream = outputDirFileSystem
            .create(new Path(stagingInputDir, Integer.toString(i)))) {
          dataOutputStream.write((dataFilePath.toString() + " " + i).getBytes(UTF_8));
          dataOutputStream.flush();
        }
      }
    }

    // Set up the job
    List<String> dataFilePathStrs = new ArrayList<>();
    for (Path dataFilePath : dataFilePaths) {
      dataFilePathStrs.add(dataFilePath.toString());
    }

    // Set table config and schema
    TableConfig tableConfig = getTableConfig();
    if (tableConfig != null) {
      validateTableConfig(tableConfig);
      _properties.put(JobConfigConstants.TABLE_CONFIG, tableConfig.toJsonString());
    }
    _properties.put(JobConfigConstants.SCHEMA, getSchema().toSingleLineJsonString());

    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
    addDepsJarToDistributedCache(sparkContext);
    JavaRDD<String> pathRDD = sparkContext.parallelize(dataFilePathStrs, numDataFiles);
    if (_localDirectorySequenceId) {
      Map<String, List<String>> localDirIndex = new HashMap<>();
      for (Path dataFilePath : dataFilePaths) {
        Path parentPath = dataFilePath.getParent();
        if (!localDirIndex.containsKey(parentPath.toString())) {
          localDirIndex.put(parentPath.toString(), new ArrayList<>());
        }
        localDirIndex.get(parentPath.toString()).add(dataFilePath.toString());
      }
      pathRDD.foreach(path -> {
        SparkSegmentCreationFunction sparkSegmentCreationFunction =
            new SparkSegmentCreationFunction(_properties, new Path(_stagingDir, "output").toString());
        sparkSegmentCreationFunction.run(path, getLocalDirIndex(localDirIndex, path));
        sparkSegmentCreationFunction.cleanup();
      });
    } else {
      pathRDD.zipWithIndex().foreach(tuple2 -> {
        SparkSegmentCreationFunction sparkSegmentCreationFunction =
            new SparkSegmentCreationFunction(_properties, new Path(_stagingDir, "output").toString());
        sparkSegmentCreationFunction.run(tuple2._1, tuple2._2);
        sparkSegmentCreationFunction.cleanup();
      });
    }

    moveSegmentsToOutputDir(outputDirFileSystem, _stagingDir, _outputDir);

    // Delete the staging directory
    _logger.info("Deleting the staging directory: {}", stagingDir);
    outputDirFileSystem.delete(stagingDir, true);
  }

  private Long getLocalDirIndex(Map<String, List<String>> localDirIndex, String pathStr) {
    return new Long(localDirIndex.get(new Path(pathStr).getParent().toString()).indexOf(pathStr));
  }

  protected void validateTableConfig(TableConfig tableConfig) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();

    // For APPEND use case, timeColumnName and timeType must be set
    if (APPEND.equalsIgnoreCase(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig))) {
      Preconditions.checkState(validationConfig.getTimeColumnName() != null && validationConfig.getTimeType() != null,
          "For APPEND use case, time column and type must be set");
    }
  }

  protected void addDepsJarToDistributedCache(JavaSparkContext sparkContext)
      throws IOException {
    if (_depsJarDir != null) {
      Path depsJarPath = new Path(_depsJarDir);
      PinotSparkJobPreparationHelper
          .addDepsJarToDistributedCacheHelper(FileSystem.get(depsJarPath.toUri(), new Configuration()), sparkContext,
              depsJarPath);
    }
  }

  protected void moveSegmentsToOutputDir(FileSystem outputDirFileSystem, String stagingDir, String outputDir)
      throws IOException {
    Path segmentTarDir = new Path(new Path(stagingDir, "output"), JobConfigConstants.SEGMENT_TAR_DIR);
    _logger.info("Moving all segment tar files from: {} to: {}", stagingDir, outputDir);
    movePath(outputDirFileSystem, segmentTarDir.toString(), outputDir, true);
  }
}
