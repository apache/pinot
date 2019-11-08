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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.common.utils.DataSize;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.data.readers.CSVRecordReaderConfig;
import org.apache.pinot.core.data.readers.FileFormat;
import org.apache.pinot.core.data.readers.RecordReaderConfig;
import org.apache.pinot.core.data.readers.ThriftRecordReaderConfig;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.core.segment.name.SegmentNameGenerator;
import org.apache.pinot.core.segment.name.SimpleSegmentNameGenerator;
import org.apache.pinot.spark.utils.JobPreparationHelper;
import org.apache.pinot.spark.utils.PushLocation;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentCreationJob extends BaseSegmentJob {
  protected static final String APPEND = "APPEND";
  protected static final Logger LOGGER = LoggerFactory.getLogger(SegmentCreationJob.class);
  protected final String _rawTableName;

  protected final String _inputPattern;
  protected final String _outputDir;
  protected final String _stagingDir;
  // Optional
  protected final String _depsJarDir;
  protected final String _schemaFile;
  protected final String _defaultPermissionsMask;
  protected final List<PushLocation> _pushLocations;

  public SegmentCreationJob(Properties properties) {
    super(properties);
    new Configuration().set("mapreduce.job.user.classpath.first", "true");

    _inputPattern = Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PATH_TO_INPUT)).toString();
    _outputDir = Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PATH_TO_OUTPUT)).toString();
    _stagingDir = new Path(_outputDir, UUID.randomUUID().toString()).toString();
    _rawTableName = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME));

    // Optional
    _depsJarDir = properties.getProperty(JobConfigConstants.PATH_TO_DEPS_JAR);
    _schemaFile = properties.getProperty(JobConfigConstants.PATH_TO_SCHEMA);
    _defaultPermissionsMask = _properties.getProperty(JobConfigConstants.DEFAULT_PERMISSIONS_MASK);

    // Optional push location and table parameters. If set, will use the table config and schema from the push hosts.
    String pushHostsString = _properties.getProperty(JobConfigConstants.PUSH_TO_HOSTS);
    String pushPortString = _properties.getProperty(JobConfigConstants.PUSH_TO_PORT);
    if (pushHostsString != null && pushPortString != null) {
      _pushLocations =
          PushLocation.getPushLocations(StringUtils.split(pushHostsString, ','), Integer.parseInt(pushPortString));
    } else {
      _pushLocations = null;
    }

    LOGGER.info("*********************************************************************");
    LOGGER.info("Input Pattern: {}", _inputPattern);
    LOGGER.info("Output Directory: {}", _outputDir);
    LOGGER.info("Staging Directory: {}", _stagingDir);
    LOGGER.info("Raw Table Name: {}", _rawTableName);
    LOGGER.info("Dependencies Directory: {}", _depsJarDir);
    LOGGER.info("Schema File: {}", _schemaFile);
    LOGGER.info("Default Permissions Mask: {}", _defaultPermissionsMask);
    LOGGER.info("Push Locations: {}", _pushLocations);
    LOGGER.info("*********************************************************************");
  }

  /**
   * Generate a relative output directory path when `useRelativePath` flag is on.
   * This method will compute the relative path based on `inputFile` and `baseInputDir`,
   * then apply only the directory part of relative path to `outputDir`.
   * E.g.
   *    baseInputDir = "/path/to/input"
   *    inputFile = "/path/to/input/a/b/c/d.avro"
   *    outputDir = "/path/to/output"
   *    getRelativeOutputPath(baseInputDir, inputFile, outputDir) = /path/to/output/a/b/c
   */
  protected static Path getRelativeOutputPath(URI baseInputDir, URI inputFile, Path outputDir) {
    URI relativePath = baseInputDir.relativize(inputFile);
    Preconditions.checkState(relativePath.getPath().length() > 0 && !relativePath.equals(inputFile),
        "Unable to extract out the relative path based on base input path: " + baseInputDir);
    return new Path(outputDir, relativePath.getPath()).getParent();
  }

  /**
   * Can be overridden to set additional segment generator configs.
   */
  @SuppressWarnings("unused")
  protected static void addAdditionalSegmentGeneratorConfigs(SegmentGeneratorConfig segmentGeneratorConfig,
      Path hdfsInputFile, int sequenceId) {
  }

  @Override
  protected boolean isDataFile(String fileName) {
    // For custom record reader, treat all files as data file
    if (_properties.getProperty(JobConfigConstants.RECORD_READER_PATH) != null) {
      return true;
    }
    return fileName.endsWith(".avro") || fileName.endsWith(".csv") || fileName.endsWith(".json") || fileName
        .endsWith(".thrift");
  }

  public void run()
      throws Exception {
    LOGGER.info("Starting {}", getClass().getSimpleName());

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
    int numDataFiles = dataFilePaths.size();
    if (numDataFiles == 0) {
      String errorMessage = "No data file founded with pattern: " + inputPattern;
      LOGGER.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      LOGGER.info("Creating segments with data files: {}", dataFilePaths);
      for (int i = 0; i < numDataFiles; i++) {
        Path dataFilePath = dataFilePaths.get(i);
        try (DataOutputStream dataOutputStream = outputDirFileSystem
            .create(new Path(stagingInputDir, Integer.toString(i)))) {
          dataOutputStream.write(StringUtil.encodeUtf8(dataFilePath.toString() + " " + i));
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
      _properties.put(JobConfigConstants.TABLE_CONFIG, tableConfig.toJsonConfigString());
    }
    _properties.put(JobConfigConstants.SCHEMA, getSchema().toSingleLineJsonString());

    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
    addDepsJarToDistributedCache(sparkContext);
    JavaRDD<String> pathRDD = sparkContext.parallelize(dataFilePathStrs, numDataFiles);
    pathRDD.zipWithIndex().foreach(tuple2 -> {
      SegmentCreationMapper segmentCreationMapper =
          new SegmentCreationMapper(_properties, new Path(_stagingDir, "output").toString());
      segmentCreationMapper.run(tuple2._1, tuple2._2);
      segmentCreationMapper.cleanup();
    });

    moveSegmentsToOutputDir(outputDirFileSystem, _stagingDir, _outputDir);

    // Delete the staging directory
    LOGGER.info("Deleting the staging directory: {}", stagingDir);
    outputDirFileSystem.delete(stagingDir, true);
  }

  @Override
  protected Schema getSchema()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      if (controllerRestApi != null) {
        return controllerRestApi.getSchema();
      } else {
        // Schema file could be stored local or remotely.
        Path schemaFilePath = new Path(_schemaFile);
        try (InputStream inputStream = FileSystem.get(schemaFilePath.toUri(), new Configuration())
            .open(schemaFilePath)) {
          return Schema.fromInputSteam(inputStream);
        }
      }
    }
  }

  protected void validateTableConfig(TableConfig tableConfig) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();

    // For APPEND use case, timeColumnName and timeType must be set
    if (APPEND.equalsIgnoreCase(validationConfig.getSegmentPushType())) {
      Preconditions.checkState(validationConfig.getTimeColumnName() != null && validationConfig.getTimeType() != null,
          "For APPEND use case, time column and type must be set");
    }
  }

  protected void addDepsJarToDistributedCache(JavaSparkContext sparkContext)
      throws IOException {
    if (_depsJarDir != null) {
      Path depsJarPath = new Path(_depsJarDir);
      JobPreparationHelper
          .addDepsJarToDistributedCacheHelper(FileSystem.get(depsJarPath.toUri(), new Configuration()), sparkContext,
              depsJarPath);
    }
  }

  protected void moveSegmentsToOutputDir(FileSystem outputDirFileSystem, String stagingDir, String outputDir)
      throws IOException {
    Path segmentTarDir = new Path(new Path(stagingDir, "output"), JobConfigConstants.SEGMENT_TAR_DIR);
    for (FileStatus segmentTarStatus : outputDirFileSystem.listStatus(segmentTarDir)) {
      Path segmentTarPath = segmentTarStatus.getPath();
      Path dest = new Path(outputDir, segmentTarPath.getName());
      LOGGER.info("Moving segment tar file from: {} to: {}", segmentTarPath, dest);
      outputDirFileSystem.rename(segmentTarPath, dest);
    }
  }
}
