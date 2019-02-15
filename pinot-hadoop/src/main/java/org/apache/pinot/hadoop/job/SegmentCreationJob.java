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
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.hadoop.job.mapper.SegmentCreationMapper;
import org.apache.pinot.hadoop.utils.PushLocation;


public class SegmentCreationJob extends BaseSegmentJob {
  protected static final String APPEND = "APPEND";

  protected final Path _inputPattern;
  protected final Path _outputDir;
  protected final Path _stagingDir;
  protected final String _rawTableName;

  // Optional
  protected final Path _depsJarDir;
  protected final Path _schemaFile;
  protected final String _defaultPermissionsMask;
  protected final List<PushLocation> _pushLocations;

  protected FileSystem _fileSystem;

  public SegmentCreationJob(Properties properties) {
    super(properties);
    _conf.set("mapreduce.job.user.classpath.first", "true");

    _inputPattern = Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PATH_TO_INPUT));
    _outputDir = Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PATH_TO_OUTPUT));
    _stagingDir = new Path(_outputDir, UUID.randomUUID().toString());
    _rawTableName = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME));

    // Optional
    _depsJarDir = getPathFromProperty(JobConfigConstants.PATH_TO_DEPS_JAR);
    _schemaFile = getPathFromProperty(JobConfigConstants.PATH_TO_SCHEMA);
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

    _logger.info("*********************************************************************");
    _logger.info("Input Pattern: {}", _inputPattern);
    _logger.info("Output Directory: {}", _outputDir);
    _logger.info("Staging Directory: {}", _stagingDir);
    _logger.info("Raw Table Name: {}", _rawTableName);
    _logger.info("Dependencies Directory: {}", _depsJarDir);
    _logger.info("Schema File: {}", _schemaFile);
    _logger.info("Default Permissions Mask: {}", _defaultPermissionsMask);
    _logger.info("Push Locations: {}", _pushLocations);
    _logger.info("*********************************************************************");
  }

  @Override
  protected boolean isDataFile(String fileName) {
    // Other files may have different extensions, eg: orc can have no extension
    if (_properties.getProperty(JobConfigConstants.RECORD_READER_PATH) != null) {
      return true;
    }
    return fileName.endsWith(".avro") || fileName.endsWith(".csv") || fileName.endsWith(".json") || fileName
        .endsWith(".thrift") || fileName.endsWith(".parquet");
  }

  public void run()
      throws Exception {
    _logger.info("Starting {}", getClass().getSimpleName());

    // Initialize all directories
    _fileSystem = FileSystem.get(_conf);
    mkdirs(_outputDir);
    mkdirs(_stagingDir);
    Path stagingInputDir = new Path(_stagingDir, "input");
    mkdirs(stagingInputDir);

    // Gather all data files
    List<Path> dataFilePaths = getDataFilePaths(_inputPattern);
    int numDataFiles = dataFilePaths.size();
    if (numDataFiles == 0) {
      String errorMessage = "No data file founded with pattern: " + _inputPattern;
      _logger.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      _logger.info("Creating segments with data files: {}", dataFilePaths);
      for (int i = 0; i < numDataFiles; i++) {
        Path dataFilePath = dataFilePaths.get(i);
        try (DataOutputStream dataOutputStream = _fileSystem.create(new Path(stagingInputDir, Integer.toString(i)))) {
          dataOutputStream.write(StringUtil.encodeUtf8(dataFilePath.toString() + " " + i));
          dataOutputStream.flush();
        }
      }
    }

    // Set up the job
    Job job = Job.getInstance(_conf);
    job.setJarByClass(getClass());
    job.setJobName(getClass().getName());

    Configuration jobConf = job.getConfiguration();
    String hadoopTokenFileLocation = System.getenv("HADOOP_TOKEN_FILE_LOCATION");
    if (hadoopTokenFileLocation != null) {
      jobConf.set("mapreduce.job.credentials.binary", hadoopTokenFileLocation);
    }
    jobConf.setInt(JobContext.NUM_MAPS, numDataFiles);

    // Set table config and schema
    TableConfig tableConfig = getTableConfig();
    if (tableConfig != null) {
      validateTableConfig(tableConfig);
      jobConf.set(JobConfigConstants.TABLE_CONFIG, tableConfig.toJsonConfigString());
    }
    jobConf.set(JobConfigConstants.SCHEMA, getSchema().toSingleLineJsonString());

    // Set additional configurations
    for (Map.Entry<Object, Object> entry : _properties.entrySet()) {
      jobConf.set(entry.getKey().toString(), entry.getValue().toString());
    }

    job.setMapperClass(getMapperClass());
    job.setNumReduceTasks(0);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, stagingInputDir);
    FileOutputFormat.setOutputPath(job, new Path(_stagingDir, "output"));

    addDepsJarToDistributedCache(job);
    addAdditionalJobProperties(job);

    // Submit the job
    job.waitForCompletion(true);
    if (!job.isSuccessful()) {
      throw new RuntimeException("Job failed: " + job);
    }

    moveSegmentsToOutputDir();

    // Delete the staging directory
    _logger.info("Deleting the staging directory: {}", _stagingDir);
    _fileSystem.delete(_stagingDir, true);
  }

  protected void mkdirs(Path dirPath)
      throws IOException {
    if (_fileSystem.exists(dirPath)) {
      _logger.warn("Deleting existing file: {}", dirPath);
      _fileSystem.delete(dirPath, true);
    }
    _logger.info("Making directory: {}", dirPath);
    _fileSystem.mkdirs(dirPath);
    setDirPermission(dirPath);
  }

  protected void setDirPermission(Path dirPath)
      throws IOException {
    if (_defaultPermissionsMask != null) {
      FsPermission permission = FsPermission.getDirDefault().applyUMask(new FsPermission(_defaultPermissionsMask));
      _logger.info("Setting permission: {} to directory: {}", permission, dirPath);
      _fileSystem.setPermission(dirPath, permission);
    }
  }

  @Nullable
  protected TableConfig getTableConfig()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      return controllerRestApi != null ? controllerRestApi.getTableConfig() : null;
    }
  }

  protected Schema getSchema()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      if (controllerRestApi != null) {
        return controllerRestApi.getSchema();
      } else {
        try (InputStream inputStream = _fileSystem.open(_schemaFile)) {
          return Schema.fromInputSteam(inputStream);
        }
      }
    }
  }

  /**
   * Can be overridden to provide custom controller Rest API.
   */
  @Nullable
  protected ControllerRestApi getControllerRestApi() {
    return _pushLocations != null ? new DefaultControllerRestApi(_pushLocations, _rawTableName) : null;
  }

  protected void validateTableConfig(TableConfig tableConfig) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();

    // For APPEND use case, timeColumnName and timeType must be set
    if (APPEND.equalsIgnoreCase(validationConfig.getSegmentPushType())) {
      Preconditions.checkState(validationConfig.getTimeColumnName() != null && validationConfig.getTimeType() != null,
          "For APPEND use case, time column and type must be set");
    }
  }

  /**
   * Can be overridden to plug in custom mapper.
   */
  protected Class<? extends Mapper<LongWritable, Text, LongWritable, Text>> getMapperClass() {
    return SegmentCreationMapper.class;
  }

  protected void addDepsJarToDistributedCache(Job job)
      throws IOException {
    if (_depsJarDir != null) {
      addDepsJarToDistributedCacheHelper(job, _depsJarDir);
    }
  }

  protected void addDepsJarToDistributedCacheHelper(Job job, Path depsJarDir)
      throws IOException {
    FileStatus[] fileStatuses = _fileSystem.listStatus(depsJarDir);
    for (FileStatus fileStatus : fileStatuses) {
      if (fileStatus.isDirectory()) {
        addDepsJarToDistributedCacheHelper(job, fileStatus.getPath());
      } else {
        Path depJarPath = fileStatus.getPath();
        if (depJarPath.getName().endsWith(".jar")) {
          _logger.info("Adding deps jar: {} to distributed cache", depJarPath);
          job.addCacheArchive(depJarPath.toUri());
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

  protected void moveSegmentsToOutputDir()
      throws IOException {
    Path segmentTarDir = new Path(new Path(_stagingDir, "output"), JobConfigConstants.SEGMENT_TAR_DIR);
    for (FileStatus segmentTarStatus : _fileSystem.listStatus(segmentTarDir)) {
      Path segmentTarPath = segmentTarStatus.getPath();
      Path dest = new Path(_outputDir, segmentTarPath.getName());
      _logger.info("Moving segment tar file from: {} to: {}", segmentTarPath, dest);
      _fileSystem.rename(segmentTarPath, dest);
    }
  }
}
