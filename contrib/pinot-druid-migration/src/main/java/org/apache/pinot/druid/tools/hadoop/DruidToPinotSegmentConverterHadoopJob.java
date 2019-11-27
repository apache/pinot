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
package org.apache.pinot.druid.tools.hadoop;

import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.pinot.common.config.ColumnPartitionConfig;
import org.apache.pinot.common.config.SegmentPartitionConfig;
import org.apache.pinot.common.config.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableTaskConfig;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.controller.helix.ControllerRequestURLBuilder;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.hadoop.job.BaseSegmentJob;
import org.apache.pinot.hadoop.job.ControllerRestApi;
import org.apache.pinot.hadoop.job.DefaultControllerRestApi;
import org.apache.pinot.hadoop.job.JobConfigConstants;
import org.apache.pinot.hadoop.utils.JobPreparationHelper;
import org.apache.pinot.hadoop.utils.PushLocation;


public class DruidToPinotSegmentConverterHadoopJob extends BaseSegmentJob {
  protected static final String APPEND = "APPEND";

  protected final Path _inputPattern;
  protected final Path _outputDir;
  protected final Path _stagingDir;
  protected final String _rawTableName;
  protected final List<PushLocation> _pushLocations;

  // Optional
  protected final Path _depsJarDir;
  protected final Path _schemaFile;
  protected final String _defaultPermissionsMask;

  // Output Directory FileSystem
  protected FileSystem _outputDirFileSystem;

  public DruidToPinotSegmentConverterHadoopJob(Properties properties) {
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
    _logger.info("*********************************************************************");
  }

  protected boolean isDataFile(URI s) {
    if (_properties.getProperty(JobConfigConstants.RECORD_READER_PATH) != null) {
      return true;
    }
    // Check that the Druid segment directory has all the expected components/files
    File file = new File(s);
    FileFileFilter fileFilter = new FileFileFilter()
    {
      @Override
      public boolean accept(File pathname)
      {
        return pathname.exists()
            && pathname.isFile();
      }
    };
    DirectoryFileFilter dirFilter = new DirectoryFileFilter() {
      @Override
      public boolean accept(File pathname)
      {
        return pathname.exists()
            && pathname.isDirectory();
      }
    };

    if (file.getAbsoluteFile().exists()) {
      File untaredInput = new File(file.getParentFile().getPath(), "untaredInput");
      untaredInput.mkdir();
      List<File> untaredFileList = new ArrayList<>();
      try {
        untaredFileList = TarGzCompressionUtils.unTar(file, untaredInput);
      } catch (Exception e) {
        e.printStackTrace();
      }
      File untaredInputFile = untaredFileList.get(0);

      Collection<File> fileList = FileUtils.listFiles(untaredInputFile, fileFilter, dirFilter);

      return fileList.stream().anyMatch(o -> o.getName().equals("meta.smoosh"))
          && fileList.stream().anyMatch(o -> o.getName().equals("version.bin"));
    }
    return false;
  }

  public void run()
      throws Exception {
    _logger.info("Starting {}", getClass().getSimpleName());
    // Initialize all directories
    _outputDirFileSystem = FileSystem.get(_outputDir.toUri(), _conf);
    JobPreparationHelper.mkdirs(_outputDirFileSystem, _outputDir, _defaultPermissionsMask);
    JobPreparationHelper.mkdirs(_outputDirFileSystem, _stagingDir, _defaultPermissionsMask);
    Path stagingInputDir = new Path(_stagingDir, "input");
    JobPreparationHelper.mkdirs(_outputDirFileSystem, stagingInputDir, _defaultPermissionsMask);

    List<Path> dataFilePaths = getDataFilePaths(_inputPattern);
    int numDataFiles = dataFilePaths.size();
    if (numDataFiles == 0) {
      String errorMessage = "No data file found with pattern: " + _inputPattern;
      _logger.error(errorMessage);
      throw new RuntimeException(errorMessage);
    } else {
      _logger.info("Creating segments with data files: {}", dataFilePaths);
      for (int i = 0; i < numDataFiles; i++) {
        Path dataFilePath = dataFilePaths.get(i);
        try (DataOutputStream dataOutputStream = _outputDirFileSystem.create(new Path(stagingInputDir, Integer.toString(i)))) {
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
    ColumnPartitionConfig columnPartitionConfig = new ColumnPartitionConfig("murmur", 2);
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put("longnum", columnPartitionConfig);
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(columnPartitionConfigMap);

    // Create the table
    TimeUnit timeUnit = getSchema().getOutgoingTimeUnit();
    String timeUnitString = null;
    if (timeUnit != null) {
      timeUnitString = timeUnit.toString();
    }
    addOfflineTable(_rawTableName, getSchema().getTimeColumnName(), timeUnitString, null, null,
        null, SegmentVersion.v3, null, null, null, segmentPartitionConfig, null);

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

    // Submit the job
    job.waitForCompletion(true);
    if (!job.isSuccessful()) {
      throw new RuntimeException("Job failed: " + job);
    }

    moveSegmentsToOutputDir();

    // Delete the staging directory
    _logger.info("Deleting the staging directory: {}", _stagingDir);
    _outputDirFileSystem.delete(_stagingDir, true);
  }

  /**
   * Can be overridden to provide custom controller Rest API.
   */
  @Nullable
  protected ControllerRestApi getControllerRestApi() {
    return _pushLocations != null ? new DefaultControllerRestApi(_pushLocations, _rawTableName) : null;
  }

  protected void addOfflineTable(String tableName, String timeColumnName, String timeType, String brokerTenant,
      String serverTenant, String loadMode, SegmentVersion segmentVersion, List<String> invertedIndexColumns,
      List<String> bloomFilterColumns, TableTaskConfig taskConfig, SegmentPartitionConfig segmentPartitionConfig,
      String sortedColumn)
      throws Exception {

    // TODO: So far this only works if one host is specified in job.properties; does not handle comma-separated hosts
    String controllerBaseApiUrl = "http://" +  _properties.get(JobConfigConstants.PUSH_TO_HOSTS) + ":"
        + _properties.get(JobConfigConstants.PUSH_TO_PORT);

    ControllerRequestURLBuilder controllerRequestURLBuilder = ControllerRequestURLBuilder.baseUrl(controllerBaseApiUrl);

    TableConfig tableConfig = getOfflineTableConfig(tableName, timeColumnName, timeType, brokerTenant, serverTenant,
        loadMode, segmentVersion, invertedIndexColumns, bloomFilterColumns, taskConfig, segmentPartitionConfig,
        sortedColumn, "daily");

    sendPostRequest(controllerRequestURLBuilder.forTableCreate(), tableConfig.toJsonConfigString());
  }

  public static String sendPostRequest(String urlString, String payload)
      throws IOException {
    return sendPostRequest(urlString, payload, Collections.EMPTY_MAP);
  }

  public static String sendPostRequest(String urlString, String payload, Map<String, String> headers)
      throws IOException {
    HttpURLConnection httpConnection = (HttpURLConnection) new URL(urlString).openConnection();
    httpConnection.setRequestMethod("POST");
    if (headers != null) {
      for (String key : headers.keySet()) {
        httpConnection.setRequestProperty(key, headers.get(key));
      }
    }

    if (payload != null && !payload.isEmpty()) {
      httpConnection.setDoOutput(true);
      try (BufferedWriter writer = new BufferedWriter(
          new OutputStreamWriter(httpConnection.getOutputStream(), StandardCharsets.UTF_8))) {
        writer.write(payload, 0, payload.length());
        writer.flush();
      }
    }

    return constructResponse(httpConnection.getInputStream());
  }

  private static String constructResponse(InputStream inputStream)
      throws IOException {
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      StringBuilder responseBuilder = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        responseBuilder.append(line);
      }
      return responseBuilder.toString();
    }
  }

  private static TableConfig getOfflineTableConfig(String tableName, String timeColumnName, String timeType,
      String brokerTenant, String serverTenant, String loadMode, SegmentVersion segmentVersion,
      List<String> invertedIndexColumns, List<String> bloomFilterColumns, TableTaskConfig taskConfig,
      SegmentPartitionConfig segmentPartitionConfig, String sortedColumn, String segmentPushFrequency) {
    return new TableConfig.Builder(CommonConstants.Helix.TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName(timeColumnName)
        .setTimeType(timeType)
        .setSegmentPushFrequency(segmentPushFrequency)
        .setNumReplicas(1)
        .setBrokerTenant(brokerTenant)
        .setServerTenant(serverTenant)
        .setLoadMode(loadMode)
        .setSegmentVersion(segmentVersion.toString())
        .setInvertedIndexColumns(invertedIndexColumns)
        .setBloomFilterColumns(bloomFilterColumns)
        .setTaskConfig(taskConfig)
        .setSegmentPartitionConfig(segmentPartitionConfig)
        .build();
  }


  protected Schema getSchema()
      throws IOException {
    try (InputStream inputStream = FileSystem.get(_schemaFile.toUri(), getConf()).open(_schemaFile)) {
      return Schema.fromInputSteam(inputStream);
    }
  }

  @Override
  protected List<Path> getDataFilePaths(Path pathPattern)
      throws IOException {
    List<Path> filePaths = new ArrayList<>();
    FileSystem fileSystem = FileSystem.get(pathPattern.toUri(), _conf);
    _logger.info("Using filesystem: {}", fileSystem);
    FileStatus[] fileStatuses = fileSystem.globStatus(pathPattern);
    if (fileStatuses == null) {
      _logger.warn("Unable to match file status from file path pattern: {}", pathPattern);
    } else {
      getDataFilePathsHelper(fileSystem, fileStatuses, filePaths);
    }
    return filePaths;
  }

  @Override
  protected void getDataFilePathsHelper(FileSystem fileSystem, FileStatus[] fileStatuses, List<Path> tarFilePaths)
      throws IOException {
    for (FileStatus fileStatus : fileStatuses) {
      Path path = fileStatus.getPath();
      if (fileStatus.isDirectory()) {
        getDataFilePathsHelper(fileSystem, fileSystem.listStatus(path), tarFilePaths);
      } else {
        // Skip temp files generated by computation frameworks like Hadoop/Spark.
        if (path.getName().startsWith("_") || path.getName().startsWith(".")) {
          continue;
        }
        if (isDataFile(path.toUri())) {
          tarFilePaths.add(path);
        }
      }
    }
  }

  // Unused
  @Override
  protected boolean isDataFile(String fileName) {
    return false;
  }

  @Nullable
  protected TableConfig getTableConfig()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      return controllerRestApi != null ? controllerRestApi.getTableConfig() : null;
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

  protected void addDepsJarToDistributedCache(Job job)
      throws IOException {
    if (_depsJarDir != null) {
      JobPreparationHelper.addDepsJarToDistributedCacheHelper(FileSystem.get(_depsJarDir.toUri(), getConf()), job, _depsJarDir);
    }
  }

  protected void moveSegmentsToOutputDir()
      throws IOException {
    Path segmentTarDir = new Path(new Path(_stagingDir, "output"), JobConfigConstants.SEGMENT_TAR_DIR);
    for (FileStatus segmentTarStatus : _outputDirFileSystem.listStatus(segmentTarDir)) {
      Path segmentTarPath = segmentTarStatus.getPath();
      Path dest = new Path(_outputDir, segmentTarPath.getName());
      _logger.info("Moving segment tar file from: {} to: {}", segmentTarPath, dest);
      _outputDirFileSystem.rename(segmentTarPath, dest);
    }
  }

  protected Class<? extends Mapper<LongWritable, Text, LongWritable, Text>> getMapperClass() {
    return DruidToPinotSegmentConverterMapper.class;
  }
}
