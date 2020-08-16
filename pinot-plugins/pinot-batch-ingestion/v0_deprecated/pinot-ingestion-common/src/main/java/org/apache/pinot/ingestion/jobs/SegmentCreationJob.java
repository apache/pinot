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
package org.apache.pinot.ingestion.jobs;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.utils.PushLocation;
import org.apache.pinot.spi.data.Schema;


public abstract class SegmentCreationJob extends BaseSegmentJob {
  protected static final String APPEND = "APPEND";

  protected final String _inputPattern;
  protected final String _outputDir;
  protected final String _stagingDir;
  protected final String _rawTableName;

  // Optional
  protected final String _depsJarDir;
  protected final String _schemaFile;
  protected final String _defaultPermissionsMask;
  protected final List<PushLocation> _pushLocations;
  protected final boolean _localDirectorySequenceId;

  public SegmentCreationJob(Properties properties) {
    super(properties);

    _inputPattern = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.PATH_TO_INPUT),
        String.format("Config: %s is missing in job property file.", JobConfigConstants.PATH_TO_INPUT));
    _outputDir = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.PATH_TO_OUTPUT),
        String.format("Config: %s is missing in job property file.", JobConfigConstants.PATH_TO_OUTPUT));
    _stagingDir = new Path(_outputDir, UUID.randomUUID().toString()).toString();
    _rawTableName = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME),
        String.format("Config: %s is missing in job property file.", JobConfigConstants.SEGMENT_TABLE_NAME));

    // Optional
    _depsJarDir = _properties.getProperty(JobConfigConstants.PATH_TO_DEPS_JAR);
    _schemaFile = _properties.getProperty(JobConfigConstants.PATH_TO_SCHEMA);
    _defaultPermissionsMask = _properties.getProperty(JobConfigConstants.DEFAULT_PERMISSIONS_MASK);
    String localDirectorySequenceId = _properties.getProperty(JobConfigConstants.LOCAL_DIRECTORY_SEQUENCE_ID);
    if (localDirectorySequenceId != null) {
      _localDirectorySequenceId = Boolean.parseBoolean(localDirectorySequenceId);
    } else {
      _localDirectorySequenceId = false;
    }

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
  public static Path getRelativeOutputPath(URI baseInputDir, URI inputFile, Path outputDir) {
    URI relativePath = baseInputDir.relativize(inputFile);
    Preconditions.checkState(relativePath.getPath().length() > 0 && !relativePath.equals(inputFile),
        "Unable to extract out the relative path based on base input path: " + baseInputDir);
    return new Path(outputDir, relativePath.getPath()).getParent();
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

  protected abstract void run()
      throws Exception;

  @Override
  protected Schema getSchema()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      if (controllerRestApi != null) {
        return controllerRestApi.getSchema();
      } else {
        // Schema file could be stored local or remotely.
        try (InputStream inputStream = FileSystem.get(new Path(_schemaFile).toUri(), getConf())
            .open(new Path(_schemaFile))) {
          return Schema.fromInputSteam(inputStream);
        }
      }
    }
  }

  protected void movePath(FileSystem fileSystem, String source, String destination, boolean override)
      throws IOException {
    for (FileStatus sourceFileStatus : fileSystem.listStatus(new Path(source))) {
      Path srcPath = sourceFileStatus.getPath();
      Path destPath = new Path(destination, srcPath.getName());
      if (fileSystem.isFile(srcPath)) {
        if (fileSystem.exists(destPath)) {
          if (override) {
            _logger.warn("The destination path {} already exists, trying to override it.", destPath);
            fileSystem.delete(destPath, false);
          } else {
            _logger.warn("The destination path {} already exists, skip it.", destPath);
            continue;
          }
        }
        _logger.info("Moving file from: {} to: {}", srcPath, destPath);
        if (!fileSystem.exists(destPath.getParent())) {
          fileSystem.mkdirs(destPath.getParent());
        }
        fileSystem.rename(srcPath, destPath);
      } else {
        movePath(fileSystem, srcPath.toString(), destPath.toString(), override);
      }
    }
  }

  public enum SchemaMisMatchCounter {
    DATA_TYPE_MISMATCH,
    SINGLE_VALUE_MULTI_VALUE_FIELD_MISMATCH,
    MULTI_VALUE_FIELD_STRUCTURE_MISMATCH
  }
}
