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
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.hadoop.utils.PushLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseSegmentJob extends Configured {
  protected final Logger _logger = LoggerFactory.getLogger(getClass());
  protected final Properties _properties;
  protected final Configuration _conf;
  protected final List<PushLocation> _pushLocations;
  protected final String _rawTableName;

  protected BaseSegmentJob(Properties properties) {
    _properties = properties;
    _conf = new Configuration();
    setConf(_conf);
    Utils.logVersions();
    logProperties();

    // Optional push location and table parameters. If set, will use the table config and schema from the push hosts.
    String pushHostsString = _properties.getProperty(JobConfigConstants.PUSH_TO_HOSTS);
    String pushPortString = _properties.getProperty(JobConfigConstants.PUSH_TO_PORT);
    if (pushHostsString != null && pushPortString != null) {
      _pushLocations =
          PushLocation.getPushLocations(StringUtils.split(pushHostsString, ','), Integer.parseInt(pushPortString));
    } else {
      _pushLocations = null;
    }

    _rawTableName = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME));

  }

  @Nullable
  protected TableConfig getTableConfig()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      return controllerRestApi != null ? controllerRestApi.getTableConfig() : null;
    }
  }

  /**
   * Can be overridden to provide custom controller Rest API.
   */
  @Nullable
  protected ControllerRestApi getControllerRestApi() {
    return _pushLocations != null ? new DefaultControllerRestApi(_pushLocations, _rawTableName) : null;
  }

  protected void logProperties() {
    _logger.info("*********************************************************************");
    _logger.info("Job Properties: {}", _properties);
    _logger.info("*********************************************************************");
  }

  @Nullable
  protected Path getPathFromProperty(String key) {
    String value = _properties.getProperty(key);
    return value != null ? new Path(value) : null;
  }

  protected List<Path> getDataFilePaths(Path pathPattern)
      throws IOException {
    List<Path> tarFilePaths = new ArrayList<>();
    FileSystem fileSystem = FileSystem.get(_conf);
    getDataFilePathsHelper(fileSystem, fileSystem.globStatus(pathPattern), tarFilePaths);
    return tarFilePaths;
  }

  protected void getDataFilePathsHelper(FileSystem fileSystem, FileStatus[] fileStatuses, List<Path> tarFilePaths)
      throws IOException {
    for (FileStatus fileStatus : fileStatuses) {
      Path path = fileStatus.getPath();
      if (fileStatus.isDirectory()) {
        getDataFilePathsHelper(fileSystem, fileSystem.listStatus(path), tarFilePaths);
      } else {
        if (isDataFile(path.getName())) {
          tarFilePaths.add(path);
        }
      }
    }
  }

  protected abstract boolean isDataFile(String fileName);
}
