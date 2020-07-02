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
import java.io.Serializable;
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
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.DefaultControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.utils.PushLocation;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class BaseSegmentJob extends Configured implements Serializable {
  protected final Logger _logger = LoggerFactory.getLogger(getClass());
  protected final Properties _properties;
  protected final List<PushLocation> _pushLocations;
  protected final String _rawTableName;
  protected final int _lookBackPeriod;

  protected BaseSegmentJob(Properties properties) {
    _properties = properties;
    setConf(new Configuration());
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

    // Optional parameter to allow skip files older than given time period.
    String lookBackPeriodInDays = _properties.getProperty(JobConfigConstants.LOOK_BACK_PERIOD_IN_DAYS);
    if (lookBackPeriodInDays != null) {
      _lookBackPeriod = Integer.parseInt(lookBackPeriodInDays);
    } else {
      _lookBackPeriod = -1;
    }
  }

  @Nullable
  protected TableConfig getTableConfig()
      throws IOException {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      return controllerRestApi != null ? controllerRestApi.getTableConfig() : null;
    }
  }

  /**
   * This method is currently implemented in SegmentCreationJob and SegmentPreprocessingJob due to a dependency on
   * the hadoop filesystem, which we can only get once the job begins to run.
   * We return null here to make it clear that for now, all implementations of this method have to support
   * reading from a schema file. In the future, we hope to deprecate reading the schema from the schema file in favor
   * of mandating that a schema is pushed to the controller.
   */
  @Nullable
  protected Schema getSchema()
      throws IOException {
    return null;
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

  protected List<Path> getDataFilePaths(String pathPattern)
      throws IOException {
    return getDataFilePaths(new Path(pathPattern));
  }

  protected List<Path> getDataFilePaths(Path pathPattern)
      throws IOException {
    List<Path> tarFilePaths = new ArrayList<>();
    FileSystem fileSystem = FileSystem.get(pathPattern.toUri(), getConf());
    _logger.info("Using filesystem: {}", fileSystem);
    FileStatus[] fileStatuses = fileSystem.globStatus(pathPattern);
    if (fileStatuses == null) {
      _logger.warn("Unable to match file status from file path pattern: {}", pathPattern);
    } else {
      getDataFilePathsHelper(fileSystem, fileStatuses, tarFilePaths);
    }
    return tarFilePaths;
  }

  protected void retainRecentFiles(List<Path> filePaths, int lookBackPeriod) {
    if (lookBackPeriod > 0) {
      filePaths.removeIf(path -> {
        try {
          FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
          DateTime mtime = new DateTime(fs.getFileStatus(path).getModificationTime());
          if (mtime.plusDays(lookBackPeriod).isBeforeNow()) {
            _logger.info("Skip process older than {} days file: {}, last modification time is {}", lookBackPeriod, path,
                mtime);
          }
          return true;
        } catch (IOException e) {
          _logger.error("Failed to evaluate last modification time for path " + path, e);
        }
        return false;
      });
    }
  }

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
        if (isDataFile(path.getName())) {
          tarFilePaths.add(path);
        }
      }
    }
  }

  protected abstract boolean isDataFile(String fileName);
}
