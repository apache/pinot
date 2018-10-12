/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.hadoop;

import azkaban.jobExecutor.AbstractJob;
import com.linkedin.pinot.hadoop.job.ControllerRestApi;
import com.linkedin.pinot.hadoop.job.JobConfigConstants;
import com.linkedin.pinot.hadoop.job.SegmentCreationJob;
import com.linkedin.pinot.hadoop.utils.PushLocation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotBuildAndPushJob extends AbstractJob {
  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotBuildAndPushJob.class);

  private static SegmentCreationJob _segmentCreationJob;
  private static ControllerRestApi _controllerRestApi;
  private static Properties _properties;

  public PinotBuildAndPushJob(String name, azkaban.utils.Props jobProps) throws Exception {
    super(name, Logger.getLogger(name));

    _properties = jobProps.toProperties();
    String hostsString = _properties.getProperty(JobConfigConstants.PUSH_TO_HOSTS);
    String portString = _properties.getProperty(JobConfigConstants.PUSH_TO_PORT);
    String tableName = _properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME);

    // Parse and add push locations
    List<PushLocation> pushLocations = new ArrayList<>();
    String[] hosts = hostsString.split(",");
    int port = Integer.parseInt(portString);
    for (String host : hosts) {
      pushLocations.add(new PushLocation.PushLocationBuilder().setHost(host).setPort(port).build());
    }

    _controllerRestApi = new ControllerRestApi(pushLocations, tableName);
    _segmentCreationJob = new SegmentCreationJob(name, _properties);
  }

  @Override
  public void run() throws Exception {
    _segmentCreationJob.run();

    FileSystem fs = FileSystem.get(new Configuration());
    String pathToOutput = _properties.getProperty(JobConfigConstants.PATH_TO_OUTPUT);

    Path pathToOutputPath = new Path(pathToOutput);
    FileStatus[] fileStatusArray = fs.globStatus(pathToOutputPath);
    if (fileStatusArray == null || fileStatusArray.length == 0) {
      throw new RuntimeException("Output path: " + pathToOutputPath + " does not exist or does not contain any file");
    }

    List<Path> tarFilePathList = new ArrayList<>();
    getTarFilePathsFromDir(fs, fileStatusArray, tarFilePathList);

    _controllerRestApi.sendSegments(tarFilePathList, fs);
  }

  /**
   * Recursively add all tar file paths under the {@link FileStatus} array into the file paths list.
   */
  private static void getTarFilePathsFromDir(FileSystem fs, FileStatus[] fileStatuses, List<Path> tarFilePathList)
      throws IOException {
    for (FileStatus fileStatus : fileStatuses) {
      Path filePath = fileStatus.getPath();
      String filePathString = filePath.toString();
      if (fileStatus.isDirectory()) {
        LOGGER.info("Trying to add all tar files from directory: " + filePathString);
        getTarFilePathsFromDir(fs, fs.listStatus(filePath), tarFilePathList);
      } else {
        if (filePathString.endsWith(JobConfigConstants.TARGZ)) {
          LOGGER.info("  Adding tar file: " + filePathString);
          tarFilePathList.add(filePath);
        }
      }
    }
  }
}