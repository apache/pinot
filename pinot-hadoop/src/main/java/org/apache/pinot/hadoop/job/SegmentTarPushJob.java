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
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.hadoop.utils.PushLocation;


public class SegmentTarPushJob extends BaseSegmentJob {
  private final Path _segmentPattern;
  private final List<PushLocation> _pushLocations;
  private final String _rawTableName;
  private final boolean _deleteExtraSegments;

  public SegmentTarPushJob(Properties properties) {
    super(properties);
    _segmentPattern = Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PATH_TO_OUTPUT));
    String[] hosts = StringUtils.split(properties.getProperty(JobConfigConstants.PUSH_TO_HOSTS), ',');
    int port = Integer.parseInt(properties.getProperty(JobConfigConstants.PUSH_TO_PORT));
    _pushLocations = PushLocation.getPushLocations(hosts, port);
    _rawTableName = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME));
    _deleteExtraSegments = Boolean.parseBoolean(properties.getProperty(JobConfigConstants.IS_DELETE_EXTRA_SEGMENTS, "false"));
  }

  @Override
  protected boolean isDataFile(String fileName) {
    return fileName.endsWith(JobConfigConstants.TAR_GZ_FILE_EXT);
  }

  public void run()
      throws Exception {
    FileSystem fileSystem = FileSystem.get(_conf);
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      // TODO: Deal with invalid prefixes in the future
      if (_deleteExtraSegments) {
        deleteExtraSegments(controllerRestApi, fileSystem);
      } else {
        controllerRestApi.pushSegments(fileSystem, getDataFilePaths(_segmentPattern));
      }
    }
  }

  /**
   * Deletes extra segments after pushing to the controller
   * @param controllerRestApi
   * @param fileSystem
   * @throws IOException
   */
  public void deleteExtraSegments(ControllerRestApi controllerRestApi, FileSystem fileSystem) throws IOException {
    List<String> allSegments = controllerRestApi.getAllSegments("OFFLINE");
    Set<String> uniqueSegmentPrefixes = new HashSet<>();

    // Get all relevant segment prefixes that we are planning on pushing
    List<Path> segmentsToPushPaths = getDataFilePaths(_segmentPattern);
    List<String> segmentsToPushNames = segmentsToPushPaths.stream().map(s -> s.getName()).collect(Collectors.toList());
    for (String segmentName : segmentsToPushNames) {
      String segmentNamePrefix = removeSequenceId(segmentName);
      uniqueSegmentPrefixes.add(segmentNamePrefix);
    }

    List<String> relevantSegments = new ArrayList<>();
    // Get relevant segments already pushed that we are planning on refreshing
    for (String segmentName : allSegments) {
      if (uniqueSegmentPrefixes.contains(removeSequenceId(segmentName))) {
        relevantSegments.add(segmentName);
      }
    }

    relevantSegments.removeAll(segmentsToPushNames);
    controllerRestApi.pushSegments(fileSystem, getDataFilePaths(_segmentPattern));
    controllerRestApi.deleteSegmentUris(relevantSegments);
  }

  /**
   * Remove trailing sequence id
   * eg: If segment name is mytable_12, it will return mytable_
   * If segment name is mytable_20190809_20190809_12, it will return mytable_20190809_20190809_
   * @param segmentName
   * @return
   */
  private String removeSequenceId(String segmentName) {
    return segmentName.replaceAll("\\d*$", "");
  }

  protected ControllerRestApi getControllerRestApi() {
    return new DefaultControllerRestApi(_pushLocations, _rawTableName);
  }
}
