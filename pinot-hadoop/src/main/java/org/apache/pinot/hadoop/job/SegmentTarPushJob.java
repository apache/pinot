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
    List<Path> segmentsToPush = getDataFilePaths(_segmentPattern);
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      // TODO: Deal with invalid prefixes in the future

      List<String> currentSegments = controllerRestApi.getAllSegments("OFFLINE");

      controllerRestApi.pushSegments(fileSystem, segmentsToPush);

      if (_deleteExtraSegments) {
        controllerRestApi.deleteSegmentUris(getSegmentsToDelete(currentSegments, segmentsToPush));
      }
    }
  }

  /**
   * Deletes extra segments after pushing to the controller
   * @param allSegments all segments on the controller for the table
   * @param segmentsToPush segments that will be pushed to the controller
   * @throws IOException
   */
  public List<String> getSegmentsToDelete(List<String> allSegments, List<Path> segmentsToPush) {
    Set<String> uniqueSegmentPrefixes = new HashSet<>();

    // Get all relevant segment prefixes that we are planning on pushing
    List<String> segmentNamesToPush = segmentsToPush.stream().map(s -> s.getName()).collect(Collectors.toList());
    for (String segmentName : segmentNamesToPush) {
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

    relevantSegments.removeAll(segmentNamesToPush);
    return relevantSegments;
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
