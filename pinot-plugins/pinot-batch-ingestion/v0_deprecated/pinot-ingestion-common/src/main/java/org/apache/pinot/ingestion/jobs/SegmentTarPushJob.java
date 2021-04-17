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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.DefaultControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.utils.PushLocation;


public class SegmentTarPushJob extends BaseSegmentJob {
  protected final String _segmentPattern;
  protected final List<PushLocation> _pushLocations;
  protected final String _rawTableName;
  protected final boolean _deleteExtraSegments;

  public SegmentTarPushJob(Properties properties) {
    super(properties);
    _segmentPattern = Preconditions.checkNotNull(properties.getProperty(JobConfigConstants.PATH_TO_OUTPUT),
        String.format("Config: %s is missing in job property file.", JobConfigConstants.PATH_TO_OUTPUT));
    String[] hosts =
        StringUtils.split(Preconditions.checkNotNull(properties.getProperty(JobConfigConstants.PUSH_TO_HOSTS),
            String.format("Config: %s is missing in job property file.", JobConfigConstants.PUSH_TO_HOSTS)), ',');
    int port = Integer.parseInt(Preconditions.checkNotNull(properties.getProperty(JobConfigConstants.PUSH_TO_PORT),
        String.format("Config: %s is missing in job property file.", JobConfigConstants.PUSH_TO_PORT)));
    _pushLocations = PushLocation.getPushLocations(hosts, port);
    _rawTableName = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME));
    _deleteExtraSegments =
        Boolean.parseBoolean(properties.getProperty(JobConfigConstants.DELETE_EXTRA_SEGMENTS, "false"));
  }

  @Override
  protected boolean isDataFile(String fileName) {
    return fileName.endsWith(JobConfigConstants.TAR_GZ_FILE_EXT);
  }

  public void run() throws Exception {
    FileSystem fileSystem = FileSystem.get(new Path(_segmentPattern).toUri(), getConf());
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
