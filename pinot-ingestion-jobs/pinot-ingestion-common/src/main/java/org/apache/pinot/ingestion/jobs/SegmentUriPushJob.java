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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.DefaultControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.utils.PushLocation;


public class SegmentUriPushJob extends BaseSegmentJob {
  private final String _segmentUriPrefix;
  private final String _segmentUriSuffix;
  private final Path _segmentPattern;
  private final List<PushLocation> _pushLocations;
  private final String _rawTableName;

  public SegmentUriPushJob(Properties properties) {
    super(properties);
    _segmentUriPrefix = properties.getProperty("uri.prefix", "");
    _segmentUriSuffix = properties.getProperty("uri.suffix", "");
    _segmentPattern = Preconditions.checkNotNull(getPathFromProperty(JobConfigConstants.PATH_TO_OUTPUT));
    String[] hosts = StringUtils.split(properties.getProperty(JobConfigConstants.PUSH_TO_HOSTS), ',');
    int port = Integer.parseInt(properties.getProperty(JobConfigConstants.PUSH_TO_PORT));
    _pushLocations = PushLocation.getPushLocations(hosts, port);
    _rawTableName = Preconditions.checkNotNull(_properties.getProperty(JobConfigConstants.SEGMENT_TABLE_NAME));
  }

  @Override
  protected boolean isDataFile(String fileName) {
    return fileName.endsWith(JobConfigConstants.TAR_GZ_FILE_EXT);
  }

  public void run()
      throws Exception {
    try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
      List<Path> tarFilePaths = getDataFilePaths(_segmentPattern);
      List<String> segmentUris = new ArrayList<>(tarFilePaths.size());
      for (Path tarFilePath : tarFilePaths) {
        segmentUris.add(_segmentUriPrefix + tarFilePath.toUri().getRawPath() + _segmentUriSuffix);
      }
      controllerRestApi.sendSegmentUris(segmentUris);
    }
  }

  protected ControllerRestApi getControllerRestApi() {
    return new DefaultControllerRestApi(_pushLocations, _rawTableName);
  }
}
