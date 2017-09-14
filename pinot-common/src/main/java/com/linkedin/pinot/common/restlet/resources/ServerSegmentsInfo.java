/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class ServerSegmentsInfo {
  private String _serverName;
  private long _reportedNumOfSegments;
  private long _reportedSegmentsSizeInBytes;

  public ServerSegmentsInfo(String serverName) {
    this._serverName = serverName;
    //We set default value to -1 as indication of error in returning reported info from a Pinot server
    _reportedNumOfSegments = -1;
    _reportedSegmentsSizeInBytes = -1;
  }

  public String getServerName() {
    return _serverName;
  }

  public long getReportedNumOfSegments() {
    return _reportedNumOfSegments;
  }

  public void setReportedNumOfSegments(long reportedNumOfSegments) {
    this._reportedNumOfSegments = reportedNumOfSegments;
  }

  public long getReportedSegmentsSizeInBytes() {
    return _reportedSegmentsSizeInBytes;
  }

  public void setReportedSegmentsSizeInBytes(long reportedSegmentsSizeInBytes) {
    this._reportedSegmentsSizeInBytes = reportedSegmentsSizeInBytes;
  }
}