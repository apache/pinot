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
public class ServerSegmentInfo {
  private String _serverName;
  private long _segmentCount;
  private long _segmentSizeInBytes;

  public ServerSegmentInfo(String serverName) {
    this._serverName = serverName;
    //We set default value to -1 as indication of error (e.g., timeout) in returning segment info from a Pinot server
    _segmentCount = -1;
    _segmentSizeInBytes = -1;
  }

  public String getServerName() {
    return _serverName;
  }

  public long getSegmentCount() {
    return _segmentCount;
  }

  public void setSegmentCount(long segmentCount) {
    this._segmentCount = segmentCount;
  }

  public long getSegmentSizeInBytes() {
    return _segmentSizeInBytes;
  }

  public void setSegmentSizeInBytes(long segmentSizeInBytes) {
    this._segmentSizeInBytes = segmentSizeInBytes;
  }
}