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
package org.apache.pinot.common.restlet.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * A simple container class to hold disk usage information.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DiskUsageInfo {
  private final String _instanceId;
  private final String _path;
  private final long _totalSpaceBytes;
  private final long _usedSpaceBytes;
  private final long _lastUpdatedTimeInEpochMs;

  public DiskUsageInfo(@JsonProperty("instanceId") String instanceId) {
    _instanceId = instanceId;
    _path = null;
    _totalSpaceBytes = -1;
    _usedSpaceBytes = -1;
    _lastUpdatedTimeInEpochMs = -1;
  }

  @JsonCreator
  public DiskUsageInfo(@JsonProperty("instanceId") String instanceId, @JsonProperty("path") String path,
      @JsonProperty("totalSpaceBytes") long totalSpaceBytes,
      @JsonProperty("usedSpaceBytes") long usedSpaceBytes,
      @JsonProperty("lastUpdatedTimeInEpochMs") long lastUpdatedTimeInEpochMs) {
    _instanceId = instanceId;
    _path = path;
    _totalSpaceBytes = totalSpaceBytes;
    _usedSpaceBytes = usedSpaceBytes;
    _lastUpdatedTimeInEpochMs = lastUpdatedTimeInEpochMs;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String getPath() {
    return _path;
  }

  public long getTotalSpaceBytes() {
    return _totalSpaceBytes;
  }

  public long getUsedSpaceBytes() {
    return _usedSpaceBytes;
  }

  public long getLastUpdatedTimeInEpochMs() {
    return _lastUpdatedTimeInEpochMs;
  }

  public String toString() {
    return "DiskUsageInfo{" + "_instanceId='" + _instanceId + '\'' + ", _path='" + _path + '\'' + ", _totalSpaceBytes="
        + _totalSpaceBytes + ", _usedSpaceBytes=" + _usedSpaceBytes + ", _lastUpdatedTimeInEpochMs="
        + _lastUpdatedTimeInEpochMs + '}';
  }
}
