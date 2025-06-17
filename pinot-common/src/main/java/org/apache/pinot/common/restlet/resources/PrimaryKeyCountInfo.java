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
import java.util.Collections;
import java.util.Set;


@JsonIgnoreProperties(ignoreUnknown = true)
public class PrimaryKeyCountInfo {
  private final String _instanceId;
  private final long _numPrimaryKeys;
  private final Set<String> _tablesWithPrimaryKeys;
  private final long _lastUpdatedTimeInEpochMs;

  public PrimaryKeyCountInfo(@JsonProperty("instanceId") String instanceId) {
    _instanceId = instanceId;
    _numPrimaryKeys = -1;
    _tablesWithPrimaryKeys = Collections.emptySet();
    _lastUpdatedTimeInEpochMs = -1;
  }

  @JsonCreator
  public PrimaryKeyCountInfo(@JsonProperty("instanceId") String instanceId,
      @JsonProperty("numPrimaryKeys") long numPrimaryKeys,
      @JsonProperty("tablesWithPrimaryKeys") Set<String> tablesWithPrimaryKeys,
      @JsonProperty("lastUpdatedTimeInEpochMs") long lastUpdatedTimeInEpochMs) {
    _instanceId = instanceId;
    _numPrimaryKeys = numPrimaryKeys;
    _tablesWithPrimaryKeys = tablesWithPrimaryKeys;
    _lastUpdatedTimeInEpochMs = lastUpdatedTimeInEpochMs;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public long getNumPrimaryKeys() {
    return _numPrimaryKeys;
  }

  public Set<String> getTablesWithPrimaryKeys() {
    return _tablesWithPrimaryKeys;
  }

  public long getLastUpdatedTimeInEpochMs() {
    return _lastUpdatedTimeInEpochMs;
  }

  public String toString() {
    return "PrimaryKeyCountInfo{" + "_instanceId='" + _instanceId + ", _numPrimaryKeys=" + _numPrimaryKeys
        + ", _tablesWithPrimaryKeys=" + _tablesWithPrimaryKeys + ", _lastUpdatedTimeInEpochMs="
        + _lastUpdatedTimeInEpochMs + '}';
  }
}
