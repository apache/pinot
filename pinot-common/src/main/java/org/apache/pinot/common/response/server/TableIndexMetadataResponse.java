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
package org.apache.pinot.common.response.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;


public class TableIndexMetadataResponse {
  private final long _totalOnlineSegments;
  private final Map<String, Map<String, Integer>> _columnToIndexesCount;

  @JsonCreator
  public TableIndexMetadataResponse(@JsonProperty("totalOnlineSegments") long totalOnlineSegments,
      @JsonProperty("columnToIndexesCount") Map<String, Map<String, Integer>> columnToIndexesCount) {
    _totalOnlineSegments = totalOnlineSegments;
    _columnToIndexesCount = columnToIndexesCount;
  }

  public long getTotalOnlineSegments() {
    return _totalOnlineSegments;
  }

  public Map<String, Map<String, Integer>> getColumnToIndexesCount() {
    return _columnToIndexesCount;
  }
}
