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
package org.apache.pinot.segment.local.data.manager;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Encapsulates the response to get list of segments that need to be refreshed. The response also contains the reason
 * why a segment has to be refreshed.
 */
public class NeedRefreshResponse {
  private final String _segmentName;
  private final boolean _needRefresh;
  private final String _reason;

  @JsonCreator
  public NeedRefreshResponse(@JsonProperty("segmentName") String segmentName, @JsonProperty("reason") String reason) {
    _segmentName = segmentName;
    _needRefresh = true;
    _reason = reason;
  }

  public NeedRefreshResponse(String segmentName, boolean needRefresh, String reason) {
    _segmentName = segmentName;
    _needRefresh = needRefresh;
    _reason = reason;
  }

  @JsonProperty
  public String getSegmentName() {
    return _segmentName;
  }

  @JsonIgnore
  public boolean isNeedRefresh() {
    return _needRefresh;
  }

  @JsonProperty
  public String getReason() {
    return _reason;
  }
}
