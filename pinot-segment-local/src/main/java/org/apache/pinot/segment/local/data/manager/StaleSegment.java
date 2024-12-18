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
 * Encapsulates information for a stale segment. It captures segment name, staleness and reason if it is stale.
 */
public class StaleSegment {
  private final String _segmentName;
  private final boolean _isStale;
  private final String _reason;

  @JsonCreator
  public StaleSegment(@JsonProperty("segmentName") String segmentName, @JsonProperty("reason") String reason) {
    _segmentName = segmentName;
    _isStale = true;
    _reason = reason;
  }

  public StaleSegment(String segmentName, boolean isStale, String reason) {
    _segmentName = segmentName;
    _isStale = isStale;
    _reason = reason;
  }

  @JsonProperty
  public String getSegmentName() {
    return _segmentName;
  }

  @JsonIgnore
  public boolean isStale() {
    return _isStale;
  }

  @JsonProperty
  public String getReason() {
    return _reason;
  }
}
