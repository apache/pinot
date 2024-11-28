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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import org.apache.pinot.segment.local.data.manager.StaleSegment;


public class TableStaleSegmentResponse {
  private final List<StaleSegment> _staleSegmentList;
  private final boolean _isValidResponse;
  private final String _errorMessage;

  @JsonCreator
  public TableStaleSegmentResponse(@JsonProperty("staleSegmentList") List<StaleSegment> staleSegmentList,
      @JsonProperty("validResponse") boolean isValidResponse,
      @JsonProperty("errorMessage") String errorMessage) {
    _staleSegmentList = staleSegmentList;
    _isValidResponse = isValidResponse;
    _errorMessage = errorMessage;
  }

  public TableStaleSegmentResponse(List<StaleSegment> staleSegmentList) {
    _staleSegmentList = staleSegmentList;
    _isValidResponse = true;
    _errorMessage = null;
  }

  public TableStaleSegmentResponse(String errorMessage) {
    _staleSegmentList = null;
    _isValidResponse = false;
    _errorMessage = errorMessage;
  }

  @JsonProperty
  public List<StaleSegment> getStaleSegmentList() {
    return _staleSegmentList;
  }

  @JsonProperty
  public boolean isValidResponse() {
    return _isValidResponse;
  }

  @JsonProperty
  public String getErrorMessage() {
    return _errorMessage;
  }
}
