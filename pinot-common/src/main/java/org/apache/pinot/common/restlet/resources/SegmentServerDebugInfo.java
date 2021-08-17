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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


/**
 * This class represents the server-side debug information for a segment.
 * NOTE: Debug classes are not expected to maintain backward compatibility,
 * and should not be exposed to the client side.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"segmentName", "segmentSize", "consumerInfo", "errorInfo"}) // For readability of JSON output
public class SegmentServerDebugInfo {

  private final String _segmentName;
  private final String _segmentSize; // Segment Size in Human readable format

  private final SegmentConsumerInfo _consumerInfo;
  private final SegmentErrorInfo _errorInfo;

  @JsonCreator
  public SegmentServerDebugInfo(@JsonProperty("segmentName") String segmentName,
      @JsonProperty("segmentSize") String segmentSize, @JsonProperty("consumerInfo") SegmentConsumerInfo consumerInfo,
      @JsonProperty("errorInfo") SegmentErrorInfo errorInfo) {
    _segmentName = segmentName;
    _segmentSize = segmentSize;
    _consumerInfo = consumerInfo;
    _errorInfo = errorInfo;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public String getSegmentSize() {
    return _segmentSize;
  }

  public SegmentConsumerInfo getConsumerInfo() {
    return _consumerInfo;
  }

  public SegmentErrorInfo getErrorInfo() {
    return _errorInfo;
  }
}
