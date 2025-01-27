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
package org.apache.pinot.core.routing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Class representing the execution information for a server.
 * It contains the list of segments and optional segments assigned to the server.
 */
public class ServerExecutionInfo {
  private final List<String> _segmentList;
  private final List<String> _optionalSegmentList;

  /**
   * Constructor for ServerExecutionInfo.
   *
   * @param segmentList List of segments assigned to the server.
   * @param optionalSegmentList List of optional segments assigned to the server.
   */
  @JsonCreator
  public ServerExecutionInfo(
      @JsonProperty("segmentList") List<String> segmentList,
      @JsonProperty("optionalSegmentList") List<String> optionalSegmentList) {
    _segmentList = segmentList;
    _optionalSegmentList = optionalSegmentList;
  }

  /**
   * Gets the list of segments assigned to the server.
   *
   * @return List of segments.
   */
  @JsonProperty("segmentList")
  public List<String> getSegmentList() {
    return _segmentList;
  }

  /**
   * Gets the list of optional segments assigned to the server.
   *
   * @return List of optional segments.
   */
  @JsonProperty("optionalSegmentList")
  public List<String> getOptionalSegmentList() {
    return _optionalSegmentList;
  }
}
