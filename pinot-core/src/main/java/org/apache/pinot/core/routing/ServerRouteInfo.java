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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Class representing the route information for a server.
 * It contains the list of segments and optional segments assigned to the server.
 */
public class ServerRouteInfo {
  private final List<String> _segments;
  private final List<String> _optionalSegments;

  /**
   * Constructor for ServerRouteInfo.
   *
   * @param segments List of segments assigned to the server.
   * @param optionalSegments List of optional segments assigned to the server.
   */
  public ServerRouteInfo(
      @JsonProperty("segmentList") List<String> segments,
      @JsonProperty("optionalSegmentList") List<String> optionalSegments) {
    _segments = segments;
    _optionalSegments = optionalSegments;
  }

  /**
   * Gets the list of segments assigned to the server.
   *
   * @return List of segments.
   */
  public List<String> getSegments() {
    return _segments;
  }

  /**
   * Gets the list of optional segments assigned to the server.
   *
   * @return List of optional segments.
   */
  public List<String> getOptionalSegments() {
    return _optionalSegments;
  }
}
