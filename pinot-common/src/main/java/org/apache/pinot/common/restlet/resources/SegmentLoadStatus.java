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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Holds segment last reload time status along with any errors for a segment with unsuccessful call to get reload times.
 *
 * NOTE: This class is being used in both the controller and the server. There is tight coupling between them.
 * So, the API contract cannot be changed without changing or refactoring this class.
 *
 * TODO: refactor this class to be handled better. Make sure to have an extensible design that helps add more
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentLoadStatus {
  // Name of the segment itself
  public String _segmentName;
  // The last segment reload time in ISO date format (yyyy-MM-dd HH:mm:ss:SSS UTC)
  // If the segment reload failed for a segment, then the value will be the previous segment reload was successful
  public String _segmentReloadTimeUTC;
  // If a segment load failed, then a status message is to be set - currently not done
  // TODO: add message description to show why call to fetch reload status has errors
  public String _segmentReloadStatusMessage;

  public SegmentLoadStatus() {
  }

  public SegmentLoadStatus(String segmentName, String segmentReloadTimeUTC, String segmentReloadStatusMessage) {
    _segmentName = segmentName;
    _segmentReloadTimeUTC = segmentReloadTimeUTC;
    _segmentReloadStatusMessage = segmentReloadStatusMessage;
  }

  @Override
  public String toString() {
    return "{ segmentName: " + _segmentName + ", segmentReloadTime: " + _segmentReloadTimeUTC +
        ", segmentReloadStatusMessage: " + _segmentReloadStatusMessage + " }";
  }
}
