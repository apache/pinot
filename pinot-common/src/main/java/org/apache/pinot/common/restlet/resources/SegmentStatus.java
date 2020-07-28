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

/**
 * Holds segment last reload time status along with any errors for a segment with unsuccessful call to get reload times.
 */
public class SegmentStatus {
  public String _segmentName;
  public String _segmentReloadTime;
  public String _segmentReloadStatusMessage;

  public SegmentStatus() {
  }

  public SegmentStatus(String segmentName, String segmentReloadTime, String segmentReloadStatusMessage) {
    _segmentName = segmentName;
    _segmentReloadTime = segmentReloadTime;
    _segmentReloadStatusMessage = segmentReloadStatusMessage;
  }

  @Override
  public String toString() {
    return "{ segmentName: " + _segmentName + ", segmentReloadTime: " + _segmentReloadTime +
        ", segmentReloadStatusMessage: " + _segmentReloadStatusMessage + " }";
  }
}
