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

import java.util.Objects;

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
  public int hashCode() {
    int result = _segmentName != null ? _segmentName.hashCode() : 0;
    result = 31 * result + (_segmentReloadTime != null ? _segmentReloadTime.hashCode() : 0);
    return result;

  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SegmentStatus)) {
      return false;
    }

    SegmentStatus that = (SegmentStatus) obj;

    if (!_segmentReloadTime.equals(that._segmentReloadTime)) {
      return false;
    }
    return Objects.equals(_segmentName, that._segmentName);
  }

  @Override
  public String toString() {
    return "{ segmentName: " + _segmentName + ", segmentReloadTime: " + _segmentReloadTime + " }";
  }
}
