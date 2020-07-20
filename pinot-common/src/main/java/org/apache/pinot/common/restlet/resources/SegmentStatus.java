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
  public String segmentName;
  public String segmentReloadTime;

  public SegmentStatus() {
  }

  public SegmentStatus(String segmentName, String segmentReloadTime) {
    this.segmentName = segmentName;
    this.segmentReloadTime = segmentReloadTime;
  }

  @Override
  public int hashCode() {
    int result = segmentName != null ? segmentName.hashCode() : 0;
    result = 31 * result + (segmentReloadTime != null ? segmentReloadTime.hashCode() : 0);
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

    if (!segmentReloadTime.equals(that.segmentReloadTime)) {
      return false;
    }
    return Objects.equals(segmentName, that.segmentName);
  }

  @Override
  public String toString() {
    return "{ segmentName: " + segmentName + ", segmentReloadTime: " + segmentReloadTime + " }";
  }
}
