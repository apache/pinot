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


@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentSizeInfo {
  public String segmentName;
  public long diskSizeInBytes = -1;

  public SegmentSizeInfo() {

  }

  public SegmentSizeInfo(String segmentName, long sizeBytes) {
    this.segmentName = segmentName;
    this.diskSizeInBytes = sizeBytes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SegmentSizeInfo)) {
      return false;
    }

    SegmentSizeInfo that = (SegmentSizeInfo) o;

    if (diskSizeInBytes != that.diskSizeInBytes) {
      return false;
    }
    return segmentName != null ? segmentName.equals(that.segmentName) : that.segmentName == null;
  }

  @Override
  public int hashCode() {
    int result = segmentName != null ? segmentName.hashCode() : 0;
    result = 31 * result + (int) (diskSizeInBytes ^ (diskSizeInBytes >>> 32));
    return result;
  }
}
