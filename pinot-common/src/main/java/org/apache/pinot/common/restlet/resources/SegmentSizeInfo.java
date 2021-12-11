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


@JsonIgnoreProperties(ignoreUnknown = true)
public class SegmentSizeInfo {
  private final String _segmentName;
  private final long _diskSizeInBytes;

  @JsonCreator
  public SegmentSizeInfo(@JsonProperty("segmentName") String segmentName,
      @JsonProperty("diskSizeInBytes") long sizeBytes) {
    _segmentName = segmentName;
    _diskSizeInBytes = sizeBytes;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public long getDiskSizeInBytes() {
    return _diskSizeInBytes;
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

    if (_diskSizeInBytes != that._diskSizeInBytes) {
      return false;
    }
    return _segmentName != null ? _segmentName.equals(that._segmentName) : that._segmentName == null;
  }

  @Override
  public int hashCode() {
    int result = _segmentName != null ? _segmentName.hashCode() : 0;
    result = 31 * result + (int) (_diskSizeInBytes ^ (_diskSizeInBytes >>> 32));
    return result;
  }
}
