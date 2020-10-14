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
package org.apache.pinot.core.upsert;

import java.util.Objects;


/**
 * Indicate a record's location on the local host.
 */
public class RecordLocation {
  private final String _segmentName;
  private final int _docId;
  private final long _timestamp;
  private final boolean _isConsuming;

  public RecordLocation(String segmentName, int docId, long timestamp, boolean isConsuming) {
    _segmentName = segmentName;
    _docId = docId;
    _timestamp = timestamp;
    _isConsuming = isConsuming;
  }

  public String getSegmentName() {
    return _segmentName;
  }

  public int getDocId() {
    return _docId;
  }

  public long getTimestamp() {
    return _timestamp;
  }

  public boolean isConsuming() {
    return _isConsuming;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RecordLocation)) {
      return false;
    }
    RecordLocation that = (RecordLocation) o;
    return _docId == that._docId && _timestamp == that._timestamp && _isConsuming == that._isConsuming && Objects
        .equals(_segmentName, that._segmentName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentName, _docId, _timestamp, _isConsuming);
  }
}
