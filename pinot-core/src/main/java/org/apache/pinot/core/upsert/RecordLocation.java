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

import com.google.common.base.Objects;


/**
 * Indicate a record's location on the local host.
 */
public class RecordLocation {
  private String _segmentName;
  private int _docId;
  private long _timestamp;

  public RecordLocation(String segmentName, int docId, long timestamp) {
    _segmentName = segmentName;
    _docId = docId;
    _timestamp = timestamp;
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RecordLocation that = (RecordLocation) o;
    return Objects.equal(_segmentName, that._segmentName) && Objects.equal(_docId, that._docId) && Objects
        .equal(_timestamp, that._timestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(_segmentName, _docId, _timestamp);
  }
}
