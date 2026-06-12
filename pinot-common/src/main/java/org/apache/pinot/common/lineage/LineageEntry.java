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
package org.apache.pinot.common.lineage;

import java.util.List;
import java.util.Objects;


/**
 * Class to represent the lineage entry.
 *
 */
public class LineageEntry {
  private final List<String> _segmentsFrom;
  private final List<String> _segmentsTo;
  private final LineageEntryState _state;
  private final long _timestamp;
  // Currently, lineage needs to be marked completed by clients be calling {@code endReplaceSegments}.
  // For few use cases, we can simplify the client logic by allowing an observer (e.g. a controller-side periodic task)
  // to mark lineage entries completed automatically once all the To segments are ONLINE in the external view.
  // This field allows us to opt in to this behavior on a per-entry basis.
  // Default false preserves client-driven completion semantics.
  private final boolean _autoCompleteLineageEntry;

  public LineageEntry(List<String> segmentsFrom, List<String> segmentsTo, LineageEntryState state, long timestamp) {
    this(segmentsFrom, segmentsTo, state, timestamp, false);
  }

  public LineageEntry(List<String> segmentsFrom, List<String> segmentsTo, LineageEntryState state, long timestamp,
      boolean autoCompleteLineageEntry) {
    _segmentsFrom = segmentsFrom;
    _segmentsTo = segmentsTo;
    _state = state;
    _timestamp = timestamp;
    _autoCompleteLineageEntry = autoCompleteLineageEntry;
  }

  public List<String> getSegmentsFrom() {
    return _segmentsFrom;
  }

  public List<String> getSegmentsTo() {
    return _segmentsTo;
  }

  public LineageEntryState getState() {
    return _state;
  }

  public long getTimestamp() {
    return _timestamp;
  }

  public boolean isAutoCompleteLineageEntry() {
    return _autoCompleteLineageEntry;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LineageEntry that = (LineageEntry) o;
    return _timestamp == that._timestamp && _autoCompleteLineageEntry == that._autoCompleteLineageEntry
        && _segmentsFrom.equals(that._segmentsFrom) && _segmentsTo.equals(that._segmentsTo) && _state == that._state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_segmentsFrom, _segmentsTo, _state, _timestamp, _autoCompleteLineageEntry);
  }
}
