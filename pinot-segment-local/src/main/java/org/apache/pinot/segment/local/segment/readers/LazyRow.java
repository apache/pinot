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
package org.apache.pinot.segment.local.segment.readers;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.IndexSegment;


/**
 * <p>A wrapper class to read column values of a row for a given {@link IndexSegment} and docId.<br>
 * The advantage of having wrapper over segment and docId is column values are read only when
 * {@link LazyRow#getValue(String)} is invoked.
 * This is useful to reduce the disk reads incurred due to loading the complete previous row during merge step.
 *
 * <p>The LazyRow has an internal state and should not be used concurrently. To reuse the LazyRow, create an instance
 * using no arg constructor and re-initialise using {@link LazyRow#init(IndexSegment, int)}
 */
public class LazyRow {
  private final Map<String, Object> _fieldToValueMap = new HashMap<>();
  private final Set<String> _nullValueFields = new HashSet<>();
  private IndexSegment _segment;
  private int _docId;

  public LazyRow() {
  }

  public void init(IndexSegment segment, int docId) {
    clear();
    _segment = segment;
    _docId = docId;
  }

  /**
   * Computes a field's value in an indexed row.
   * @param fieldName
   * @return Returns value or null for persisted null values
   */
  @Nullable
  public Object getValue(String fieldName) {

    // if field's value was previously read as null, return null
    if (_nullValueFields.contains(fieldName)) {
      return null;
    }
    if (_segment == null) {
      throw new IllegalStateException("Index segment for Lazy row is uninitialized.");
    }

    // compute the _fieldToValueMap or _nullValueFields based on the indexed value
    return _fieldToValueMap.computeIfAbsent(fieldName, col -> {
      Object value = null;
      try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(_segment, col)) {
        if (!columnReader.isNull(_docId)) {
          value = columnReader.getValue(_docId);
        } else {
          _nullValueFields.add(fieldName);
        }
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Caught exception while closing pinotSegmentColumnReader for fieldName: %s", fieldName), e);
      }
      return value;
    });
  }

  public boolean isNullValue(String fieldName) {
    return _nullValueFields.contains(fieldName) || getValue(fieldName) == null;
  }

  public void clear() {
    _fieldToValueMap.clear();
    _nullValueFields.clear();
  }

  public Set<String> getColumnNames() {
    if (_segment == null) {
      throw new IllegalStateException("Index segment for Lazy row is uninitialized.");
    }
    return _segment.getColumnNames();
  }
}
