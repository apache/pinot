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
package org.apache.pinot.segment.local.indexsegment.immutable;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import org.apache.pinot.segment.spi.ColumnMetadata;


/// Unmodifiable view of the physical columns of an immutable segment: the keys of its column metadata map whose field
/// spec is not produced by a virtual column provider, in the map's (sorted) key order.
///
/// It is a view rather than a copy so a segment retains nothing per column for it: the segment schema this replaces
/// held a `TreeMap` entry per column, and a cached `TreeSet` would hold the same. `contains` is one map lookup and
/// iteration is a filtered pass over the map. The virtual column count is taken once at construction, which is sound
/// because the column metadata map is fixed once the segment is loaded.
///
/// Thread-safe for reads, like the underlying map once loaded.
final class PhysicalColumnNames extends AbstractSet<String> {
  private final SortedMap<String, ColumnMetadata> _columnMetadataMap;
  private final int _numVirtualColumns;

  PhysicalColumnNames(SortedMap<String, ColumnMetadata> columnMetadataMap) {
    _columnMetadataMap = columnMetadataMap;
    int numVirtualColumns = 0;
    for (ColumnMetadata columnMetadata : columnMetadataMap.values()) {
      if (!isPhysical(columnMetadata)) {
        numVirtualColumns++;
      }
    }
    _numVirtualColumns = numVirtualColumns;
  }

  private static boolean isPhysical(ColumnMetadata columnMetadata) {
    return !columnMetadata.getFieldSpec().isVirtualColumn();
  }

  @Override
  public boolean contains(Object o) {
    if (!(o instanceof String)) {
      return false;
    }
    ColumnMetadata columnMetadata = _columnMetadataMap.get(o);
    return columnMetadata != null && isPhysical(columnMetadata);
  }

  @Override
  public int size() {
    return _columnMetadataMap.size() - _numVirtualColumns;
  }

  @Override
  public Iterator<String> iterator() {
    Iterator<Map.Entry<String, ColumnMetadata>> entries = _columnMetadataMap.entrySet().iterator();
    return new Iterator<>() {
      private String _next = advance();

      private String advance() {
        while (entries.hasNext()) {
          Map.Entry<String, ColumnMetadata> entry = entries.next();
          if (isPhysical(entry.getValue())) {
            return entry.getKey();
          }
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return _next != null;
      }

      @Override
      public String next() {
        String next = _next;
        if (next == null) {
          throw new NoSuchElementException();
        }
        _next = advance();
        return next;
      }
    };
  }
}
