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
import java.util.NoSuchElementException;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.SegmentMetadata;


/// Unmodifiable view of the physical columns of an immutable segment: the columns of its metadata whose field spec is
/// not produced by a virtual column provider, in natural column-name order.
///
/// It is a view rather than a copy so a segment retains nothing per column for it: the segment schema this replaces
/// held a `TreeMap` entry per column, and a cached `TreeSet` would hold the same. `contains` is one column lookup and
/// iteration is a filtered pass over the column metadata. The size is counted once at construction, from the same
/// column metadata the iteration reads, which is sound because the column metadata is fixed once the segment is
/// loaded (the loader registers the virtual columns before the segment is built).
///
/// Thread-safe for reads, like the underlying segment metadata once loaded.
final class PhysicalColumnNames extends AbstractSet<String> {
  private final SegmentMetadata _segmentMetadata;
  private final int _numPhysicalColumns;

  PhysicalColumnNames(SegmentMetadata segmentMetadata) {
    _segmentMetadata = segmentMetadata;
    int numPhysicalColumns = 0;
    for (ColumnMetadata columnMetadata : segmentMetadata.getAllColumnMetadata()) {
      if (isPhysical(columnMetadata)) {
        numPhysicalColumns++;
      }
    }
    _numPhysicalColumns = numPhysicalColumns;
  }

  private static boolean isPhysical(ColumnMetadata columnMetadata) {
    return !columnMetadata.getFieldSpec().isVirtualColumn();
  }

  @Override
  public boolean contains(Object o) {
    if (!(o instanceof String)) {
      return false;
    }
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor((String) o);
    return columnMetadata != null && isPhysical(columnMetadata);
  }

  @Override
  public int size() {
    return _numPhysicalColumns;
  }

  @Override
  public Iterator<String> iterator() {
    Iterator<ColumnMetadata> columnMetadata = _segmentMetadata.getAllColumnMetadata().iterator();
    return new Iterator<>() {
      private String _next = advance();

      private String advance() {
        while (columnMetadata.hasNext()) {
          ColumnMetadata next = columnMetadata.next();
          if (isPhysical(next)) {
            return next.getColumnName();
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
