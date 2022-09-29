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
package org.apache.pinot.segment.local.upsert;

import java.util.Iterator;
import java.util.List;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.data.readers.PrimaryKey;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


@SuppressWarnings("rawtypes")
public class UpsertUtils {
  private UpsertUtils() {
  }

  /**
   * Returns an iterator of {@link RecordInfo} for all the documents from the segment.
   */
  public static Iterator<RecordInfo> getRecordInfoIterator(ImmutableSegment segment, List<String> primaryKeyColumns,
      String comparisonColumn) {
    int numTotalDocs = segment.getSegmentMetadata().getTotalDocs();
    return new Iterator<RecordInfo>() {
      private int _docId = 0;

      @Override
      public boolean hasNext() {
        return _docId < numTotalDocs;
      }

      @Override
      public RecordInfo next() {
        return getRecordInfo(segment, primaryKeyColumns, comparisonColumn, _docId++);
      }
    };
  }

  /**
   * Returns an iterator of {@link RecordInfo} for the valid documents from the segment.
   */
  public static Iterator<RecordInfo> getRecordInfoIterator(ImmutableSegment segment, List<String> primaryKeyColumns,
      String comparisonColumn, MutableRoaringBitmap validDocIds) {
    return new Iterator<RecordInfo>() {
      private final PeekableIntIterator _docIdIterator = validDocIds.getIntIterator();

      @Override
      public boolean hasNext() {
        return _docIdIterator.hasNext();
      }

      @Override
      public RecordInfo next() {
        return getRecordInfo(segment, primaryKeyColumns, comparisonColumn, _docIdIterator.next());
      }
    };
  }

  /**
   * Reads a {@link RecordInfo} from the segment.
   */
  public static RecordInfo getRecordInfo(ImmutableSegment segment, List<String> primaryKeyColumns,
      String comparisonColumn, int docId) {
    PrimaryKey primaryKey = new PrimaryKey(new Object[primaryKeyColumns.size()]);
    getPrimaryKey(segment, primaryKeyColumns, docId, primaryKey);
    Object comparisonValue = segment.getValue(docId, comparisonColumn);
    if (comparisonValue instanceof byte[]) {
      comparisonValue = new ByteArray((byte[]) comparisonValue);
    }
    return new RecordInfo(primaryKey, docId, (Comparable) comparisonValue);
  }

  /**
   * Reads a primary key from the segment.
   */
  public static void getPrimaryKey(IndexSegment segment, List<String> primaryKeyColumns, int docId, PrimaryKey buffer) {
    Object[] values = buffer.getValues();
    int numPrimaryKeyColumns = values.length;
    for (int i = 0; i < numPrimaryKeyColumns; i++) {
      Object value = segment.getValue(docId, primaryKeyColumns.get(i));
      if (value instanceof byte[]) {
        value = new ByteArray((byte[]) value);
      }
      values[i] = value;
    }
  }
}
