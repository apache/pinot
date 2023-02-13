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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
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
  public static Iterator<RecordInfo> getRecordInfoIterator(RecordInfoReader recordInfoReader, int numDocs) {
    return new Iterator<RecordInfo>() {
      private int _docId = 0;

      @Override
      public boolean hasNext() {
        return _docId < numDocs;
      }

      @Override
      public RecordInfo next() {
        return recordInfoReader.getRecordInfo(_docId++);
      }
    };
  }

  /**
   * Returns an iterator of {@link RecordInfo} for the valid documents from the segment.
   */
  public static Iterator<RecordInfo> getRecordInfoIterator(RecordInfoReader recordInfoReader,
      MutableRoaringBitmap validDocIds) {
    return new Iterator<RecordInfo>() {
      private final PeekableIntIterator _docIdIterator = validDocIds.getIntIterator();

      @Override
      public boolean hasNext() {
        return _docIdIterator.hasNext();
      }

      @Override
      public RecordInfo next() {
        return recordInfoReader.getRecordInfo(_docIdIterator.next());
      }
    };
  }

  public static class RecordInfoReader implements Closeable {
    public final PrimaryKeyReader _primaryKeyReader;
    public final ComparisonColumnReader _comparisonColumnReader;

    public RecordInfoReader(IndexSegment segment, List<String> primaryKeyColumns, List<String> comparisonColumns) {
      _primaryKeyReader = new PrimaryKeyReader(segment, primaryKeyColumns);
      _comparisonColumnReader = new ComparisonColumnReader(segment, comparisonColumns);
    }

    public RecordInfo getRecordInfo(int docId) {
      PrimaryKey primaryKey = _primaryKeyReader.getPrimaryKey(docId);
      return new RecordInfo(primaryKey, docId, _comparisonColumnReader.getComparisonColumns(docId));
    }

    @Override
    public void close()
        throws IOException {
      _primaryKeyReader.close();
      _comparisonColumnReader.close();
    }
  }

  public static class PrimaryKeyReader implements Closeable {
    public final List<PinotSegmentColumnReader> _primaryKeyColumnReaders;

    public PrimaryKeyReader(IndexSegment segment, List<String> primaryKeyColumns) {
      _primaryKeyColumnReaders = new ArrayList<>(primaryKeyColumns.size());
      for (String primaryKeyColumn : primaryKeyColumns) {
        _primaryKeyColumnReaders.add(new PinotSegmentColumnReader(segment, primaryKeyColumn));
      }
    }

    public PrimaryKey getPrimaryKey(int docId) {
      int numPrimaryKeys = _primaryKeyColumnReaders.size();
      Object[] values = new Object[numPrimaryKeys];
      for (int i = 0; i < numPrimaryKeys; i++) {
        values[i] = getValue(_primaryKeyColumnReaders.get(i), docId);
      }
      return new PrimaryKey(values);
    }

    public void getPrimaryKey(int docId, PrimaryKey buffer) {
      Object[] values = buffer.getValues();
      int numPrimaryKeys = values.length;
      for (int i = 0; i < numPrimaryKeys; i++) {
        values[i] = getValue(_primaryKeyColumnReaders.get(i), docId);
      }
    }

    @Override
    public void close()
        throws IOException {
      for (PinotSegmentColumnReader primaryKeyColumnReader : _primaryKeyColumnReaders) {
        primaryKeyColumnReader.close();
      }
    }
  }

  public static class ComparisonColumnReader implements Closeable {
    public final Map<String, PinotSegmentColumnReader> _comparisonColumnReaders;

    public ComparisonColumnReader(IndexSegment segment, List<String> comparisonColumns) {
      _comparisonColumnReaders = new HashMap<>();
      for (String comparisonColumn : comparisonColumns) {
        _comparisonColumnReaders.put(comparisonColumn, new PinotSegmentColumnReader(segment, comparisonColumn));
      }
    }

    public ComparisonColumns getComparisonColumns(int docId) {
      Map<String, ComparisonValue> comparisonColumns = new HashMap<>();

      for (String comparisonColumnName : _comparisonColumnReaders.keySet()) {
        PinotSegmentColumnReader columnReader = _comparisonColumnReaders.get(comparisonColumnName);
        Comparable comparisonValue = (Comparable) getValue(columnReader, docId);

        comparisonColumns.put(comparisonColumnName,
            new ComparisonValue(comparisonColumnName, comparisonValue, columnReader.isNull(docId)));

      }
      return new ComparisonColumns(comparisonColumns);
    }

    @Override
    public void close()
        throws IOException {
      for (PinotSegmentColumnReader comparisonColumnReader : _comparisonColumnReaders.values()) {
        comparisonColumnReader.close();
      }
    }
  }

  private static Object getValue(PinotSegmentColumnReader columnReader, int docId) {
    Object value = columnReader.getValue(docId);
    return value instanceof byte[] ? new ByteArray((byte[]) value) : value;
  }
}
