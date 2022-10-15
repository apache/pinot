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
import java.util.Iterator;
import java.util.List;
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
        return getRecordInfo(recordInfoReader, _docId++);
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
        return getRecordInfo(recordInfoReader, _docIdIterator.next());
      }
    };
  }

  /**
   * Reads a {@link RecordInfo} from the segment.
   */
  public static RecordInfo getRecordInfo(RecordInfoReader recordInfoReader, int docId) {
    PrimaryKey primaryKey = new PrimaryKey(new Object[recordInfoReader.getNumPrimaryKeys()]);
    getPrimaryKey(recordInfoReader._primaryKeyReader, docId, primaryKey);
    Object comparisonValue = getValue(recordInfoReader._comparisonColumnReader, docId);
    return new RecordInfo(primaryKey, docId, (Comparable) comparisonValue);
  }

  /**
   * Reads a primary key from the segment.
   */
  public static void getPrimaryKey(PrimaryKeyReader primaryKeyReader, int docId, PrimaryKey buffer) {
    Object[] values = buffer.getValues();
    int numPrimaryKeyColumns = values.length;
    for (int i = 0; i < numPrimaryKeyColumns; i++) {
      values[i] = getValue(primaryKeyReader._primaryKeyColumnReaders.get(i), docId);
    }
  }

  private static Object getValue(PinotSegmentColumnReader columnReader, int docId) {
    Object value = columnReader.getValue(docId);
    return value instanceof byte[] ? new ByteArray((byte[]) value) : value;
  }

  public static class RecordInfoReader implements Closeable {
    public final PrimaryKeyReader _primaryKeyReader;
    public final PinotSegmentColumnReader _comparisonColumnReader;

    public RecordInfoReader(IndexSegment segment, List<String> primaryKeyColumns, String comparisonColumn) {
      _primaryKeyReader = new PrimaryKeyReader(segment, primaryKeyColumns);
      _comparisonColumnReader = new PinotSegmentColumnReader(segment, comparisonColumn);
    }

    public int getNumPrimaryKeys() {
      return _primaryKeyReader.getNumPrimaryKeys();
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

    public int getNumPrimaryKeys() {
      return _primaryKeyColumnReaders.size();
    }

    @Override
    public void close()
        throws IOException {
      for (PinotSegmentColumnReader primaryKeyColumnReader : _primaryKeyColumnReaders) {
        primaryKeyColumnReader.close();
      }
    }
  }
}
