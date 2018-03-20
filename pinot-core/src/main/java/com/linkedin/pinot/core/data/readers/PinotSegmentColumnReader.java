/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


@SuppressWarnings("unchecked")
public class PinotSegmentColumnReader {
  private final Dictionary _dictionary;
  private final DataFileReader _reader;
  private final ReaderContext _readerContext;
  private final int[] _mvBuffer;
  private final boolean _isColumnSorted;

  public PinotSegmentColumnReader(IndexSegmentImpl indexSegment, String columnName) {
    _dictionary = indexSegment.getDictionaryFor(columnName);
    _reader = indexSegment.getForwardIndexReaderFor(columnName);
    _readerContext = _reader.createContext();
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) indexSegment.getSegmentMetadata();
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(columnName);
    if (columnMetadata.isSingleValue()) {
      _mvBuffer = null;
    } else {
      _mvBuffer = new int[columnMetadata.getMaxNumberOfMultiValues()];
    }
    _isColumnSorted = columnMetadata.isSorted();
  }

  public Object readInt(int docId) {
    SingleColumnSingleValueReader svReader = (SingleColumnSingleValueReader) _reader;
    if (_dictionary != null) {
      int dictId = svReader.getInt(docId, _readerContext);
      return _dictionary.get(dictId);
    } else {
      return svReader.getInt(docId, _readerContext);
    }
  }

  public Object readLong(int docId) {
    SingleColumnSingleValueReader svReader = (SingleColumnSingleValueReader) _reader;
    if (_dictionary != null) {
      int dictId = svReader.getInt(docId, _readerContext);
      return _dictionary.get(dictId);
    } else {
      return svReader.getLong(docId, _readerContext);
    }
  }

  public Object readFloat(int docId) {
    SingleColumnSingleValueReader svReader = (SingleColumnSingleValueReader) _reader;
    if (_dictionary != null) {
      int dictId = svReader.getInt(docId, _readerContext);
      return _dictionary.get(dictId);
    } else {
      return svReader.getFloat(docId, _readerContext);
    }
  }

  public Object readDouble(int docId) {
    SingleColumnSingleValueReader svReader = (SingleColumnSingleValueReader) _reader;
    if (_dictionary != null) {
      int dictId = svReader.getInt(docId, _readerContext);
      return _dictionary.get(dictId);
    } else {
      return svReader.getDouble(docId, _readerContext);
    }
  }

  public Object readString(int docId) {
    SingleColumnSingleValueReader svReader = (SingleColumnSingleValueReader) _reader;
    if (_dictionary != null) {
      int dictId = svReader.getInt(docId, _readerContext);
      return _dictionary.get(dictId);
    } else {
      return svReader.getString(docId, _readerContext);
    }
  }

  Object[] readMV(int docId) {
    SingleColumnMultiValueReader mvReader = (SingleColumnMultiValueReader) _reader;
    int numValues = mvReader.getIntArray(docId, _mvBuffer, _readerContext);
    Object[] values = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = _dictionary.get(_mvBuffer[i]);
    }
    return values;
  }

  public boolean hasDictionary() {
    return _dictionary != null;
  }

  public int getDictionaryId(int docId) {
    if (_mvBuffer != null) {
      throw new UnsupportedOperationException("multi value column is not supported");
    }

    SingleColumnSingleValueReader svReader = (SingleColumnSingleValueReader) _reader;
    if (_dictionary != null) {
      return svReader.getInt(docId, _readerContext);
    } else {
      throw new UnsupportedOperationException("dictionary does not exist.");
    }
  }
}
