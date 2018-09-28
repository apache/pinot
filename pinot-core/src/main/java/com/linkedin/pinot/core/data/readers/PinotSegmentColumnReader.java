/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


@SuppressWarnings("unchecked")
public class PinotSegmentColumnReader {
  private final Dictionary _dictionary;
  private final DataFileReader _reader;
  private final ReaderContext _readerContext;
  private final int[] _mvBuffer;

  public PinotSegmentColumnReader(ImmutableSegment immutableSegment, String column) {
    _dictionary = immutableSegment.getDictionary(column);
    _reader = immutableSegment.getForwardIndex(column);
    _readerContext = _reader.createContext();
    SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) immutableSegment.getSegmentMetadata();
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
    if (columnMetadata.isSingleValue()) {
      _mvBuffer = null;
    } else {
      _mvBuffer = new int[columnMetadata.getMaxNumberOfMultiValues()];
    }
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

  public Object readBytes(int docId) {
    SingleColumnSingleValueReader svReader = (SingleColumnSingleValueReader) _reader;
    if (_dictionary != null) {
      int dictId = svReader.getInt(docId, _readerContext);
      return _dictionary.get(dictId);
    } else {
      return svReader.getBytes(docId, _readerContext);
    }
  }

  public Object readSV(int docId, DataType dataType) {
    switch (dataType) {
      case INT:
        return readInt(docId);
      case LONG:
        return readLong(docId);
      case FLOAT:
        return readFloat(docId);
      case DOUBLE:
        return readDouble(docId);
      case STRING:
        return readString(docId);
      case BYTES:
        return readBytes(docId);
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  public Object[] readMV(int docId) {
    SingleColumnMultiValueReader mvReader = (SingleColumnMultiValueReader) _reader;
    int numValues = mvReader.getIntArray(docId, _mvBuffer, _readerContext);
    Object[] values = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = _dictionary.get(_mvBuffer[i]);
    }
    return values;
  }

  public int getDictionaryId(int docId) {
    if (_mvBuffer != null) {
      throw new IllegalStateException("Multi value column is not supported");
    }
    SingleColumnSingleValueReader svReader = (SingleColumnSingleValueReader) _reader;
    if (_dictionary != null) {
      return svReader.getInt(docId, _readerContext);
    } else {
      throw new IllegalStateException("No dictionary column is not supported");
    }
  }

  public boolean hasDictionary() {
    return _dictionary != null;
  }
}
