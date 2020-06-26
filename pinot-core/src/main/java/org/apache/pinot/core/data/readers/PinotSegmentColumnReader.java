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
package org.apache.pinot.core.data.readers;

import org.apache.pinot.core.common.ColumnValueReader;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class PinotSegmentColumnReader {
  private final ColumnValueReader _valueReader;
  private final Dictionary _dictionary;
  private final int[] _mvBuffer;

  public PinotSegmentColumnReader(ImmutableSegment immutableSegment, String column) {
    DataSource dataSource = immutableSegment.getDataSource(column);
    _valueReader = dataSource.getValueReader();
    _dictionary = dataSource.getDictionary();
    if (_valueReader.isSingleValue()) {
      _mvBuffer = null;
    } else {
      _mvBuffer = new int[dataSource.getDataSourceMetadata().getMaxNumValuesPerMVEntry()];
    }
  }

  public Object readInt(int docId) {
    if (_dictionary != null) {
      return _dictionary.get(_valueReader.getIntValue(docId));
    } else {
      return _valueReader.getIntValue(docId);
    }
  }

  public Object readLong(int docId) {
    if (_dictionary != null) {
      return _dictionary.get(_valueReader.getIntValue(docId));
    } else {
      return _valueReader.getLongValue(docId);
    }
  }

  public Object readFloat(int docId) {
    if (_dictionary != null) {
      return _dictionary.get(_valueReader.getIntValue(docId));
    } else {
      return _valueReader.getFloatValue(docId);
    }
  }

  public Object readDouble(int docId) {
    if (_dictionary != null) {
      return _dictionary.get(_valueReader.getIntValue(docId));
    } else {
      return _valueReader.getDoubleValue(docId);
    }
  }

  public Object readString(int docId) {
    if (_dictionary != null) {
      return _dictionary.get(_valueReader.getIntValue(docId));
    } else {
      return _valueReader.getStringValue(docId);
    }
  }

  public Object readBytes(int docId) {
    if (_dictionary != null) {
      return _dictionary.get(_valueReader.getIntValue(docId));
    } else {
      return _valueReader.getBytesValue(docId);
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
    int numValues = _valueReader.getIntValues(docId, _mvBuffer);
    Object[] values = new Object[numValues];
    for (int i = 0; i < numValues; i++) {
      values[i] = _dictionary.get(_mvBuffer[i]);
    }
    return values;
  }

  public int getDictionaryId(int docId) {
    return _valueReader.getIntValue(docId);
  }

  public boolean hasDictionary() {
    return _dictionary != null;
  }
}
