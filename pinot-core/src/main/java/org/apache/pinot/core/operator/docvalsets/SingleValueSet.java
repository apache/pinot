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
package org.apache.pinot.core.operator.docvalsets;

import org.apache.pinot.core.common.BaseBlockValSet;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;


@SuppressWarnings({"rawtypes", "unchecked"})
public final class SingleValueSet extends BaseBlockValSet {
  private final SingleColumnSingleValueReader _reader;
  private final ReaderContext _readerContext;
  private final DataType _dataType;

  public SingleValueSet(SingleColumnSingleValueReader reader, DataType dataType) {
    _reader = reader;
    _readerContext = reader.createContext();
    _dataType = dataType;
  }

  @Override
  public DataType getValueType() {
    return _dataType;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public int getIntValue(int docId) {
    return _reader.getInt(docId, _readerContext);
  }

  @Override
  public long getLongValue(int docId) {
    return _reader.getLong(docId, _readerContext);
  }

  @Override
  public float getFloatValue(int docId) {
    return _reader.getFloat(docId, _readerContext);
  }

  @Override
  public double getDoubleValue(int docId) {
    return _reader.getDouble(docId, _readerContext);
  }

  @Override
  public String getStringValue(int docId) {
    return _reader.getString(docId, _readerContext);
  }

  @Override
  public byte[] getBytesValue(int docId) {
    return _reader.getBytes(docId, _readerContext);
  }

  @Override
  public void getDictionaryIds(int[] docIds, int length, int[] dictIdBuffer) {
    _reader.readValues(docIds, 0, length, dictIdBuffer, 0);
  }

  @Override
  public void getIntValues(int[] docIds, int length, int[] valueBuffer) {
    if (_dataType == DataType.INT) {
      for (int i = 0; i < length; i++) {
        valueBuffer[i] = _reader.getInt(docIds[i], _readerContext);
      }
    } else {
      throw new IllegalStateException(String.format("Cannot read %s as INT", _dataType));
    }
  }

  @Override
  public void getLongValues(int[] docIds, int length, long[] valueBuffer) {
    switch (_dataType) {
      case INT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getInt(docIds[i], _readerContext);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getLong(docIds[i], _readerContext);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Cannot read %s as LONG", _dataType));
    }
  }

  @Override
  public void getFloatValues(int[] docIds, int length, float[] valueBuffer) {
    switch (_dataType) {
      case INT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getInt(docIds[i], _readerContext);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getLong(docIds[i], _readerContext);
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getFloat(docIds[i], _readerContext);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Cannot read %s as FLOAT", _dataType));
    }
  }

  @Override
  public void getDoubleValues(int[] docIds, int length, double[] valueBuffer) {
    switch (_dataType) {
      case INT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getInt(docIds[i], _readerContext);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getLong(docIds[i], _readerContext);
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getFloat(docIds[i], _readerContext);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getDouble(docIds[i], _readerContext);
        }
        break;
      default:
        throw new IllegalStateException(String.format("Cannot read %s as DOUBLE", _dataType));
    }
  }

  @Override
  public void getStringValues(int[] docIds, int length, String[] valueBuffer) {
    if (_dataType == DataType.STRING) {
      for (int i = 0; i < length; i++) {
        valueBuffer[i] = _reader.getString(docIds[i], _readerContext);
      }
    } else {
      throw new IllegalStateException(String.format("Cannot read %s as STRING", _dataType));
    }
  }

  @Override
  public void getBytesValues(int[] docIds, int length, byte[][] valueBuffer) {
    if (_dataType.equals(DataType.BYTES)) {
      for (int i = 0; i < length; i++) {
        valueBuffer[i] = _reader.getBytes(docIds[i], _readerContext);
      }
    } else {
      throw new IllegalStateException(String.format("Cannot read %s as BYTES", _dataType));
    }
  }
}
