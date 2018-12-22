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
package com.linkedin.pinot.core.operator.docvalsets;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BaseBlockValSet;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.io.reader.ReaderContext;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.operator.docvaliterators.SingleValueIterator;


@SuppressWarnings("unchecked")
public final class SingleValueSet extends BaseBlockValSet {
  private final SingleColumnSingleValueReader _reader;
  private final int _numDocs;
  private final DataType _dataType;

  public SingleValueSet(SingleColumnSingleValueReader reader, int numDocs, DataType dataType) {
    _reader = reader;
    _numDocs = numDocs;
    _dataType = dataType;
  }

  @Override
  public BlockValIterator iterator() {
    return new SingleValueIterator(_reader, _numDocs);
  }

  @Override
  public DataType getValueType() {
    return _dataType;
  }

  @Override
  public long getIntValues(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    if (_dataType == DataType.INT) {
      for (int i = inStartPos; i < inEndPos; i++) {
        outValues[outStartPos++] = _reader.getInt(inDocIds[i], context);
      }
    } else {
      throw new UnsupportedOperationException();
    }
    return inDocIdsSize * Integer.BYTES;
  }

  @Override
  public long getLongValues(int[] inDocIds, int inStartPos, int inDocIdsSize, long[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    switch (_dataType) {
      case INT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getInt(inDocIds[i], context);
        }
        return inDocIdsSize * Integer.BYTES;
      case LONG:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getLong(inDocIds[i], context);
        }
        return inDocIdsSize * Long.BYTES;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public long getFloatValues(int[] inDocIds, int inStartPos, int inDocIdsSize, float[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    switch (_dataType) {
      case INT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getInt(inDocIds[i], context);
        }
        return inDocIdsSize * Integer.BYTES;
      case LONG:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getLong(inDocIds[i], context);
        }
        return inDocIdsSize * Long.BYTES;
      case FLOAT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getFloat(inDocIds[i], context);
        }
        return inDocIdsSize * Float.BYTES;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public long getDoubleValues(int[] inDocIds, int inStartPos, int inDocIdsSize, double[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    switch (_dataType) {
      case INT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getInt(inDocIds[i], context);
        }
        return inDocIdsSize * Integer.BYTES;
      case LONG:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getLong(inDocIds[i], context);
        }
        return inDocIdsSize * Long.BYTES;
      case FLOAT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getFloat(inDocIds[i], context);
        }
        return inDocIdsSize * Float.BYTES;
      case DOUBLE:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getDouble(inDocIds[i], context);
        }
        return inDocIdsSize * Double.BYTES;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public long getStringValues(int[] inDocIds, int inStartPos, int inDocIdsSize, String[] outValues, int outStartPos) {

    long bytesRead = 0;
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    if (_dataType == DataType.STRING) {
      for (int i = inStartPos; i < inEndPos; i++) {
        String val =  _reader.getString(inDocIds[i], context);
        outValues[outStartPos++] = val;
        bytesRead += val.length();
      }
      return bytesRead;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public long getBytesValues(int[] inDocIds, int inStartPos, int inDocIdsSize, byte[][] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    long bytesRead = 0;
    ReaderContext context = _reader.createContext();
    if (_dataType.equals(DataType.BYTES)) {
      for (int i = inStartPos; i < inEndPos; i++) {
        byte[] val = _reader.getBytes(inDocIds[i], context);
        outValues[outStartPos++] = val;
        bytesRead += val.length;
      }
      return bytesRead;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void getDictionaryIds(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outDictionaryIds,
      int outStartPos) {
    _reader.readValues(inDocIds, inStartPos, inDocIdsSize, outDictionaryIds, outStartPos);
  }
}
