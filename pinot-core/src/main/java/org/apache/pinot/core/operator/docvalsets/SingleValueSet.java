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

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.core.common.BaseBlockValSet;
import org.apache.pinot.core.common.BlockValIterator;
import org.apache.pinot.core.io.reader.ReaderContext;
import org.apache.pinot.core.io.reader.SingleColumnSingleValueReader;
import org.apache.pinot.core.operator.docvaliterators.SingleValueIterator;


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
  public boolean isSingleValue() {
    return true;
  }

  @Override
  public void getIntValues(int[] inDocIds, int inStartPos, int inDocIdsSize, int[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    if (_dataType == DataType.INT) {
      for (int i = inStartPos; i < inEndPos; i++) {
        outValues[outStartPos++] = _reader.getInt(inDocIds[i], context);
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void getLongValues(int[] inDocIds, int inStartPos, int inDocIdsSize, long[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    switch (_dataType) {
      case INT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getInt(inDocIds[i], context);
        }
        break;
      case LONG:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getLong(inDocIds[i], context);
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void getFloatValues(int[] inDocIds, int inStartPos, int inDocIdsSize, float[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    switch (_dataType) {
      case INT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getInt(inDocIds[i], context);
        }
        break;
      case LONG:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getLong(inDocIds[i], context);
        }
        break;
      case FLOAT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getFloat(inDocIds[i], context);
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void getDoubleValues(int[] inDocIds, int inStartPos, int inDocIdsSize, double[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    switch (_dataType) {
      case INT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getInt(inDocIds[i], context);
        }
        break;
      case LONG:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getLong(inDocIds[i], context);
        }
        break;
      case FLOAT:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getFloat(inDocIds[i], context);
        }
        break;
      case DOUBLE:
        for (int i = inStartPos; i < inEndPos; i++) {
          outValues[outStartPos++] = _reader.getDouble(inDocIds[i], context);
        }
        break;
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public void getStringValues(int[] inDocIds, int inStartPos, int inDocIdsSize, String[] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    if (_dataType == DataType.STRING) {
      for (int i = inStartPos; i < inEndPos; i++) {
        outValues[outStartPos++] = _reader.getString(inDocIds[i], context);
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public void getBytesValues(int[] inDocIds, int inStartPos, int inDocIdsSize, byte[][] outValues, int outStartPos) {
    int inEndPos = inStartPos + inDocIdsSize;
    ReaderContext context = _reader.createContext();
    if (_dataType.equals(DataType.BYTES)) {
      for (int i = inStartPos; i < inEndPos; i++) {
        outValues[outStartPos++] = _reader.getBytes(inDocIds[i], context);
      }
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
