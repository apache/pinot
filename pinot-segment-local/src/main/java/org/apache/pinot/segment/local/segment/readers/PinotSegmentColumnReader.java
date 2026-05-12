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
package org.apache.pinot.segment.local.segment.readers;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


@SuppressWarnings({"rawtypes", "unchecked"})
public class PinotSegmentColumnReader implements Closeable {
  private final ForwardIndexReader _forwardIndexReader;
  private final ForwardIndexReaderContext _forwardIndexReaderContext;
  private final Dictionary _dictionary;
  private final NullValueVectorReader _nullValueVectorReader;
  private final int[] _dictIdBuffer;
  private final DataType _valueType;

  public PinotSegmentColumnReader(IndexSegment indexSegment, String column) {
    DataSource dataSource = indexSegment.getDataSource(column);
    _forwardIndexReader = dataSource.getForwardIndex();
    Preconditions.checkArgument(_forwardIndexReader != null, "Forward index disabled for column: %s", column);
    _forwardIndexReaderContext = _forwardIndexReader.createContext();
    _dictionary = dataSource.getDictionary();
    _nullValueVectorReader = dataSource.getNullValueVector();
    _valueType = _dictionary != null ? _dictionary.getValueType() : _forwardIndexReader.getStoredType();
    if (_forwardIndexReader.isSingleValue()) {
      _dictIdBuffer = null;
    } else {
      int maxNumValuesPerMVEntry = dataSource.getDataSourceMetadata().getMaxNumValuesPerMVEntry();
      Preconditions.checkState(maxNumValuesPerMVEntry >= 0, "maxNumValuesPerMVEntry is negative for an MV column.");
      _dictIdBuffer = new int[maxNumValuesPerMVEntry];
    }
  }

  public PinotSegmentColumnReader(ForwardIndexReader forwardIndexReader, @Nullable Dictionary dictionary,
      @Nullable NullValueVectorReader nullValueVectorReader, int maxNumValuesPerMVEntry) {
    _forwardIndexReader = forwardIndexReader;
    _forwardIndexReaderContext = _forwardIndexReader.createContext();
    _dictionary = dictionary;
    _nullValueVectorReader = nullValueVectorReader;
    _valueType = _dictionary != null ? _dictionary.getValueType() : _forwardIndexReader.getStoredType();
    if (_forwardIndexReader.isSingleValue()) {
      _dictIdBuffer = null;
    } else {
      _dictIdBuffer = new int[maxNumValuesPerMVEntry];
    }
  }

  public boolean isSingleValue() {
    return _forwardIndexReader.isSingleValue();
  }

  public DataType getValueType() {
    return _valueType;
  }

  public boolean hasDictionary() {
    return _dictionary != null;
  }

  @Nullable
  public Dictionary getDictionary() {
    return _dictionary;
  }

  public int getDictId(int docId) {
    return _forwardIndexReader.getDictId(docId, _forwardIndexReaderContext);
  }

  public Object getValue(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      // Dictionary-encoded forward index
      if (_forwardIndexReader.isSingleValue()) {
        return _dictionary.get(_forwardIndexReader.getDictId(docId, _forwardIndexReaderContext));
      } else {
        int numValues = _forwardIndexReader.getDictIdMV(docId, _dictIdBuffer, _forwardIndexReaderContext);
        switch (_valueType) {
          case INT: {
            Integer[] values = new Integer[numValues];
            _dictionary.readIntValues(_dictIdBuffer, numValues, values);
            return values;
          }
          case LONG: {
            Long[] values = new Long[numValues];
            _dictionary.readLongValues(_dictIdBuffer, numValues, values);
            return values;
          }
          case FLOAT: {
            Float[] values = new Float[numValues];
            _dictionary.readFloatValues(_dictIdBuffer, numValues, values);
            return values;
          }
          case DOUBLE: {
            Double[] values = new Double[numValues];
            _dictionary.readDoubleValues(_dictIdBuffer, numValues, values);
            return values;
          }
          case BIG_DECIMAL: {
            BigDecimal[] values = new BigDecimal[numValues];
            _dictionary.readBigDecimalValues(_dictIdBuffer, numValues, values);
            return values;
          }
          case STRING: {
            String[] values = new String[numValues];
            _dictionary.readStringValues(_dictIdBuffer, numValues, values);
            return values;
          }
          case BYTES: {
            byte[][] values = new byte[numValues][];
            _dictionary.readBytesValues(_dictIdBuffer, numValues, values);
            return values;
          }
          default:
            throw new IllegalStateException("Unsupported MV dictionary column type: " + _valueType);
        }
      }
    } else {
      // Raw index based
      if (_forwardIndexReader.isSingleValue()) {
        switch (_valueType) {
          case INT:
            return _forwardIndexReader.getInt(docId, _forwardIndexReaderContext);
          case LONG:
            return _forwardIndexReader.getLong(docId, _forwardIndexReaderContext);
          case FLOAT:
            return _forwardIndexReader.getFloat(docId, _forwardIndexReaderContext);
          case DOUBLE:
            return _forwardIndexReader.getDouble(docId, _forwardIndexReaderContext);
          case BIG_DECIMAL:
            return _forwardIndexReader.getBigDecimal(docId, _forwardIndexReaderContext);
          case STRING:
            return _forwardIndexReader.getString(docId, _forwardIndexReaderContext);
          case BYTES:
            return _forwardIndexReader.getBytes(docId, _forwardIndexReaderContext);
          case MAP:
            return _forwardIndexReader.getMap(docId, _forwardIndexReaderContext);
          default:
            throw new IllegalStateException("Unsupported SV no-dictionary column type: " + _valueType);
        }
      } else {
        switch (_valueType) {
          case INT:
            return ArrayUtils.toObject(_forwardIndexReader.getIntMV(docId, _forwardIndexReaderContext));
          case LONG:
            return ArrayUtils.toObject(_forwardIndexReader.getLongMV(docId, _forwardIndexReaderContext));
          case FLOAT:
            return ArrayUtils.toObject(_forwardIndexReader.getFloatMV(docId, _forwardIndexReaderContext));
          case DOUBLE:
            return ArrayUtils.toObject(_forwardIndexReader.getDoubleMV(docId, _forwardIndexReaderContext));
          case BIG_DECIMAL:
            return _forwardIndexReader.getBigDecimalMV(docId, _forwardIndexReaderContext);
          case STRING:
            return _forwardIndexReader.getStringMV(docId, _forwardIndexReaderContext);
          case BYTES:
            return _forwardIndexReader.getBytesMV(docId, _forwardIndexReaderContext);
          default:
            throw new IllegalStateException("Unsupported MV no-dictionary column type: " + _valueType);
        }
      }
    }
  }

  public boolean isNull(int docId) {
    return _nullValueVectorReader != null && _nullValueVectorReader.isNull(docId);
  }

  // Single-value accessors

  public int getInt(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      return _dictionary.getIntValue(_forwardIndexReader.getDictId(docId, _forwardIndexReaderContext));
    } else {
      return _forwardIndexReader.getInt(docId, _forwardIndexReaderContext);
    }
  }

  public long getLong(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      return _dictionary.getLongValue(_forwardIndexReader.getDictId(docId, _forwardIndexReaderContext));
    } else {
      return _forwardIndexReader.getLong(docId, _forwardIndexReaderContext);
    }
  }

  public float getFloat(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      return _dictionary.getFloatValue(_forwardIndexReader.getDictId(docId, _forwardIndexReaderContext));
    } else {
      return _forwardIndexReader.getFloat(docId, _forwardIndexReaderContext);
    }
  }

  public double getDouble(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      return _dictionary.getDoubleValue(_forwardIndexReader.getDictId(docId, _forwardIndexReaderContext));
    } else {
      return _forwardIndexReader.getDouble(docId, _forwardIndexReaderContext);
    }
  }

  public BigDecimal getBigDecimal(int docId) {
    if (_dictionary != null) {
      return _dictionary.getBigDecimalValue(_forwardIndexReader.getDictId(docId, _forwardIndexReaderContext));
    } else {
      return _forwardIndexReader.getBigDecimal(docId, _forwardIndexReaderContext);
    }
  }

  public String getString(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      return _dictionary.getStringValue(_forwardIndexReader.getDictId(docId, _forwardIndexReaderContext));
    } else {
      return _forwardIndexReader.getString(docId, _forwardIndexReaderContext);
    }
  }

  public byte[] getBytes(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      return _dictionary.getBytesValue(_forwardIndexReader.getDictId(docId, _forwardIndexReaderContext));
    } else {
      return _forwardIndexReader.getBytes(docId, _forwardIndexReaderContext);
    }
  }

  // Multi-value accessors

  public int[] getIntMV(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      int numValues = _forwardIndexReader.getDictIdMV(docId, _dictIdBuffer, _forwardIndexReaderContext);
      int[] values = new int[numValues];
      _dictionary.readIntValues(_dictIdBuffer, numValues, values);
      return values;
    } else {
      return _forwardIndexReader.getIntMV(docId, _forwardIndexReaderContext);
    }
  }

  public long[] getLongMV(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      int numValues = _forwardIndexReader.getDictIdMV(docId, _dictIdBuffer, _forwardIndexReaderContext);
      long[] values = new long[numValues];
      _dictionary.readLongValues(_dictIdBuffer, numValues, values);
      return values;
    } else {
      return _forwardIndexReader.getLongMV(docId, _forwardIndexReaderContext);
    }
  }

  public float[] getFloatMV(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      int numValues = _forwardIndexReader.getDictIdMV(docId, _dictIdBuffer, _forwardIndexReaderContext);
      float[] values = new float[numValues];
      _dictionary.readFloatValues(_dictIdBuffer, numValues, values);
      return values;
    } else {
      return _forwardIndexReader.getFloatMV(docId, _forwardIndexReaderContext);
    }
  }

  public double[] getDoubleMV(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      int numValues = _forwardIndexReader.getDictIdMV(docId, _dictIdBuffer, _forwardIndexReaderContext);
      double[] values = new double[numValues];
      _dictionary.readDoubleValues(_dictIdBuffer, numValues, values);
      return values;
    } else {
      return _forwardIndexReader.getDoubleMV(docId, _forwardIndexReaderContext);
    }
  }

  public BigDecimal[] getBigDecimalMV(int docId) {
    if (_dictionary != null) {
      int numValues = _forwardIndexReader.getDictIdMV(docId, _dictIdBuffer, _forwardIndexReaderContext);
      BigDecimal[] values = new BigDecimal[numValues];
      _dictionary.readBigDecimalValues(_dictIdBuffer, numValues, values);
      return values;
    } else {
      return _forwardIndexReader.getBigDecimalMV(docId, _forwardIndexReaderContext);
    }
  }

  public String[] getStringMV(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      int numValues = _forwardIndexReader.getDictIdMV(docId, _dictIdBuffer, _forwardIndexReaderContext);
      String[] values = new String[numValues];
      _dictionary.readStringValues(_dictIdBuffer, numValues, values);
      return values;
    } else {
      return _forwardIndexReader.getStringMV(docId, _forwardIndexReaderContext);
    }
  }

  public byte[][] getBytesMV(int docId) {
    if (_forwardIndexReader.isDictionaryEncoded()) {
      int numValues = _forwardIndexReader.getDictIdMV(docId, _dictIdBuffer, _forwardIndexReaderContext);
      byte[][] values = new byte[numValues][];
      _dictionary.readBytesValues(_dictIdBuffer, numValues, values);
      return values;
    } else {
      return _forwardIndexReader.getBytesMV(docId, _forwardIndexReaderContext);
    }
  }

  /// Reads all values for this column. SV primitive type values are boxed; MV primitive type values are stored as
  /// primitive arrays.
  public Object[] readAllValues(int numDocs) {
    Object[] values = new Object[numDocs];
    if (_dictionary != null) {
      if (_forwardIndexReader.isSingleValue()) {
        for (int i = 0; i < numDocs; i++) {
          values[i] = _dictionary.get(_forwardIndexReader.getDictId(i, _forwardIndexReaderContext));
        }
      } else {
        switch (_valueType) {
          case INT:
            for (int i = 0; i < numDocs; i++) {
              int numValues = _forwardIndexReader.getDictIdMV(i, _dictIdBuffer, _forwardIndexReaderContext);
              int[] value = new int[numValues];
              _dictionary.readIntValues(_dictIdBuffer, numValues, value);
              values[i] = value;
            }
            break;
          case LONG:
            for (int i = 0; i < numDocs; i++) {
              int numValues = _forwardIndexReader.getDictIdMV(i, _dictIdBuffer, _forwardIndexReaderContext);
              long[] value = new long[numValues];
              _dictionary.readLongValues(_dictIdBuffer, numValues, value);
              values[i] = value;
            }
            break;
          case FLOAT:
            for (int i = 0; i < numDocs; i++) {
              int numValues = _forwardIndexReader.getDictIdMV(i, _dictIdBuffer, _forwardIndexReaderContext);
              float[] value = new float[numValues];
              _dictionary.readFloatValues(_dictIdBuffer, numValues, value);
              values[i] = value;
            }
            break;
          case DOUBLE:
            for (int i = 0; i < numDocs; i++) {
              int numValues = _forwardIndexReader.getDictIdMV(i, _dictIdBuffer, _forwardIndexReaderContext);
              double[] value = new double[numValues];
              _dictionary.readDoubleValues(_dictIdBuffer, numValues, value);
              values[i] = value;
            }
            break;
          case BIG_DECIMAL:
            for (int i = 0; i < numDocs; i++) {
              int numValues = _forwardIndexReader.getDictIdMV(i, _dictIdBuffer, _forwardIndexReaderContext);
              BigDecimal[] value = new BigDecimal[numValues];
              _dictionary.readBigDecimalValues(_dictIdBuffer, numValues, value);
              values[i] = value;
            }
            break;
          case STRING:
            for (int i = 0; i < numDocs; i++) {
              int numValues = _forwardIndexReader.getDictIdMV(i, _dictIdBuffer, _forwardIndexReaderContext);
              String[] value = new String[numValues];
              _dictionary.readStringValues(_dictIdBuffer, numValues, value);
              values[i] = value;
            }
            break;
          case BYTES:
            for (int i = 0; i < numDocs; i++) {
              int numValues = _forwardIndexReader.getDictIdMV(i, _dictIdBuffer, _forwardIndexReaderContext);
              byte[][] value = new byte[numValues][];
              _dictionary.readBytesValues(_dictIdBuffer, numValues, value);
              values[i] = value;
            }
            break;
          default:
            throw new IllegalStateException("Unsupported MV dictionary column type: " + _valueType);
        }
      }
    } else {
      if (_forwardIndexReader.isSingleValue()) {
        switch (_valueType) {
          case INT:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getInt(i, _forwardIndexReaderContext);
            }
            break;
          case LONG:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getLong(i, _forwardIndexReaderContext);
            }
            break;
          case FLOAT:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getFloat(i, _forwardIndexReaderContext);
            }
            break;
          case DOUBLE:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getDouble(i, _forwardIndexReaderContext);
            }
            break;
          case BIG_DECIMAL:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getBigDecimal(i, _forwardIndexReaderContext);
            }
            break;
          case STRING:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getString(i, _forwardIndexReaderContext);
            }
            break;
          case BYTES:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getBytes(i, _forwardIndexReaderContext);
            }
            break;
          case MAP:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getMap(i, _forwardIndexReaderContext);
            }
            break;
          default:
            throw new IllegalStateException("Unsupported SV no-dictionary column type: " + _valueType);
        }
      } else {
        switch (_valueType) {
          case INT:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getIntMV(i, _forwardIndexReaderContext);
            }
            break;
          case LONG:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getLongMV(i, _forwardIndexReaderContext);
            }
            break;
          case FLOAT:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getFloatMV(i, _forwardIndexReaderContext);
            }
            break;
          case DOUBLE:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getDoubleMV(i, _forwardIndexReaderContext);
            }
            break;
          case BIG_DECIMAL:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getBigDecimalMV(i, _forwardIndexReaderContext);
            }
            break;
          case STRING:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getStringMV(i, _forwardIndexReaderContext);
            }
            break;
          case BYTES:
            for (int i = 0; i < numDocs; i++) {
              values[i] = _forwardIndexReader.getBytesMV(i, _forwardIndexReaderContext);
            }
            break;
          default:
            throw new IllegalStateException("Unsupported MV no-dictionary column type: " + _valueType);
        }
      }
    }
    return values;
  }

  /// Reads all values for this single-value column into a `Comparable` array in a single sequential pass.
  /// Pre-materializing values avoids repeated forward-index access during quicksort's random-access comparisons.
  @SuppressWarnings("rawtypes")
  public Comparable[] readAllValuesForSorting(int numDocs) {
    assert _forwardIndexReader.isSingleValue();
    Comparable[] values = new Comparable[numDocs];
    if (_dictionary != null) {
      switch (_valueType) {
        case INT:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _dictionary.getIntValue(_forwardIndexReader.getDictId(i, _forwardIndexReaderContext));
          }
          break;
        case LONG:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _dictionary.getLongValue(_forwardIndexReader.getDictId(i, _forwardIndexReaderContext));
          }
          break;
        case FLOAT:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _dictionary.getFloatValue(_forwardIndexReader.getDictId(i, _forwardIndexReaderContext));
          }
          break;
        case DOUBLE:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _dictionary.getDoubleValue(_forwardIndexReader.getDictId(i, _forwardIndexReaderContext));
          }
          break;
        case BIG_DECIMAL:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _dictionary.getBigDecimalValue(_forwardIndexReader.getDictId(i, _forwardIndexReaderContext));
          }
          break;
        case STRING:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _dictionary.getStringValue(_forwardIndexReader.getDictId(i, _forwardIndexReaderContext));
          }
          break;
        case BYTES:
          for (int i = 0; i < numDocs; i++) {
            values[i] =
                new ByteArray(_dictionary.getBytesValue(_forwardIndexReader.getDictId(i, _forwardIndexReaderContext)));
          }
          break;
        default:
          throw new IllegalStateException("Unsupported SV dictionary column type for comparison: " + _valueType);
      }
    } else {
      switch (_valueType) {
        case INT:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _forwardIndexReader.getInt(i, _forwardIndexReaderContext);
          }
          break;
        case LONG:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _forwardIndexReader.getLong(i, _forwardIndexReaderContext);
          }
          break;
        case FLOAT:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _forwardIndexReader.getFloat(i, _forwardIndexReaderContext);
          }
          break;
        case DOUBLE:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _forwardIndexReader.getDouble(i, _forwardIndexReaderContext);
          }
          break;
        case BIG_DECIMAL:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _forwardIndexReader.getBigDecimal(i, _forwardIndexReaderContext);
          }
          break;
        case STRING:
          for (int i = 0; i < numDocs; i++) {
            values[i] = _forwardIndexReader.getString(i, _forwardIndexReaderContext);
          }
          break;
        case BYTES:
          for (int i = 0; i < numDocs; i++) {
            values[i] = new ByteArray(_forwardIndexReader.getBytes(i, _forwardIndexReaderContext));
          }
          break;
        default:
          throw new IllegalStateException("Unsupported SV no-dictionary column type for comparison: " + _valueType);
      }
    }
    return values;
  }

  /// Compares two documents by this column's value. For dictionary-encoded columns the comparison is done via
  /// dictionary ids; for no-dictionary columns values are read directly from the forward index and compared by type.
  public int compare(int docId1, int docId2) {
    assert _forwardIndexReader.isSingleValue();
    if (_dictionary != null) {
      return _dictionary.compare(_forwardIndexReader.getDictId(docId1, _forwardIndexReaderContext),
          _forwardIndexReader.getDictId(docId2, _forwardIndexReaderContext));
    }
    switch (_valueType) {
      case INT:
        return Integer.compare(_forwardIndexReader.getInt(docId1, _forwardIndexReaderContext),
            _forwardIndexReader.getInt(docId2, _forwardIndexReaderContext));
      case LONG:
        return Long.compare(_forwardIndexReader.getLong(docId1, _forwardIndexReaderContext),
            _forwardIndexReader.getLong(docId2, _forwardIndexReaderContext));
      case FLOAT:
        return Float.compare(_forwardIndexReader.getFloat(docId1, _forwardIndexReaderContext),
            _forwardIndexReader.getFloat(docId2, _forwardIndexReaderContext));
      case DOUBLE:
        return Double.compare(_forwardIndexReader.getDouble(docId1, _forwardIndexReaderContext),
            _forwardIndexReader.getDouble(docId2, _forwardIndexReaderContext));
      case BIG_DECIMAL:
        return _forwardIndexReader.getBigDecimal(docId1, _forwardIndexReaderContext)
            .compareTo(_forwardIndexReader.getBigDecimal(docId2, _forwardIndexReaderContext));
      case STRING:
        return _forwardIndexReader.getString(docId1, _forwardIndexReaderContext)
            .compareTo(_forwardIndexReader.getString(docId2, _forwardIndexReaderContext));
      case BYTES:
        return ByteArray.compare(_forwardIndexReader.getBytes(docId1, _forwardIndexReaderContext),
            _forwardIndexReader.getBytes(docId2, _forwardIndexReaderContext));
      default:
        throw new IllegalStateException("Unsupported SV no-dictionary column type for comparison: " + _valueType);
    }
  }

  @Override
  public void close()
      throws IOException {
    if (_forwardIndexReaderContext != null) {
      _forwardIndexReaderContext.close();
    }
  }
}
