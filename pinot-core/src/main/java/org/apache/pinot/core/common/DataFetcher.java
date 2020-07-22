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
package org.apache.pinot.core.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReaderContext;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * DataFetcher is a higher level abstraction for data fetching. Given the DataSource, DataFetcher can manage the
 * readers (ForwardIndexReader and Dictionary) for the column, preventing redundant construction for these instances.
 * DataFetcher can be used by both selection, aggregation and group-by data fetching process, reducing duplicate codes
 * and garbage collection.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DataFetcher {
  // Thread local (reusable) buffer for single-valued column dictionary Ids
  private static final ThreadLocal<int[]> THREAD_LOCAL_DICT_IDS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  // TODO: Figure out a way to close the reader context within the ColumnValueReader
  //       ChunkReaderContext should be closed explicitly to release the off-heap buffer
  private final Map<String, ColumnValueReader> _columnValueReaderMap;
  private final int[] _reusableMVDictIds;

  /**
   * Constructor for DataFetcher.
   *
   * @param dataSourceMap Map from column to data source
   */
  public DataFetcher(Map<String, DataSource> dataSourceMap) {
    _columnValueReaderMap = new HashMap<>();
    int maxNumValuesPerMVEntry = 0;
    for (Map.Entry<String, DataSource> entry : dataSourceMap.entrySet()) {
      String column = entry.getKey();
      DataSource dataSource = entry.getValue();
      ColumnValueReader columnValueReader =
          new ColumnValueReader(dataSource.getForwardIndex(), dataSource.getDictionary());
      _columnValueReaderMap.put(column, columnValueReader);
      DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
      if (!dataSourceMetadata.isSingleValue()) {
        maxNumValuesPerMVEntry = Math.max(maxNumValuesPerMVEntry, dataSourceMetadata.getMaxNumValuesPerMVEntry());
      }
    }
    _reusableMVDictIds = new int[maxNumValuesPerMVEntry];
  }

  /**
   * SINGLE-VALUED COLUMN API
   */

  /**
   * Fetch the dictionary Ids for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outDictIds Buffer for output
   */
  public void fetchDictIds(String column, int[] inDocIds, int length, int[] outDictIds) {
    _columnValueReaderMap.get(column).readDictIds(inDocIds, length, outDictIds);
  }

  /**
   * Fetch the int values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchIntValues(String column, int[] inDocIds, int length, int[] outValues) {
    _columnValueReaderMap.get(column).readIntValues(inDocIds, length, outValues);
  }

  /**
   * Fetch the long values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchLongValues(String column, int[] inDocIds, int length, long[] outValues) {
    _columnValueReaderMap.get(column).readLongValues(inDocIds, length, outValues);
  }

  /**
   * Fetch the float values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchFloatValues(String column, int[] inDocIds, int length, float[] outValues) {
    _columnValueReaderMap.get(column).readFloatValues(inDocIds, length, outValues);
  }

  /**
   * Fetch the double values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchDoubleValues(String column, int[] inDocIds, int length, double[] outValues) {
    _columnValueReaderMap.get(column).readDoubleValues(inDocIds, length, outValues);
  }

  /**
   * Fetch the string values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchStringValues(String column, int[] inDocIds, int length, String[] outValues) {
    _columnValueReaderMap.get(column).readStringValues(inDocIds, length, outValues);
  }

  /**
   * Fetch byte[] values for a single-valued column.
   *
   * @param column Column to read
   * @param inDocIds Input document id's buffer
   * @param length Number of input document id'
   * @param outValues Buffer for output
   */
  public void fetchBytesValues(String column, int[] inDocIds, int length, byte[][] outValues) {
    _columnValueReaderMap.get(column).readBytesValues(inDocIds, length, outValues);
  }

  /**
   * MULTI-VALUED COLUMN API
   */

  /**
   * Fetch the dictionary Ids for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outDictIds Buffer for output
   */
  public void fetchDictIds(String column, int[] inDocIds, int length, int[][] outDictIds) {
    _columnValueReaderMap.get(column).readDictIdsMV(inDocIds, length, outDictIds);
  }

  /**
   * Fetch the int values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchIntValues(String column, int[] inDocIds, int length, int[][] outValues) {
    _columnValueReaderMap.get(column).readIntValuesMV(inDocIds, length, outValues);
  }

  /**
   * Fetch the long values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchLongValues(String column, int[] inDocIds, int length, long[][] outValues) {
    _columnValueReaderMap.get(column).readLongValuesMV(inDocIds, length, outValues);
  }

  /**
   * Fetch the float values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchFloatValues(String column, int[] inDocIds, int length, float[][] outValues) {
    _columnValueReaderMap.get(column).readFloatValuesMV(inDocIds, length, outValues);
  }

  /**
   * Fetch the double values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchDoubleValues(String column, int[] inDocIds, int length, double[][] outValues) {
    _columnValueReaderMap.get(column).readDoubleValuesMV(inDocIds, length, outValues);
  }

  /**
   * Fetch the string values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchStringValues(String column, int[] inDocIds, int length, String[][] outValues) {
    _columnValueReaderMap.get(column).readStringValuesMV(inDocIds, length, outValues);
  }

  /**
   * Fetch the number of values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outNumValues Buffer for output
   */
  public void fetchNumValues(String column, int[] inDocIds, int length, int[] outNumValues) {
    _columnValueReaderMap.get(column).readNumValuesMV(inDocIds, length, outNumValues);
  }

  /**
   * Helper class to read values for a column from forward index and dictionary. For raw (non-dictionary-encoded)
   * forward index, similar to Dictionary, type conversion among INT, LONG, FLOAT, DOUBLE, STRING is supported; type
   * conversion between STRING and BYTES via Hex encoding/decoding is supported.
   */
  private class ColumnValueReader implements Closeable {
    final ForwardIndexReader _reader;
    final ForwardIndexReaderContext _readerContext;
    final Dictionary _dictionary;

    ColumnValueReader(ForwardIndexReader reader, @Nullable Dictionary dictionary) {
      _reader = reader;
      _readerContext = reader.createContext();
      _dictionary = dictionary;
    }

    void readDictIds(int[] docIds, int length, int[] dictIdBuffer) {
      _reader.readDictIds(docIds, length, dictIdBuffer, _readerContext);
    }

    void readIntValues(int[] docIds, int length, int[] valueBuffer) {
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, _readerContext);
        _dictionary.readIntValues(dictIdBuffer, length, valueBuffer);
      } else {
        switch (_reader.getValueType()) {
          case INT:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = _reader.getInt(docIds[i], _readerContext);
            }
            break;
          case LONG:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = (int) _reader.getLong(docIds[i], _readerContext);
            }
            break;
          case FLOAT:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = (int) _reader.getFloat(docIds[i], _readerContext);
            }
            break;
          case DOUBLE:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = (int) _reader.getDouble(docIds[i], _readerContext);
            }
            break;
          case STRING:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Integer.parseInt(_reader.getString(docIds[i], _readerContext));
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    void readLongValues(int[] docIds, int length, long[] valueBuffer) {
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, _readerContext);
        _dictionary.readLongValues(dictIdBuffer, length, valueBuffer);
      } else {
        switch (_reader.getValueType()) {
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
              valueBuffer[i] = (long) _reader.getFloat(docIds[i], _readerContext);
            }
            break;
          case DOUBLE:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = (long) _reader.getDouble(docIds[i], _readerContext);
            }
            break;
          case STRING:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Long.parseLong(_reader.getString(docIds[i], _readerContext));
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    void readFloatValues(int[] docIds, int length, float[] valueBuffer) {
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, _readerContext);
        _dictionary.readFloatValues(dictIdBuffer, length, valueBuffer);
      } else {
        switch (_reader.getValueType()) {
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
              valueBuffer[i] = (float) _reader.getDouble(docIds[i], _readerContext);
            }
            break;
          case STRING:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Float.parseFloat(_reader.getString(docIds[i], _readerContext));
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    void readDoubleValues(int[] docIds, int length, double[] valueBuffer) {
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, _readerContext);
        _dictionary.readDoubleValues(dictIdBuffer, length, valueBuffer);
      } else {
        switch (_reader.getValueType()) {
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
          case STRING:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Double.parseDouble(_reader.getString(docIds[i], _readerContext));
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    void readStringValues(int[] docIds, int length, String[] valueBuffer) {
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, _readerContext);
        _dictionary.readStringValues(dictIdBuffer, length, valueBuffer);
      } else {
        switch (_reader.getValueType()) {
          case INT:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Integer.toString(_reader.getInt(docIds[i], _readerContext));
            }
            break;
          case LONG:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Long.toString(_reader.getLong(docIds[i], _readerContext));
            }
            break;
          case FLOAT:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Float.toString(_reader.getFloat(docIds[i], _readerContext));
            }
            break;
          case DOUBLE:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Double.toString(_reader.getDouble(docIds[i], _readerContext));
            }
            break;
          case STRING:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = _reader.getString(docIds[i], _readerContext);
            }
            break;
          case BYTES:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = BytesUtils.toHexString(_reader.getBytes(docIds[i], _readerContext));
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    void readBytesValues(int[] docIds, int length, byte[][] valueBuffer) {
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, _readerContext);
        _dictionary.readBytesValues(dictIdBuffer, length, valueBuffer);
      } else {
        switch (_reader.getValueType()) {
          case STRING:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = BytesUtils.toBytes(_reader.getString(docIds[i], _readerContext));
            }
            break;
          case BYTES:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = _reader.getBytes(docIds[i], _readerContext);
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    void readDictIdsMV(int[] docIds, int length, int[][] dictIdsBuffer) {
      for (int i = 0; i < length; i++) {
        int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, _readerContext);
        dictIdsBuffer[i] = Arrays.copyOfRange(_reusableMVDictIds, 0, numValues);
      }
    }

    void readIntValuesMV(int[] docIds, int length, int[][] valuesBuffer) {
      for (int i = 0; i < length; i++) {
        int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, _readerContext);
        int[] values = new int[numValues];
        _dictionary.readIntValues(_reusableMVDictIds, numValues, values);
        valuesBuffer[i] = values;
      }
    }

    void readLongValuesMV(int[] docIds, int length, long[][] valuesBuffer) {
      for (int i = 0; i < length; i++) {
        int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, _readerContext);
        long[] values = new long[numValues];
        _dictionary.readLongValues(_reusableMVDictIds, numValues, values);
        valuesBuffer[i] = values;
      }
    }

    void readFloatValuesMV(int[] docIds, int length, float[][] valuesBuffer) {
      for (int i = 0; i < length; i++) {
        int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, _readerContext);
        float[] values = new float[numValues];
        _dictionary.readFloatValues(_reusableMVDictIds, numValues, values);
        valuesBuffer[i] = values;
      }
    }

    void readDoubleValuesMV(int[] docIds, int length, double[][] valuesBuffer) {
      for (int i = 0; i < length; i++) {
        int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, _readerContext);
        double[] values = new double[numValues];
        _dictionary.readDoubleValues(_reusableMVDictIds, numValues, values);
        valuesBuffer[i] = values;
      }
    }

    void readStringValuesMV(int[] docIds, int length, String[][] valuesBuffer) {
      for (int i = 0; i < length; i++) {
        int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, _readerContext);
        String[] values = new String[numValues];
        _dictionary.readStringValues(_reusableMVDictIds, numValues, values);
        valuesBuffer[i] = values;
      }
    }

    public void readNumValuesMV(int[] docIds, int length, int[] numValuesBuffer) {
      for (int i = 0; i < length; i++) {
        numValuesBuffer[i] = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, _readerContext);
      }
    }

    @Override
    public void close()
        throws IOException {
      _readerContext.close();
    }
  }
}
