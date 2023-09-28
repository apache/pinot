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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.evaluator.TransformEvaluator;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.trace.Tracing;
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
  private final int _maxNumValuesPerMVEntry;

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
      DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
      ForwardIndexReader<?> forwardIndexReader = dataSource.getForwardIndex();
      Preconditions.checkState(forwardIndexReader != null,
          "Forward index disabled for column: %s, cannot create DataFetcher!", column);
      ColumnValueReader columnValueReader =
          new ColumnValueReader(forwardIndexReader, dataSource.getDictionary());
      _columnValueReaderMap.put(column, columnValueReader);
      if (!dataSourceMetadata.isSingleValue()) {
        maxNumValuesPerMVEntry = Math.max(maxNumValuesPerMVEntry, dataSourceMetadata.getMaxNumValuesPerMVEntry());
      }
    }
    _reusableMVDictIds = new int[maxNumValuesPerMVEntry];
    _maxNumValuesPerMVEntry = maxNumValuesPerMVEntry;
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
   * Fetch and transform the int values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchIntValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length, int[] outValues) {
    _columnValueReaderMap.get(column).readIntValues(evaluator, inDocIds, length, outValues);
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
   * Fetch and transform the int values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchLongValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      long[] outValues) {
    _columnValueReaderMap.get(column).readLongValues(evaluator, inDocIds, length, outValues);
  }

  /**
   * Fetch long values for a single-valued column.
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
   * Fetch and transform float values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchFloatValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      float[] outValues) {
    _columnValueReaderMap.get(column).readFloatValues(evaluator, inDocIds, length, outValues);
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
   * Fetch and transform double values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchDoubleValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      double[] outValues) {
    _columnValueReaderMap.get(column).readDoubleValues(evaluator, inDocIds, length, outValues);
  }

  /**
   * Fetch the BigDecimal values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchBigDecimalValues(String column, int[] inDocIds, int length, BigDecimal[] outValues) {
    _columnValueReaderMap.get(column).readBigDecimalValues(inDocIds, length, outValues);
  }

  /**
   * Fetch and transform BigDecimal values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchVectorValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      Vector[] outValues) {
    _columnValueReaderMap.get(column).readVectorValues(evaluator, inDocIds, length, outValues);
  }

  /**
   * Fetch the BigDecimal values for a single-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchVectorValues(String column, int[] inDocIds, int length, Vector[] outValues) {
    _columnValueReaderMap.get(column).readVectorValues(inDocIds, length, outValues);
  }

  /**
   * Fetch and transform BigDecimal values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchBigDecimalValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      BigDecimal[] outValues) {
    _columnValueReaderMap.get(column).readBigDecimalValues(evaluator, inDocIds, length, outValues);
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
   * Fetch and transform String values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchStringValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      String[] outValues) {
    _columnValueReaderMap.get(column).readStringValues(evaluator, inDocIds, length, outValues);
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
   * Fetch int[] values from a JSON column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchIntValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      int[][] outValues) {
    _columnValueReaderMap.get(column).readIntValuesMV(evaluator, inDocIds, length, outValues);
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
   * Fetch and transform long[] values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchLongValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      long[][] outValues) {
    _columnValueReaderMap.get(column).readLongValuesMV(evaluator, inDocIds, length, outValues);
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
   * Fetch and transform float[] values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchFloatValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      float[][] outValues) {
    _columnValueReaderMap.get(column).readFloatValuesMV(evaluator, inDocIds, length, outValues);
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
   * Fetch and transform double[] values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchDoubleValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      double[][] outValues) {
    _columnValueReaderMap.get(column).readDoubleValuesMV(evaluator, inDocIds, length, outValues);
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
   * Fetch and transform String[][] values from a column.
   *
   * @param column Column name
   * @param evaluator transform evaluator
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchStringValues(String column, TransformEvaluator evaluator, int[] inDocIds, int length,
      String[][] outValues) {
    _columnValueReaderMap.get(column).readStringValuesMV(evaluator, inDocIds, length, outValues);
  }

  /**
   * Fetch the bytes values for a multi-valued column.
   *
   * @param column Column name
   * @param inDocIds Input document Ids buffer
   * @param length Number of input document Ids
   * @param outValues Buffer for output
   */
  public void fetchBytesValues(String column, int[] inDocIds, int length, byte[][][] outValues) {
    _columnValueReaderMap.get(column).readBytesValuesMV(inDocIds, length, outValues);
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
   *
   * TODO: Type conversion for BOOLEAN and TIMESTAMP is not handled
   */
  private class ColumnValueReader implements Closeable {
    final ForwardIndexReader _reader;
    final Dictionary _dictionary;
    final DataType _storedType;
    final boolean _singleValue;

    boolean _readerContextCreated;
    ForwardIndexReaderContext _readerContext;

    ColumnValueReader(ForwardIndexReader reader, @Nullable Dictionary dictionary) {
      _reader = reader;
      _dictionary = dictionary;
      _storedType = reader.getStoredType();
      _singleValue = reader.isSingleValue();
    }

    private ForwardIndexReaderContext getReaderContext() {
      // Create reader context lazily to reduce the duration of existence
      if (!_readerContextCreated) {
        _readerContext = _reader.createContext();
        _readerContextCreated = true;
      }
      return _readerContext;
    }

    void readDictIds(int[] docIds, int length, int[] dictIdBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      _reader.readDictIds(docIds, length, dictIdBuffer, getReaderContext());
    }

    void readIntValues(int[] docIds, int length, int[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, readerContext);
        _dictionary.readIntValues(dictIdBuffer, length, valueBuffer);
      } else {
        _reader.readValuesSV(docIds, length, valueBuffer, readerContext);
      }
    }

    void readIntValues(TransformEvaluator evaluator, int[] docIds, int length, int[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valueBuffer);
    }

    void readLongValues(int[] docIds, int length, long[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, readerContext);
        _dictionary.readLongValues(dictIdBuffer, length, valueBuffer);
      } else {
        _reader.readValuesSV(docIds, length, valueBuffer, readerContext);
      }
    }

    void readLongValues(TransformEvaluator evaluator, int[] docIds, int length, long[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valueBuffer);
    }

    void readFloatValues(int[] docIds, int length, float[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, readerContext);
        _dictionary.readFloatValues(dictIdBuffer, length, valueBuffer);
      } else {
        _reader.readValuesSV(docIds, length, valueBuffer, readerContext);
      }
    }

    void readFloatValues(TransformEvaluator evaluator, int[] docIds, int length, float[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valueBuffer);
    }

    void readDoubleValues(int[] docIds, int length, double[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, readerContext);
        _dictionary.readDoubleValues(dictIdBuffer, length, valueBuffer);
      } else {
        _reader.readValuesSV(docIds, length, valueBuffer, readerContext);
      }
    }

    void readDoubleValues(TransformEvaluator evaluator, int[] docIds, int length, double[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valueBuffer);
    }

    void readBigDecimalValues(int[] docIds, int length, BigDecimal[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, readerContext);
        _dictionary.readBigDecimalValues(dictIdBuffer, length, valueBuffer);
      } else {
        _reader.readValuesSV(docIds, length, valueBuffer, readerContext);
      }
    }

    void readBigDecimalValues(TransformEvaluator evaluator, int[] docIds, int length, BigDecimal[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valueBuffer);
    }

    void readVectorValues(int[] docIds, int length, Vector[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, readerContext);
        _dictionary.readVectorValues(dictIdBuffer, length, valueBuffer);
      } else {
        _reader.readValuesSV(docIds, length, valueBuffer, readerContext);
      }
    }

    void readVectorValues(TransformEvaluator evaluator, int[] docIds, int length, Vector[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valueBuffer);
    }

    void readStringValues(int[] docIds, int length, String[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, readerContext);
        _dictionary.readStringValues(dictIdBuffer, length, valueBuffer);
      } else {
        switch (_storedType) {
          case INT:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Integer.toString(_reader.getInt(docIds[i], readerContext));
            }
            break;
          case LONG:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Long.toString(_reader.getLong(docIds[i], readerContext));
            }
            break;
          case FLOAT:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Float.toString(_reader.getFloat(docIds[i], readerContext));
            }
            break;
          case DOUBLE:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = Double.toString(_reader.getDouble(docIds[i], readerContext));
            }
            break;
          case BIG_DECIMAL:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = _reader.getBigDecimal(docIds[i], readerContext).toPlainString();
            }
            break;
          case STRING:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = _reader.getString(docIds[i], readerContext);
            }
            break;
          case BYTES:
            for (int i = 0; i < length; i++) {
              valueBuffer[i] = BytesUtils.toHexString(_reader.getBytes(docIds[i], readerContext));
            }
            break;
          default:
            throw new IllegalStateException();
        }
      }
    }

    void readStringValues(TransformEvaluator evaluator, int[] docIds, int length, String[] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valueBuffer);
    }

    void readBytesValues(int[] docIds, int length, byte[][] valueBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        int[] dictIdBuffer = THREAD_LOCAL_DICT_IDS.get();
        _reader.readDictIds(docIds, length, dictIdBuffer, readerContext);
        _dictionary.readBytesValues(dictIdBuffer, length, valueBuffer);
      } else {
        for (int i = 0; i < length; i++) {
          valueBuffer[i] = _reader.getBytes(docIds[i], readerContext);
        }
      }
    }

    void readDictIdsMV(int[] docIds, int length, int[][] dictIdsBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      for (int i = 0; i < length; i++) {
        int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, readerContext);
        dictIdsBuffer[i] = Arrays.copyOfRange(_reusableMVDictIds, 0, numValues);
      }
    }

    void readIntValuesMV(int[] docIds, int length, int[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        for (int i = 0; i < length; i++) {
          int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, readerContext);
          int[] values = new int[numValues];
          _dictionary.readIntValues(_reusableMVDictIds, numValues, values);
          valuesBuffer[i] = values;
        }
      } else {
        _reader.readValuesMV(docIds, length, _maxNumValuesPerMVEntry, valuesBuffer, readerContext);
      }
    }

    void readIntValuesMV(TransformEvaluator evaluator, int[] docIds, int length, int[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valuesBuffer);
    }

    void readLongValuesMV(int[] docIds, int length, long[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        for (int i = 0; i < length; i++) {
          int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, readerContext);
          long[] values = new long[numValues];
          _dictionary.readLongValues(_reusableMVDictIds, numValues, values);
          valuesBuffer[i] = values;
        }
      } else {
        _reader.readValuesMV(docIds, length, _maxNumValuesPerMVEntry, valuesBuffer, readerContext);
      }
    }

    void readLongValuesMV(TransformEvaluator evaluator, int[] docIds, int length, long[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valuesBuffer);
    }

    void readFloatValuesMV(int[] docIds, int length, float[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        for (int i = 0; i < length; i++) {
          int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, readerContext);
          float[] values = new float[numValues];
          _dictionary.readFloatValues(_reusableMVDictIds, numValues, values);
          valuesBuffer[i] = values;
        }
      } else {
        _reader.readValuesMV(docIds, length, _maxNumValuesPerMVEntry, valuesBuffer, readerContext);
      }
    }

    void readFloatValuesMV(TransformEvaluator evaluator, int[] docIds, int length, float[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valuesBuffer);
    }

    void readDoubleValuesMV(int[] docIds, int length, double[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        for (int i = 0; i < length; i++) {
          int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, readerContext);
          double[] values = new double[numValues];
          _dictionary.readDoubleValues(_reusableMVDictIds, numValues, values);
          valuesBuffer[i] = values;
        }
      } else {
        _reader.readValuesMV(docIds, length, _maxNumValuesPerMVEntry, valuesBuffer, readerContext);
      }
    }

    void readDoubleValuesMV(TransformEvaluator evaluator, int[] docIds, int length, double[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valuesBuffer);
    }

    void readStringValuesMV(int[] docIds, int length, String[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        for (int i = 0; i < length; i++) {
          int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, readerContext);
          String[] values = new String[numValues];
          _dictionary.readStringValues(_reusableMVDictIds, numValues, values);
          valuesBuffer[i] = values;
        }
      } else {
        _reader.readValuesMV(docIds, length, _maxNumValuesPerMVEntry, valuesBuffer, readerContext);
      }
    }

    void readStringValuesMV(TransformEvaluator evaluator, int[] docIds, int length, String[][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      evaluator.evaluateBlock(docIds, length, _reader, getReaderContext(), _dictionary, getSVDictIdsBuffer(),
          valuesBuffer);
    }

    void readBytesValuesMV(int[] docIds, int length, byte[][][] valuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      ForwardIndexReaderContext readerContext = getReaderContext();
      if (_dictionary != null) {
        for (int i = 0; i < length; i++) {
          int numValues = _reader.getDictIdMV(docIds[i], _reusableMVDictIds, readerContext);
          byte[][] values = new byte[numValues][];
          _dictionary.readBytesValues(_reusableMVDictIds, numValues, values);
          valuesBuffer[i] = values;
        }
      } else {
        _reader.readValuesMV(docIds, length, _maxNumValuesPerMVEntry, valuesBuffer, readerContext);
      }
    }

    public void readNumValuesMV(int[] docIds, int length, int[] numValuesBuffer) {
      Tracing.activeRecording().setInputDataType(_storedType, _singleValue);
      for (int i = 0; i < length; i++) {
        numValuesBuffer[i] = _reader.getNumValuesMV(docIds[i], getReaderContext());
      }
    }

    private int[] getSVDictIdsBuffer() {
      return _dictionary == null ? null : THREAD_LOCAL_DICT_IDS.get();
    }

    @Override
    public void close()
        throws IOException {
      if (_readerContext != null) {
        _readerContext.close();
      }
    }
  }
}
