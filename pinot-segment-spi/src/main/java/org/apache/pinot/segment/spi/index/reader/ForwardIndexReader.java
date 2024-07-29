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
package org.apache.pinot.segment.spi.index.reader;

import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.DictIdCompressionType;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;


/**
 * Interface for forward index reader.
 *
 * @param <T> Type of the ReaderContext
 */
public interface ForwardIndexReader<T extends ForwardIndexReaderContext> extends IndexReader {

  /**
   * Returns {@code true} if the forward index is dictionary-encoded, {@code false} if it is raw.
   */
  boolean isDictionaryEncoded();

  /**
   * Returns {@code true} if the forward index is for a single-value column, {@code false} if it is for a multi-value
   * column.
   */
  boolean isSingleValue();

  /**
   * Returns the data type of the values in the forward index. Returns {@link DataType#INT} for dictionary-encoded
   * forward index.
   */
  DataType getStoredType();

  /**
   * Returns the compression type (if valid). Only valid for RAW forward index columns implemented in
   * BaseChunkForwardIndexReader.
   */
  @Nullable
  default ChunkCompressionType getCompressionType() {
    return null;
  }

  /**
   * Returns the compression type for dictionary encoded forward index.
   */
  @Nullable
  default DictIdCompressionType getDictIdCompressionType() {
    return null;
  }

  /**
   * Returns the length of the longest entry. Only valid for RAW forward index columns implemented in
   * BaseChunkForwardIndexReader. Returns -1 otherwise.
   * @return
   */
  default int getLengthOfLongestEntry() {
    return -1;
  }

  /**
   * Creates a new {@link ForwardIndexReaderContext} of the reader which can be used to accelerate the reads.
   * NOTE: Caller is responsible for closing the returned reader context.
   */
  @Nullable
  default T createContext() {
    return null;
  }

  /**
   * DICTIONARY-ENCODED INDEX APIs
   */

  /**
   * Reads the dictionary id for a single-value column at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return Dictionary id at the given document id
   */
  default int getDictId(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Batch reads multiple dictionary ids for a single-value column at the given document ids into the passed in buffer
   * (the buffer size must be larger than or equal to the length).
   *
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param dictIdBuffer Dictionary id buffer
   * @param context Reader context
   */
  default void readDictIds(int[] docIds, int length, int[] dictIdBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the dictionary ids for a multi-value column at the given document id into the passed in buffer (the buffer
   * size must be enough to hold all the values for the multi-value entry) and returns the number of values within the
   * multi-value entry.
   *
   * @param docId Document id
   * @param dictIdBuffer Dictionary id buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getDictIdMV(int docId, int[] dictIdBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the dictionary ids for a multi-value column at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return Dictionary ids at the given document id
   */
  default int[] getDictIdMV(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * SINGLE-VALUE COLUMN RAW INDEX APIs
   */

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesSV(int[] docIds, int length, int[] values, T context) {
    switch (getStoredType()) {
      case INT:
        for (int i = 0; i < length; i++) {
          values[i] = getInt(docIds[i], context);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          values[i] = (int) getLong(docIds[i], context);
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          values[i] = (int) getFloat(docIds[i], context);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < length; i++) {
          values[i] = (int) getDouble(docIds[i], context);
        }
        break;
      case BIG_DECIMAL:
        for (int i = 0; i < length; i++) {
          values[i] = getBigDecimal(docIds[i], context).intValue();
        }
        break;
      case STRING:
        for (int i = 0; i < length; i++) {
          values[i] = Integer.parseInt(getString(docIds[i], context));
        }
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesSV(int[] docIds, int length, long[] values, T context) {
    switch (getStoredType()) {
      case INT:
        for (int i = 0; i < length; i++) {
          values[i] = getInt(docIds[i], context);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          values[i] = getLong(docIds[i], context);
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          values[i] = (long) getFloat(docIds[i], context);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < length; i++) {
          values[i] = (long) getDouble(docIds[i], context);
        }
        break;
      case BIG_DECIMAL:
        for (int i = 0; i < length; i++) {
          values[i] = getBigDecimal(docIds[i], context).longValue();
        }
        break;
      case STRING:
        for (int i = 0; i < length; i++) {
          values[i] = Long.parseLong(getString(docIds[i], context));
        }
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesSV(int[] docIds, int length, float[] values, T context) {
    switch (getStoredType()) {
      case INT:
        for (int i = 0; i < length; i++) {
          values[i] = getInt(docIds[i], context);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          values[i] = getLong(docIds[i], context);
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          values[i] = getFloat(docIds[i], context);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < length; i++) {
          values[i] = (float) getDouble(docIds[i], context);
        }
        break;
      case BIG_DECIMAL:
        for (int i = 0; i < length; i++) {
          values[i] = getBigDecimal(docIds[i], context).floatValue();
        }
        break;
      case STRING:
        for (int i = 0; i < length; i++) {
          values[i] = Float.parseFloat(getString(docIds[i], context));
        }
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesSV(int[] docIds, int length, double[] values, T context) {
    switch (getStoredType()) {
      case INT:
        for (int i = 0; i < length; i++) {
          values[i] = getInt(docIds[i], context);
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          values[i] = getLong(docIds[i], context);
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          values[i] = getFloat(docIds[i], context);
        }
        break;
      case DOUBLE:
        for (int i = 0; i < length; i++) {
          values[i] = getDouble(docIds[i], context);
        }
        break;
      case BIG_DECIMAL:
        for (int i = 0; i < length; i++) {
          values[i] = getBigDecimal(docIds[i], context).doubleValue();
        }
        break;
      case STRING:
        for (int i = 0; i < length; i++) {
          values[i] = Double.parseDouble(getString(docIds[i], context));
        }
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesSV(int[] docIds, int length, BigDecimal[] values, T context) {
    // todo(nhejazi): add raw index support to the BIG_DECIMAL type. In most of the cases, it will be more efficient
    //  to store big decimal as raw.
    switch (getStoredType()) {
      case INT:
        for (int i = 0; i < length; i++) {
          values[i] = BigDecimal.valueOf(getInt(docIds[i], context));
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          values[i] = BigDecimal.valueOf(getLong(docIds[i], context));
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          values[i] = BigDecimal.valueOf(getFloat(docIds[i], context));
        }
        break;
      case DOUBLE:
        for (int i = 0; i < length; i++) {
          values[i] = BigDecimal.valueOf(getDouble(docIds[i], context));
        }
        break;
      case BIG_DECIMAL:
        for (int i = 0; i < length; i++) {
          values[i] = getBigDecimal(docIds[i], context);
        }
        break;
      case STRING:
        for (int i = 0; i < length; i++) {
          values[i] = new BigDecimal(getString(docIds[i], context));
        }
        break;
      case BYTES:
        for (int i = 0; i < length; i++) {
          values[i] = BigDecimalUtils.deserialize(getBytes(docIds[i], context));
        }
        break;
      default:
        throw new IllegalArgumentException();
    }
  }

  /**
   * Reads the INT value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return INT type single-value at the given document id
   */
  default int getInt(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the LONG type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return LONG type single-value at the given document id
   */
  default long getLong(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the FLOAT type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return FLOAT type single-value at the given document id
   */
  default float getFloat(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the DOUBLE type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return DOUBLE type single-value at the given document id
   */
  default double getDouble(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the BIG_DECIMAL type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return BIG_DECIMAL type single-value at the given document id
   */
  default BigDecimal getBigDecimal(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the STRING type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return STRING type single-value at the given document id
   */
  default String getString(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the BYTES type single-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return BYTES type single-value at the given document id
   */
  default byte[] getBytes(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * MULTI-VALUE COLUMN RAW INDEX APIs
   */

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param maxNumValuesPerMVEntry Maximum number of values per MV entry
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesMV(int[] docIds, int length, int maxNumValuesPerMVEntry, int[][] values, T context) {
    switch (getStoredType()) {
      case INT:
        for (int i = 0; i < length; i++) {
          values[i] = getIntMV(docIds[i], context);
        }
        break;
      case LONG:
        long[] longValueBuffer = new long[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getLongMV(docIds[i], longValueBuffer, context);
          values[i] = new int[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = (int) longValueBuffer[j];
          }
        }
        break;
      case FLOAT:
        float[] floatValueBuffer = new float[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getFloatMV(docIds[i], floatValueBuffer, context);
          values[i] = new int[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = (int) floatValueBuffer[j];
          }
        }
        break;
      case DOUBLE:
        double[] doubleValueBuffer = new double[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getDoubleMV(docIds[i], doubleValueBuffer, context);
          values[i] = new int[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = (int) doubleValueBuffer[j];
          }
        }
        break;
      case STRING:
        String[] stringValueBuffer = new String[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getStringMV(docIds[i], stringValueBuffer, context);
          values[i] = new int[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = Integer.parseInt(stringValueBuffer[j]);
          }
        }
        break;
      default:
        throw new IllegalArgumentException("readValuesMV not supported for type " + getStoredType());
    }
  }

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param maxNumValuesPerMVEntry Maximum number of values per MV entry
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesMV(int[] docIds, int length, int maxNumValuesPerMVEntry, long[][] values, T context) {
    switch (getStoredType()) {
      case INT:
        int[] intValueBuffer = new int[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getIntMV(docIds[i], intValueBuffer, context);
          values[i] = new long[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = intValueBuffer[j];
          }
        }
        break;
      case LONG:
        for (int i = 0; i < length; i++) {
          values[i] = getLongMV(docIds[i], context);
        }
        break;
      case FLOAT:
        float[] floatValueBuffer = new float[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getFloatMV(docIds[i], floatValueBuffer, context);
          values[i] = new long[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = (long) floatValueBuffer[j];
          }
        }
        break;
      case DOUBLE:
        double[] doubleValueBuffer = new double[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getDoubleMV(docIds[i], doubleValueBuffer, context);
          values[i] = new long[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = (long) doubleValueBuffer[j];
          }
        }
        break;
      case STRING:
        String[] stringValueBuffer = new String[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getStringMV(docIds[i], stringValueBuffer, context);
          values[i] = new long[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = Long.parseLong(stringValueBuffer[j]);
          }
        }
        break;
      default:
        throw new IllegalArgumentException("readValuesMV not supported for type " + getStoredType());
    }
  }

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param maxNumValuesPerMVEntry Maximum number of values per MV entry
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesMV(int[] docIds, int length, int maxNumValuesPerMVEntry, float[][] values, T context) {
    switch (getStoredType()) {
      case INT:
        int[] intValueBuffer = new int[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getIntMV(docIds[i], intValueBuffer, context);
          values[i] = new float[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = intValueBuffer[j];
          }
        }
        break;
      case LONG:
        long[] longValueBuffer = new long[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getLongMV(docIds[i], longValueBuffer, context);
          values[i] = new float[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = longValueBuffer[j];
          }
        }
        break;
      case FLOAT:
        for (int i = 0; i < length; i++) {
          values[i] = getFloatMV(docIds[i], context);
        }
        break;
      case DOUBLE:
        double[] doubleValueBuffer = new double[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getDoubleMV(docIds[i], doubleValueBuffer, context);
          values[i] = new float[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = (float) doubleValueBuffer[j];
          }
        }
        break;
      case STRING:
        String[] stringValueBuffer = new String[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getStringMV(docIds[i], stringValueBuffer, context);
          values[i] = new float[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = Float.parseFloat(stringValueBuffer[j]);
          }
        }
        break;
      default:
        throw new IllegalArgumentException("readValuesMV not supported for type " + getStoredType());
    }
  }

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param maxNumValuesPerMVEntry Maximum number of values per MV entry
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesMV(int[] docIds, int length, int maxNumValuesPerMVEntry, double[][] values, T context) {
    switch (getStoredType()) {
      case INT:
        int[] intValueBuffer = new int[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getIntMV(docIds[i], intValueBuffer, context);
          values[i] = new double[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = intValueBuffer[j];
          }
        }
        break;
      case LONG:
        long[] longValueBuffer = new long[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getLongMV(docIds[i], longValueBuffer, context);
          values[i] = new double[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = longValueBuffer[j];
          }
        }
        break;
      case FLOAT:
        float[] floatValueBuffer = new float[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getFloatMV(docIds[i], floatValueBuffer, context);
          values[i] = new double[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = floatValueBuffer[j];
          }
        }
        break;
      case DOUBLE:
        for (int i = 0; i < length; i++) {
          values[i] = getDoubleMV(docIds[i], context);
        }
        break;
      case STRING:
        String[] stringValueBuffer = new String[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getStringMV(docIds[i], stringValueBuffer, context);
          values[i] = new double[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = Double.parseDouble(stringValueBuffer[j]);
          }
        }
        break;
      default:
        throw new IllegalArgumentException("readValuesMV not supported for type " + getStoredType());
    }
  }

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param maxNumValuesPerMVEntry Maximum number of values per MV entry
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesMV(int[] docIds, int length, int maxNumValuesPerMVEntry, String[][] values, T context) {
    switch (getStoredType()) {
      case INT:
        int[] intValueBuffer = new int[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getIntMV(docIds[i], intValueBuffer, context);
          values[i] = new String[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = String.valueOf(intValueBuffer[j]);
          }
        }
        break;
      case LONG:
        long[] longValueBuffer = new long[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getLongMV(docIds[i], longValueBuffer, context);
          values[i] = new String[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = String.valueOf(longValueBuffer[j]);
          }
        }
        break;
      case FLOAT:
        float[] floatValueBuffer = new float[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getFloatMV(docIds[i], floatValueBuffer, context);
          values[i] = new String[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = String.valueOf(floatValueBuffer[j]);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValueBuffer = new double[maxNumValuesPerMVEntry];
        for (int i = 0; i < length; i++) {
          int numValues = getDoubleMV(docIds[i], doubleValueBuffer, context);
          values[i] = new String[numValues];
          for (int j = 0; j < numValues; j++) {
            values[i][j] = String.valueOf(doubleValueBuffer[j]);
          }
        }
        break;
      case STRING:
        for (int i = 0; i < length; i++) {
          values[i] = getStringMV(docIds[i], context);
        }
        break;
      default:
        throw new IllegalArgumentException("readValuesMV not supported for type " + getStoredType());
    }
  }

  /**
   * Fills the values
   * @param docIds Array containing the document ids to read
   * @param length Number of values to read
   * @param maxNumValuesPerMVEntry Maximum number of values per MV entry
   * @param values Values to fill
   * @param context Reader context
   */
  default void readValuesMV(int[] docIds, int length, int maxNumValuesPerMVEntry, byte[][][] values, T context) {
    for (int i = 0; i < length; i++) {
      values[i] = getBytesMV(docIds[i], context);
    }
  }

  /**
   * Reads the INT type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getIntMV(int docId, int[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the INT type multi-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return INT values at the given document id
   */
  default int[] getIntMV(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the LONG type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getLongMV(int docId, long[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the LONG type multi-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return LONG values at the given document id
   */
  default long[] getLongMV(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the FLOAT type multi-value at the given document id into the passed in value buffer (the buffer size must be
   * enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getFloatMV(int docId, float[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the FLOAT type multi-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return FLOAT values at the given document id
   */
  default float[] getFloatMV(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the DOUBLE type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getDoubleMV(int docId, double[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the DOUBLE type multi-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return DOUBLE values at the given document id
   */
  default double[] getDoubleMV(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the STRING type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getStringMV(int docId, String[] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the STRING type multi-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return STRING values at the given document id
   */
  default String[] getStringMV(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the bytes type multi-value at the given document id into the passed in value buffer (the buffer size must
   * be enough to hold all the values for the multi-value entry) and returns the number of values within the multi-value
   * entry.
   *
   * @param docId Document id
   * @param valueBuffer Value buffer
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getBytesMV(int docId, byte[][] valueBuffer, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Reads the bytes type multi-value at the given document id.
   *
   * @param docId Document id
   * @param context Reader context
   * @return BYTE values at the given document id
   */
  default byte[][] getBytesMV(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  /**
   * Gets the number of multi-values at a given document id and returns it.
   *
   * @param docId Document id
   * @param context Reader context
   * @return Number of values within the multi-value entry
   */
  default int getNumValuesMV(int docId, T context) {
    throw new UnsupportedOperationException();
  }

  // Functions for recording absolute buffer byte ranges accessed while reading a given docId

  /**
   * Returns whether the forward index supports recording the byte ranges accessed while reading a given docId.
   * For readers that do support this info, caller should check if the buffer is a {@link isFixedOffsetMappingType()}.
   * If yes, the byte range mapping for a docId can be calculated using the {@link getRawDataStartOffset()} and the
   * {@link getDocLength()} functions.
   * if not, caller should use the {@link recordDocIdByteRanges()} function to get the list of byte ranges accessed
   * for a docId.
   */
  default boolean isBufferByteRangeInfoSupported() {
    return false;
  }

  /**
   * Returns a list of {@link ByteRange} that represents all the distinct
   * buffer byte ranges (absolute offset, sizeInBytes) that are accessed when reading the given (@param docId}
   * @param docId to find the range for
   * @param context Reader context
   * @param ranges List of {@link ByteRange} to which the applicable value ranges will be added
   */
  default void recordDocIdByteRanges(int docId, T context, List<ByteRange> ranges) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns whether the forward index is of fixed length type, and therefore the docId -> byte range mapping is fixed
   * @return true if forward index has a fixed mapping of docId -> buffer offsets
   * (eg: FixedBitSVForwardIndexReader, FixedByteChunkSVForwardIndexReader (if buffer is uncompressed) etc), false
   * otherwise
   */
  default boolean isFixedOffsetMappingType() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the base offset of raw data start within the fwd index buffer, if it's of fixed offset mapping type
   * @return raw data start offset if the reader is of fixed offset mapping type
   */
  default long getRawDataStartOffset() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the length of each entry in the forward index, if it's of fixed offset mapping type
   */
  default int getDocLength() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns whether the length of each entry in the forward index is in bits, if it's of fixed offset mapping type
   */
  default boolean isDocLengthInBits() {
    return false;
  }

  /**
   * This class represents the buffer byte ranges accessed while reading a given docId.
   */
  class ByteRange {
    private final long _offset;
    private final int _sizeInBytes;

    public ByteRange(long offset, int sizeInBytes) {
      _offset = offset;
      _sizeInBytes = sizeInBytes;
    }

    public long getOffset() {
      return _offset;
    }

    public int getSizeInBytes() {
      return _sizeInBytes;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ByteRange byteRange = (ByteRange) o;
      return _offset == byteRange._offset && _sizeInBytes == byteRange._sizeInBytes;
    }

    @Override
    public int hashCode() {
      return Objects.hash(_offset, _sizeInBytes);
    }

    @Override
    public String toString() {
      return "Range{" + "_offset=" + _offset + ", _size=" + _sizeInBytes + '}';
    }
  }
}
