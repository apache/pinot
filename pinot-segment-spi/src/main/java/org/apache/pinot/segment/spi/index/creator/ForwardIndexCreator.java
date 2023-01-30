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
package org.apache.pinot.segment.spi.index.creator;

import java.io.IOException;
import java.math.BigDecimal;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexCreator;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Interface for forward index creator.
 */
public interface ForwardIndexCreator extends IndexCreator {

  @Override
  default void addSingleValueCell(@Nonnull Object cellValue, int dictId, @Nullable Object alternative) {
    if (dictId >= 0) {
      putDictId(dictId);
    } else {
      Object value = alternative == null ? cellValue : alternative;
      switch (getValueType()) {
        case INT:
          putInt((int) value);
          break;
        case LONG:
          putLong((long) value);
          break;
        case FLOAT:
          putFloat((float) value);
          break;
        case DOUBLE:
          putDouble((double) value);
          break;
        case BIG_DECIMAL:
          putBigDecimal((BigDecimal) value);
          break;
        case STRING:
          putString((String) value);
          break;
        case BYTES:
          putBytes((byte[]) value);
          break;
        case JSON:
          if (value instanceof String) {
            putString((String) value);
          } else if (value instanceof byte[]) {
            putBytes((byte[]) value);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  default void addMultiValueCell(@Nonnull Object[] cellValues, @Nullable int[] dictIds, Object[] alternative)
      throws IOException {
    if (dictIds != null) {
      putDictIdMV(dictIds);
    } else {
      Object[] values = alternative == null ? cellValues : alternative;
      int length = values.length;
      // TODO: The new int[], long[], etc objects are not actually used as arrays iterated again, so we could skip the
      //  array copy by calling an add(Object[]) method and having overloaded indexes for each type.
      // TODO: Longer methods are not optimized by C2 JIT. We should try to move each loop to a specific method.
      switch (getValueType()) {
        case INT:
          int[] ints = new int[length];
          for (int i = 0; i < length; i++) {
            ints[i] = (Integer) values[i];
          }
          putIntMV(ints);
          break;
        case LONG:
          long[] longs = new long[length];
          for (int i = 0; i < length; i++) {
            longs[i] = (Long) values[i];
          }
          putLongMV(longs);
          break;
        case FLOAT:
          float[] floats = new float[length];
          for (int i = 0; i < length; i++) {
            floats[i] = (Float) values[i];
          }
          putFloatMV(floats);
          break;
        case DOUBLE:
          double[] doubles = new double[length];
          for (int i = 0; i < length; i++) {
            doubles[i] = (Double) values[i];
          }
          putDoubleMV(doubles);
          break;
        case STRING:
          if (values instanceof String[]) {
            putStringMV((String[]) values);
          } else {
            String[] strings = new String[length];
            for (int i = 0; i < length; i++) {
              strings[i] = (String) values[i];
            }
            putStringMV(strings);
          }
          break;
        case BYTES:
          if (values instanceof byte[][]) {
            putBytesMV((byte[][]) values);
          } else {
            byte[][] bytesArray = new byte[length][];
            for (int i = 0; i < length; i++) {
              bytesArray[i] = (byte[]) values[i];
            }
            putBytesMV(bytesArray);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

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
  DataType getValueType();

  /**
   * DICTIONARY-ENCODED INDEX APIs
   */

  /**
   * Writes the dictionary id for the next single-value into the forward index.
   *
   * @param dictId Document id to write
   */
  default void putDictId(int dictId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the dictionary ids for the next multi-value into the forward index.
   *
   * @param dictIds Document ids to write
   */
  default void putDictIdMV(int[] dictIds) {
    throw new UnsupportedOperationException();
  }

  /**
   * SINGLE-VALUE COLUMN RAW INDEX APIs
   */

  /**
   * Writes the next INT type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putInt(int value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next LONG type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putLong(long value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next FLOAT type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putFloat(float value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next DOUBLE type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putDouble(double value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next BIG_DECIMAL type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putBigDecimal(BigDecimal value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next STRING type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putString(String value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next BYTES type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putBytes(byte[] value) {
    throw new UnsupportedOperationException();
  }

  /**
   * MULTI-VALUE COLUMN RAW INDEX APIs
   * TODO: Not supported yet
   */

  /**
   * Writes the next INT type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putIntMV(int[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next LONG type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putLongMV(long[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next FLOAT type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putFloatMV(float[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next DOUBLE type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putDoubleMV(double[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next STRING type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putStringMV(String[] values) {
    throw new UnsupportedOperationException();
  }

  /**
   * Writes the next byte[] type multi-value into the forward index.
   *
   * @param values Values to write
   */
  default void putBytesMV(byte[][] values) {
    throw new UnsupportedOperationException();
  }
}
