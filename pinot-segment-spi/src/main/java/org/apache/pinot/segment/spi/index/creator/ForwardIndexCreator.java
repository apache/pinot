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
import org.apache.pinot.spi.data.readers.Vector;


/**
 * Interface for forward index creator.
 */
public interface ForwardIndexCreator extends IndexCreator {

  @Override
  default void seal()
      throws IOException {
  }

  @Override
  default void add(@Nonnull Object cellValue, int dictId) {
    if (dictId >= 0) {
      putDictId(dictId);
    } else {
      switch (getValueType()) {
        case INT:
          putInt((int) cellValue);
          break;
        case LONG:
          putLong((long) cellValue);
          break;
        case FLOAT:
          putFloat((float) cellValue);
          break;
        case DOUBLE:
          putDouble((double) cellValue);
          break;
        case BIG_DECIMAL:
          putBigDecimal((BigDecimal) cellValue);
          break;
        case STRING:
          putString((String) cellValue);
          break;
        case BYTES:
          putBytes((byte[]) cellValue);
          break;
        case VECTOR:
          putVector((Vector) cellValue);
          break;
        case JSON:
          if (cellValue instanceof String) {
            putString((String) cellValue);
          } else if (cellValue instanceof byte[]) {
            putBytes((byte[]) cellValue);
          }
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Override
  default void add(@Nonnull Object[] cellValues, @Nullable int[] dictIds)
      throws IOException {
    if (dictIds != null) {
      putDictIdMV(dictIds);
    } else {
      int length = cellValues.length;
      // TODO: The new int[], long[], etc objects are not actually used as arrays iterated again, so we could skip the
      //  array copy by calling an add(Object[]) method and having overloaded indexes for each type.
      // TODO: Longer methods are not optimized by C2 JIT. We should try to move each loop to a specific method.
      switch (getValueType()) {
        case INT:
          int[] ints = new int[length];
          for (int i = 0; i < length; i++) {
            ints[i] = (Integer) cellValues[i];
          }
          putIntMV(ints);
          break;
        case LONG:
          long[] longs = new long[length];
          for (int i = 0; i < length; i++) {
            longs[i] = (Long) cellValues[i];
          }
          putLongMV(longs);
          break;
        case FLOAT:
          float[] floats = new float[length];
          for (int i = 0; i < length; i++) {
            floats[i] = (Float) cellValues[i];
          }
          putFloatMV(floats);
          break;
        case DOUBLE:
          double[] doubles = new double[length];
          for (int i = 0; i < length; i++) {
            doubles[i] = (Double) cellValues[i];
          }
          putDoubleMV(doubles);
          break;
        case STRING:
          if (cellValues instanceof String[]) {
            putStringMV((String[]) cellValues);
          } else {
            String[] strings = new String[length];
            for (int i = 0; i < length; i++) {
              strings[i] = (String) cellValues[i];
            }
            putStringMV(strings);
          }
          break;
        case BYTES:
          if (cellValues instanceof byte[][]) {
            putBytesMV((byte[][]) cellValues);
          } else {
            byte[][] bytesArray = new byte[length][];
            for (int i = 0; i < length; i++) {
              bytesArray[i] = (byte[]) cellValues[i];
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
   * Writes the next VECTOR type single-value into the forward index.
   *
   * @param value Value to write
   */
  default void putVector(Vector value) {
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
