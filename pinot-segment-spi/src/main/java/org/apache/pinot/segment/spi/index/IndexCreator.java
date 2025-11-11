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

package org.apache.pinot.segment.spi.index;

import java.io.Closeable;
import java.io.IOException;
import javax.annotation.Nullable;


/**
 * The interface used to create indexes.
 *
 * The lifecycle for an IndexCreator is:
 * <ol>
 *   <li>To be created.</li>
 *   <li>Zero or more calls to either {@link #add(Object, int)} or {@link #add(Object[], int[])} (but not mix them).
 *   Calls to add methods must be done in document id order, starting from the first document id.</li>
 *   <li>A call to {@link #seal()}</li>
 *   <li>A call to {@link #close()}</li>
 * </ol>
 */
public interface IndexCreator extends Closeable {
  /**
   * Adds the given single value cell to the index.
   *
   * Rows will be added in docId order, starting with the one with docId 0.
   *
   * @param value The nonnull value of the cell. In case the cell was actually null, a default value is received instead
   * @param dictId An optional dictionary value of the cell. If there is no dictionary, -1 is received
   */
  void add(Object value, int dictId)
      throws IOException;

  /**
   * Adds the given multi value cell to the index
   *
   * Rows will be added in docId order, starting with the one with docId 0.
   *
   * @param values The nonnull value of the cell. In case the cell was actually null, an empty array is received instead
   * @param dictIds An optional array of dictionary values. If there is no dictionary, null is received.
   */
  void add(Object[] values, @Nullable int[] dictIds)
      throws IOException;

  void seal()
      throws IOException;

  /**
   * Primitive type additions for columnar processing optimization.
   * These methods avoid boxing overhead when iterating over columnar data.
   * Default implementation boxes the value for backward compatibility.
   */

  default void addInt(int value, int dictId)
      throws IOException {
    add(value, dictId);
  }

  default void addLong(long value, int dictId)
      throws IOException {
    add(value, dictId);
  }

  default void addFloat(float value, int dictId)
      throws IOException {
    add(value, dictId);
  }

  default void addDouble(double value, int dictId)
      throws IOException {
    add(value, dictId);
  }

  default void addString(String value, int dictId)
      throws IOException {
    add(value, dictId);
  }

  default void addBytes(byte[] value, int dictId)
      throws IOException {
    add(value, dictId);
  }

  // The default implementations box the values for backward compatibility.
  // This is extremely inefficient because the implementations of add(Object[], int[]) method will end up
  // unboxing them again to write to the index.
  default void addIntMV(int[] values, @Nullable int[] dictIds)
      throws IOException {
    Integer[] boxedValues = new Integer[values.length];
    for (int i = 0; i < values.length; i++) {
      boxedValues[i] = values[i];
    }
    add(boxedValues, dictIds);
  }

  default void addLongMV(long[] values, @Nullable int[] dictIds)
      throws IOException {
    Long[] boxedValues = new Long[values.length];
    for (int i = 0; i < values.length; i++) {
      boxedValues[i] = values[i];
    }
    add(boxedValues, dictIds);
  }

  default void addFloatMV(float[] values, @Nullable int[] dictIds)
      throws IOException {
    Float[] boxedValues = new Float[values.length];
    for (int i = 0; i < values.length; i++) {
      boxedValues[i] = values[i];
    }
    add(boxedValues, dictIds);
  }

  default void addDoubleMV(double[] values, @Nullable int[] dictIds)
      throws IOException {
    Double[] boxedValues = new Double[values.length];
    for (int i = 0; i < values.length; i++) {
      boxedValues[i] = values[i];
    }
    add(boxedValues, dictIds);
  }

  default void addStringMV(String[] values, @Nullable int[] dictIds)
      throws IOException {
    add(values, dictIds);
  }

  default void addBytesMV(byte[][] values, @Nullable int[] dictIds)
      throws IOException {
    add(values, dictIds);
  }
}
