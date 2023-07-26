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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * This is the index used to create range indexes
 */
public interface CombinedInvertedIndexCreator
    extends DictionaryBasedInvertedIndexCreator, RawValueBasedInvertedIndexCreator {

  FieldSpec.DataType getDataType();

  @Override
  default void add(@Nonnull Object value, int dictId) {
    if (dictId >= 0) {
      add(dictId);
    } else {
      switch (getDataType()) {
        case INT:
          add((Integer) value);
          break;
        case LONG:
          add((Long) value);
          break;
        case FLOAT:
          add((Float) value);
          break;
        case DOUBLE:
          add((Double) value);
          break;
        default:
          throw new RuntimeException("Unsupported data type " + getDataType() + " for range index");
      }
    }
  }

  @Override
  default void add(@Nonnull Object[] values, @Nullable int[] dictIds) {
    if (dictIds != null) {
      add(dictIds, dictIds.length);
    } else {
      switch (getDataType()) {
        case INT:
          int[] intValues = new int[values.length];
          for (int i = 0; i < values.length; i++) {
            intValues[i] = (Integer) values[i];
          }
          add(intValues, values.length);
          break;
        case LONG:
          long[] longValues = new long[values.length];
          for (int i = 0; i < values.length; i++) {
            longValues[i] = (Long) values[i];
          }
          add(longValues, values.length);
          break;
        case FLOAT:
          float[] floatValues = new float[values.length];
          for (int i = 0; i < values.length; i++) {
            floatValues[i] = (Float) values[i];
          }
          add(floatValues, values.length);
          break;
        case DOUBLE:
          double[] doubleValues = new double[values.length];
          for (int i = 0; i < values.length; i++) {
            doubleValues[i] = (Double) values[i];
          }
          add(doubleValues, values.length);
          break;
        default:
          throw new RuntimeException("Unsupported data type " + getDataType() + " for range index");
      }
    }
  }
}
