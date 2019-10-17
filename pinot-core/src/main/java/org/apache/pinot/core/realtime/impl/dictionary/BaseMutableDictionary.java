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
package org.apache.pinot.core.realtime.impl.dictionary;

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.pinot.core.segment.index.readers.BaseDictionary;


public abstract class BaseMutableDictionary extends BaseDictionary {

  public boolean isSorted() {
    return false;
  }

  /**
   * Indexes a single-value entry (a value of the dictionary type) into the dictionary, and returns the dictId of the
   * value.
   */
  public abstract int index(Object value);

  /**
   * Indexes a multi-value entry (an array of values of the dictionary type) into the dictionary, and returns an array
   * of dictIds for each value.
   */
  public abstract int[] index(Object[] values);

  /**
   * Returns the comparison result of value for dictId 1 and dictId 2, i.e. {@code value1.compareTo(value2)}.
   */
  public abstract int compare(int dictId1, int dictId2);

  /**
   * Returns a set of dictIds in the given value range, where lower/upper bound can be "*" which indicates unbounded
   * range. This API is for range predicate evaluation.
   */
  public abstract IntSet getDictIdsInRange(String lower, String upper, boolean includeLower, boolean includeUpper);

  /**
   * Returns the minimum value in the dictionary. Note that for type BYTES, {@code ByteArray} will be returned. This API
   * is for stats collection and will be called after all values are inserted.
   */
  public abstract Object getMinVal();

  /**
   * Returns the maximum value in the dictionary. Note that for type BYTES, {@code ByteArray} will be returned. This API
   * is for stats collection and will be called after all values are inserted.
   */
  public abstract Object getMaxVal();

  /**
   * Returns an sorted array of all values in the dictionary. For type INT/LONG/FLOAT/DOUBLE, primitive type array will
   * be returned; for type STRING, {@code String[]} will be returned; for type BYTES, {@code ByteArray[]} will be
   * returned. This API is for stats collection and will be called after all values are inserted.
   */
  public abstract Object getSortedValues();
}
