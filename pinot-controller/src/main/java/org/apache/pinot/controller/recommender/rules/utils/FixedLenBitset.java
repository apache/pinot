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
package org.apache.pinot.controller.recommender.rules.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.controller.recommender.rules.impl.InvertedSortedIndexJointRule;


/**
 * A simple bitset implementation for performance, to hold the candidate dimensions for indices.
 * For fast enumeration of the power set of dimension set in the index recommendation across all queries
 * in {@link InvertedSortedIndexJointRule#findOptimalCombination(List)}.
 */
public class FixedLenBitset {
  public static final int NO_INDEX_APPLICABLE = 0;
  private long[] _bytes;
  private final int _lenOfArray;
  private final int _size;
  private int _cardinality;
  private static final int[] num_to_bits = {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4};
  public static final FixedLenBitset IMMUTABLE_EMPTY_SET = new FixedLenBitset();
  public static final int SIZE_OF_LONG = 64;

  private FixedLenBitset() {
    this._size = 0;
    this._lenOfArray = 0;
    this._bytes = null;
    this._cardinality = 0;
  }

  public boolean isEmpty() {
    return _cardinality == 0;
  }

  public List<Integer> getOffsets() {
    if (isEmpty())
      return Collections.emptyList();
    List<Integer> ret = new ArrayList<>();
    for (int i = 0; i < _size; i++) {
      if (contains(i))
        ret.add(i);
    }
    return ret;
  }

  /**
   * Fast algorithm to compute the number of bits set in a long
   * @param num long
   * @return the number of bits set in num
   */
  static int countSetBitsRec(long num) {
    int ret = 0;
    while (num != 0) {
      int nibble = (int) (num & 0xf);
      ret += num_to_bits[nibble];
      num = num >>> 4;
    }
    return ret;
  }

  public int getSize() {
    return _size;
  }

  /**
   *
   * @return number of bits set in the current bitset
   */
  public int getCardinality() {
    return _cardinality;
  }

  public FixedLenBitset(int size) {
    this._size = size;
    this._lenOfArray = (size + SIZE_OF_LONG - 1) / SIZE_OF_LONG;
    this._bytes = new long[_lenOfArray];
    this._cardinality = 0;
  }

  /**
   * Add an integer to this set.
   * @param n integer to add, should be 0<=n<_size
   */
  public void add(int n) {
    if (n >= 0 && n < _size && !contains(n)) {
      _bytes[n / SIZE_OF_LONG] |= 1L << (n % SIZE_OF_LONG);
      _cardinality++;
    }
  }

  /**
   * Test if an integer exists in the current set
   * @param n integer to test
   */
  public boolean contains(int n) {
    return (_bytes[n / SIZE_OF_LONG] & 1L << (n % SIZE_OF_LONG)) != 0;
  }

  public boolean hasCandidateDim() {
    return _cardinality != 0;
  }

  /**
   * To test if this set contains(>=) another set b
   */
  public boolean contains(FixedLenBitset b) {
    if (b._cardinality == NO_INDEX_APPLICABLE)
      return true;
    for (int i = 0; i < _lenOfArray; i++) {
      if ((b._bytes[i] & _bytes[i]) != b._bytes[i]) {
        return false;
      }
    }
    return true;
  }

  /**
   * The or of this and another bitset b. Results in current bitset.
    * @param b another bitset
   * @return this bitset
   */
  public FixedLenBitset union(FixedLenBitset b) {
    if (b._cardinality == NO_INDEX_APPLICABLE)
      return this;
    for (int i = 0; i < _lenOfArray; i++) {
      _bytes[i] |= b._bytes[i];
    }
    _cardinality = 0;
    for (int i = 0; i < _lenOfArray; i++) {
      _cardinality += countSetBitsRec(_bytes[i]);
    }
    return this;
  }

  public FixedLenBitset intersect(FixedLenBitset b) {
    if (b._cardinality == NO_INDEX_APPLICABLE) {
      Arrays.fill(this._bytes, 0);
      return this;
    }
    for (int i = 0; i < _lenOfArray; i++) {
      _bytes[i] &= b._bytes[i];
    }
    _cardinality = 0;
    for (int i = 0; i < _lenOfArray; i++) {
      _cardinality += countSetBitsRec(_bytes[i]);
    }
    return this;
  }

  @Override
  public String toString() {
    //    StringBuilder stringBuilder = new StringBuilder();
    //    for (int i = _lenOfArray-1; i >=0 ; i--) {
    //      stringBuilder.append(String.format("%16s", Long.toHexString(_bytes[i])).replace(" ", "0"));
    //    }
    return "{" + getOffsets() + '}';
  }
}
