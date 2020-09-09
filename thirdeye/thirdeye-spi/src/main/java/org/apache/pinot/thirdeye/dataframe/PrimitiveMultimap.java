/*
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
 *
 */

package org.apache.pinot.thirdeye.dataframe;

import java.util.Arrays;


/**
 * Custom hash-based multimap implementation for joins. Primitive, fixed size, append only.
 * Minimizes memory footprint and overheads. Open-addressing, linear scan, block skip.
 */
class PrimitiveMultimap {
  private static final int M = 0x5bd1e995;
  private static final int SEED = 0xb7f93ea;
  private static final long TO_LONG = 0xFFFFFFFFL;
  private static final int INITIAL_SIZE = 1;

  public static final int RESERVED_VALUE = 0xFFFFFFFF;
  public static final double SCALING_FACTOR = 4;

  private final int maxSize;
  private final int shift;
  private final long[] data;
  private int[] writeInfo;

  private int size;
  private long collisions = 0;
  private long rereads = 0;

  private int iteratorKey = -1;
  private int iterator = -1;

  private int[] outBuffer = new int[INITIAL_SIZE];

  // 0xHHHHHHHHVVVVVVVV
  //
  // H hash
  // V value (+1)
  //
  // 0x0000000000000000    indicates empty

  /* **************************************************************************
   * Series-specific
   * *************************************************************************/
  public PrimitiveMultimap(Series... series) {
    this(series[0].size());
    Series.assertSameLength(series);
    for(int i=0; i<series[0].size(); i++) {
      putFast(hashRow(series, i), i);
    }
  }

  public int[] get(Series[] series, int row, Series[] compare) {
    int key = hashRow(series, row);
    int val = this.get(key);

    int cntr = 0;
    while(val != -1) {
      if(Series.equalsMultiple(series, compare, row, val)) {
        if(cntr >= this.outBuffer.length) {
          int[] newBuffer = new int[this.outBuffer.length * 2];
          System.arraycopy(this.outBuffer, 0, newBuffer, 0, this.outBuffer.length);
          this.outBuffer = newBuffer;
        }
        this.outBuffer[cntr++] = val;
      }
      val = this.getNext();
    }
    return Arrays.copyOf(this.outBuffer, cntr);
  }

  static int hashRow(Series[] series, int row) {
    int k = SEED;
    for(Series s : series) {
      k ^= s.hashCode(row);
    }
    return k;
  }

  /* **************************************************************************
   * Base implementation
   * *************************************************************************/
  public PrimitiveMultimap(final int maxSize) {
    this.maxSize = maxSize;

    final int minCapacity = (int)(maxSize * SCALING_FACTOR);
    this.shift = log2(minCapacity) + 1;

    final int capacity = pow2(this.shift);
    this.data = new long[capacity];

    this.writeInfo = new int[this.data.length];
  }

  public void put(final int key, final int value) {
    // NOTE: conservative - (hash, value) must be != 0
    if(value == RESERVED_VALUE)
      throw new IllegalArgumentException(String.format("Value must be different from %d", RESERVED_VALUE));
    if(this.size >= this.maxSize)
      throw new IllegalArgumentException(String.format("Map is at max size %d", this.maxSize));
    putFast(key, value);
  }

  private void putFast(final int key, final int value) {
    final int keyHash = hash(key);
    final int keyIndex = safeIndex(keyHash);

    int index = keyIndex;
    long tuple = this.data[index];
    while(tuple != 0) {
      index = this.writeInfo[index];
      tuple = this.data[index];
      this.collisions++;
    }

    this.data[index] = tuple(key, value + 1); // ensure 0 indicates empty
    this.writeInfo[index] = keyIndex;
    this.writeInfo[keyIndex] = safeIndex(index + 1);
    this.size++;
  }

  public int get(int key) {
    return get(key, 0);
  }

  public int get(int key, int offset) {
    return getInternal(key, hash(key), offset);
  }

  private int getInternal(int key, int hash, int offset) {
    int toff = 0;
    int index = safeIndex(hash);
    long tuple = this.data[index];

    while(tuple != 0) {
      int tkey = tuple2key(tuple);

      if(tkey == key) {
        if(offset == toff++) {
          this.iteratorKey = key;
          this.iterator = index + 1;
          return tuple2val(tuple) - 1; // fix value offset
        }
      }

      index = safeIndex(index + 1);
      tuple = this.data[index];
      this.rereads++;
    }

    this.iteratorKey = 0;
    this.iterator = -1;
    return -1;
  }

  public int getNext() {
    if(this.iterator == -1)
      return -1;
    return getInternal(this.iteratorKey, this.iterator, 0);
  }

  public int size() {
    return this.size;
  }

  public int capacity() {
    return this.maxSize;
  }

  public int capacityEffective() {
    return this.data.length;
  }

  public long getCollisions() {
    return this.collisions;
  }

  public long getRereads() {
    return rereads;
  }

  public String visualize() {
    final int rowSize = (int) (Math.ceil(Math.sqrt(this.data.length)) * 1.5);
    final StringBuilder sb = new StringBuilder();
    for(int i=0; i<this.size(); i++) {
      if(i % rowSize == 0)
        sb.append('\n');
      if(this.data[i] == 0)
        sb.append('.');
      else
        sb.append('X');
    }
    return sb.toString();
  }

  int safeIndex(int index) {
    return index & ((1 << this.shift) - 1);
  }

  static int tuple2key(long tuple) {
    return (int) (tuple >>> 32);
  }

  static int tuple2val(long tuple) {
    return (int) tuple;
  }

  static long tuple(int key, int val) {
    return ((key & TO_LONG) << 32) | (val & TO_LONG);
  }

  static int log2(int value) {
    if(value == 0)
      return 0;
    return 31 - Integer.numberOfLeadingZeros(value);
  }

  static int pow2(int value) {
    return 1 << value;
  }

  static int hash(int k) {
    final int r = 24;
    k *= M;
    k ^= k >>> r;
    return k;
  }
}
