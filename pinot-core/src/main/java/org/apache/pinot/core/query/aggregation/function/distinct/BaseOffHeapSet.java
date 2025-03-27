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
package org.apache.pinot.core.query.aggregation.function.distinct;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.HashCommon;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.segment.spi.memory.unsafe.Unsafer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;


/**
 * Base class for set implementations using off-heap memory.
 *
 * This class is used to calculate the count of distinct values, and it doesn't implement Set interface.
 * The following operations are allowed:
 * - Add value to the set (not included in the base implementation)
 * - Merge with another set
 * - Get the size of the set / check if the set is empty
 * - Iterate over the set
 * - Serialize/deserialize the set
 *
 * The values stored in the set might not be the original values for performance reasons (e.g. avoid re-computing hash
 * during set growing). The iterator returns the internal stored values.
 */
@NotThreadSafe
public abstract class BaseOffHeapSet implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseOffHeapSet.class.getName());

  protected static final float LOAD_FACTOR = 0.75f;
  protected static final Unsafe UNSAFE = Unsafer.UNSAFE;

  protected int _capacity;
  protected int _mask;
  protected int _maxFill;
  protected long _address;
  protected boolean _containsZero;
  protected int _sizeWithoutZero;
  protected boolean _closed;

  protected BaseOffHeapSet(int expectedValues) {
    _capacity = arraySize(expectedValues);
    _mask = _capacity - 1;
    _maxFill = maxFill(_capacity);
    _address = allocateMemory(getMemorySize(_capacity));
  }

  /**
   * Returns the memory size for the given capacity.
   */
  protected abstract long getMemorySize(int capacity);

  /**
   * Returns the size of the set.
   */
  public int size() {
    return _containsZero ? _sizeWithoutZero + 1 : _sizeWithoutZero;
  }

  /**
   * Returns whether the set is empty.
   */
  public boolean isEmpty() {
    return !_containsZero && _sizeWithoutZero == 0;
  }

  /**
   * Returns an iterator over the set. The iterator returns the internal stored values which might not be the original
   * added values.
   */
  public abstract Iterator<?> iterator();

  /**
   * Merges a set into this set.
   */
  public abstract void merge(BaseOffHeapSet another);

  /**
   * Serializes the set into a byte array.
   */
  public abstract byte[] serialize();

  /**
   * Expands the set (double its capacity).
   */
  protected void expand() {
    _capacity = _capacity << 1;
    Preconditions.checkState(_capacity > 0, "Integer overflow! Invalid capacity: %s", _capacity);
    _mask = _capacity - 1;
    _maxFill = maxFill(_capacity);
    long newAddress = allocateMemory(getMemorySize(_capacity));
    try {
      rehash(newAddress);
    } catch (Throwable t) {
      UNSAFE.freeMemory(newAddress);
      throw t;
    }
    long oldAddress = _address;
    _address = newAddress;
    UNSAFE.freeMemory(oldAddress);
  }

  /**
   * Rehashes the set into the expanded memory.
   */
  protected abstract void rehash(long newAddress);

  @Override
  public void close() {
    if (!_closed) {
      UNSAFE.freeMemory(_address);
      _closed = true;
    }
  }

  @Override
  protected void finalize()
      throws Throwable {
    try {
      if (!_closed) {
        LOGGER.warn("Direct memory of size: {} wasn't explicitly closed", getMemorySize(_capacity));
        close();
      }
    } finally {
      super.finalize();
    }
  }

  /**
   * Returns the size of the array to allocate based on the expected number of values.
   */
  private static int arraySize(int expectedValues) {
    return HashCommon.arraySize(expectedValues, LOAD_FACTOR);
  }

  /**
   * Returns the maximum fill of the array based on the capacity.
   */
  private static int maxFill(int capacity) {
    return HashCommon.maxFill(capacity, LOAD_FACTOR);
  }

  /**
   * Allocates memory of the given size and initializes it to 0.
   */
  private static long allocateMemory(long size) {
    long address = UNSAFE.allocateMemory(size);
    UNSAFE.setMemory(address, size, (byte) 0);
    return address;
  }
}
