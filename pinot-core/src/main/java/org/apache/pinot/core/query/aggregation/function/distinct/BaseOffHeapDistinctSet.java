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

import it.unimi.dsi.fastutil.HashCommon;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.pinot.segment.spi.memory.unsafe.Unsafer;
import sun.misc.Unsafe;


@NotThreadSafe
public abstract class BaseOffHeapDistinctSet implements AutoCloseable {
  protected static final float LOAD_FACTOR = 0.75f;
  protected static final Unsafe UNSAFE = Unsafer.UNSAFE;

  protected long _address;
  protected boolean _containsZero;
  protected int _sizeWithoutZero;
  protected boolean _closed;

  public int size() {
    return _containsZero ? _sizeWithoutZero + 1 : _sizeWithoutZero;
  }

  public boolean isEmpty() {
    return !_containsZero && _sizeWithoutZero == 0;
  }

  public abstract Iterator<?> iterator();

  public abstract void merge(BaseOffHeapDistinctSet another);

  public abstract byte[] serialize();

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
      close();
    } finally {
      super.finalize();
    }
  }

  protected static int arraySize(int expectedValues) {
    return HashCommon.arraySize(expectedValues, LOAD_FACTOR);
  }

  protected static int maxFill(int capacity) {
    return HashCommon.maxFill(capacity, LOAD_FACTOR);
  }

  protected static long allocateMemory(long size) {
    long address = UNSAFE.allocateMemory(size);
    UNSAFE.setMemory(address, size, (byte) 0);
    return address;
  }
}
