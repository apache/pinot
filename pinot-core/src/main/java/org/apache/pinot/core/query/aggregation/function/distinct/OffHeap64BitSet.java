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
import it.unimi.dsi.fastutil.longs.LongIterator;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Off-heap set for 64-bit values.
 *
 * NOTE: We store and return hash mixed values instead of original values.
 */
@NotThreadSafe
public class OffHeap64BitSet extends BaseOffHeapSet {

  public OffHeap64BitSet(int expectedValues) {
    super(expectedValues);
  }

  @Override
  protected long getMemorySize(int capacity) {
    return (long) capacity << 3;
  }

  public void add(long value) {
    if (value == 0) {
      _containsZero = true;
    } else {
      addMix(HashCommon.mix(value));
    }
  }

  private void addMix(long mix) {
    assert mix != 0;
    int pos = (int) (mix & _mask);
    long cursor = _address + ((long) pos << 3);
    long curr = UNSAFE.getLong(cursor);
    if (curr != 0) {
      do {
        if (curr == mix) {
          return;
        }
        pos = (pos + 1) & _mask;
        cursor = _address + ((long) pos << 3);
        curr = UNSAFE.getLong(cursor);
      } while (curr != 0);
    }
    UNSAFE.putLong(cursor, mix);
    _sizeWithoutZero++;
    if (_sizeWithoutZero > _maxFill) {
      expand();
    }
  }

  @Override
  protected void rehash(long newAddress) {
    long cursor = _address;
    int numValues = _sizeWithoutZero;
    for (int i = 0; i < numValues; i++) {
      long mix;
      do {
        mix = UNSAFE.getLong(cursor);
        cursor += 8;
      } while (mix == 0);
      int pos = (int) (mix & _mask);
      long newCursor = newAddress + ((long) pos << 3);
      if (UNSAFE.getLong(newCursor) != 0) {
        do {
          pos = (pos + 1) & _mask;
          newCursor = newAddress + ((long) pos << 3);
        } while (UNSAFE.getLong(newCursor) != 0);
      }
      UNSAFE.putLong(newCursor, mix);
    }
  }

  public LongIterator iterator() {
    return new LongIterator() {
      private long _cursor = _address;
      private int _remaining = size();
      private boolean _returnZero = _containsZero;

      @Override
      public long nextLong() {
        if (_returnZero) {
          _returnZero = false;
          _remaining--;
          return 0L;
        }
        while (true) {
          long mix = UNSAFE.getLong(_cursor);
          _cursor += 8;
          if (mix != 0) {
            _remaining--;
            return mix;
          }
        }
      }

      @Override
      public boolean hasNext() {
        return _remaining > 0;
      }
    };
  }

  @Override
  public void merge(BaseOffHeapSet another) {
    if (another.isEmpty()) {
      return;
    }
    OffHeap64BitSet another64BitSet = (OffHeap64BitSet) another;
    _containsZero |= another64BitSet._containsZero;
    long cursor = another64BitSet._address;
    int numValues = another64BitSet._sizeWithoutZero;
    for (int i = 0; i < numValues; i++) {
      long mix;
      do {
        mix = UNSAFE.getLong(cursor);
        cursor += 8;
      } while (mix == 0);
      addMix(mix);
    }
  }

  @Override
  public byte[] serialize() {
    int size = size();
    byte[] bytes = new byte[Integer.BYTES + size << 3];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(size);
    LongIterator iterator = iterator();
    while (iterator.hasNext()) {
      byteBuffer.putLong(iterator.nextLong());
    }
    return bytes;
  }

  public static OffHeap64BitSet deserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    OffHeap64BitSet set = new OffHeap64BitSet(size);
    if (size > 0) {
      int startIndex = 0;
      int position = buffer.position();
      if (buffer.getLong() == 0) {
        set._containsZero = true;
        startIndex = 1;
      } else {
        buffer.position(position);
      }
      for (int i = startIndex; i < size; i++) {
        set.addMix(buffer.getLong());
      }
    }
    return set;
  }
}
