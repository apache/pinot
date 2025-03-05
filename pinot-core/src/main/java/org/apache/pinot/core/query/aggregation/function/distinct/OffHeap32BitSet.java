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
import it.unimi.dsi.fastutil.ints.IntIterator;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Off-heap set for 32-bit values.
 *
 * NOTE: We store and return hash mixed values instead of original values.
 */
@NotThreadSafe
public class OffHeap32BitSet extends BaseOffHeapSet {

  public OffHeap32BitSet(int expectedValues) {
    super(expectedValues);
  }

  @Override
  protected long getMemorySize(int capacity) {
    return (long) capacity << 2;
  }

  public void add(int value) {
    if (value == 0) {
      _containsZero = true;
    } else {
      addMix(HashCommon.mix(value));
    }
  }

  private void addMix(int mix) {
    assert mix != 0;
    int pos = mix & _mask;
    long cursor = _address + ((long) pos << 2);
    int curr = UNSAFE.getInt(cursor);
    if (curr != 0) {
      do {
        if (curr == mix) {
          return;
        }
        pos = (pos + 1) & _mask;
        cursor = _address + ((long) pos << 2);
        curr = UNSAFE.getInt(cursor);
      } while (curr != 0);
    }
    UNSAFE.putInt(cursor, mix);
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
      int mix;
      do {
        mix = UNSAFE.getInt(cursor);
        cursor += 4;
      } while (mix == 0);
      int pos = mix & _mask;
      long newCursor = newAddress + ((long) pos << 2);
      if (UNSAFE.getInt(newCursor) != 0) {
        do {
          pos = (pos + 1) & _mask;
          newCursor = newAddress + ((long) pos << 2);
        } while (UNSAFE.getInt(newCursor) != 0);
      }
      UNSAFE.putInt(newCursor, mix);
    }
  }

  @Override
  public IntIterator iterator() {
    return new IntIterator() {
      private long _cursor = _address;
      private int _remaining = size();
      private boolean _returnZero = _containsZero;

      @Override
      public int nextInt() {
        if (_returnZero) {
          _returnZero = false;
          _remaining--;
          return 0;
        }
        while (true) {
          int mix = UNSAFE.getInt(_cursor);
          _cursor += 4;
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
    OffHeap32BitSet another32BitSet = (OffHeap32BitSet) another;
    _containsZero |= another32BitSet._containsZero;
    long cursor = another32BitSet._address;
    int numValues = another32BitSet._sizeWithoutZero;
    for (int i = 0; i < numValues; i++) {
      int mix;
      do {
        mix = UNSAFE.getInt(cursor);
        cursor += 4;
      } while (mix == 0);
      addMix(mix);
    }
  }

  @Override
  public byte[] serialize() {
    int size = size();
    byte[] bytes = new byte[Integer.BYTES + size << 2];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(size);
    IntIterator iterator = iterator();
    while (iterator.hasNext()) {
      byteBuffer.putInt(iterator.nextInt());
    }
    return bytes;
  }

  public static OffHeap32BitSet deserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    OffHeap32BitSet set = new OffHeap32BitSet(size);
    if (size > 0) {
      int startIndex = 0;
      int position = buffer.position();
      if (buffer.getInt() == 0) {
        set._containsZero = true;
        startIndex = 1;
      } else {
        buffer.position(position);
      }
      for (int i = startIndex; i < size; i++) {
        set.addMix(buffer.getInt());
      }
    }
    return set;
  }
}
