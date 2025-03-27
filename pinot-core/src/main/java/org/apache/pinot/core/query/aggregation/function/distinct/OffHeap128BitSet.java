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
import java.nio.ByteBuffer;
import java.util.Iterator;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Off-heap set for 128-bit values.
 *
 * NOTE: We store and return hash mixed values instead of original values.
 */
@NotThreadSafe
public class OffHeap128BitSet extends BaseOffHeapSet {

  public OffHeap128BitSet(int expectedValues) {
    super(expectedValues);
  }

  @Override
  protected long getMemorySize(int capacity) {
    return (long) capacity << 4;
  }

  public void add(long high, long low) {
    if (high == 0 && low == 0) {
      _containsZero = true;
    } else {
      addMix(HashCommon.mix(high), HashCommon.mix(low));
    }
  }

  private void addMix(long highMix, long lowMix) {
    assert highMix != 0 || lowMix != 0;
    int pos = (int) (highMix & _mask);
    long cursor = _address + ((long) pos << 4);
    long currHighMix = UNSAFE.getLong(cursor);
    long currLowMix = UNSAFE.getLong(cursor + 8);
    if (currHighMix != 0 || currLowMix != 0) {
      do {
        if (currHighMix == highMix && currLowMix == lowMix) {
          return;
        }
        pos = (pos + 1) & _mask;
        cursor = _address + ((long) pos << 4);
        currHighMix = UNSAFE.getLong(cursor);
        currLowMix = UNSAFE.getLong(cursor + 8);
      } while (currHighMix != 0 || currLowMix != 0);
    }
    UNSAFE.putLong(cursor, highMix);
    UNSAFE.putLong(cursor + 8, lowMix);
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
      long highMix;
      long lowMix;
      do {
        highMix = UNSAFE.getLong(cursor);
        lowMix = UNSAFE.getLong(cursor + 8);
        cursor += 16;
      } while (highMix == 0 && lowMix == 0);
      int pos = (int) (highMix & _mask);
      long newCursor = newAddress + ((long) pos << 4);
      if (UNSAFE.getLong(newCursor) != 0 || UNSAFE.getLong(newCursor + 8) != 0) {
        do {
          pos = (pos + 1) & _mask;
          newCursor = newAddress + ((long) pos << 4);
        } while (UNSAFE.getLong(newCursor) != 0 || UNSAFE.getLong(newCursor + 8) != 0);
      }
      UNSAFE.putLong(newCursor, highMix);
      UNSAFE.putLong(newCursor + 8, lowMix);
    }
  }

  @Override
  public Iterator<Value> iterator() {
    return new Iterator<>() {
      private long _cursor = _address;
      private int _remaining = size();
      private boolean _returnZero = _containsZero;

      @Override
      public Value next() {
        if (_returnZero) {
          _returnZero = false;
          _remaining--;
          return new Value(0, 0);
        }
        while (true) {
          long highMix = UNSAFE.getLong(_cursor);
          long lowMix = UNSAFE.getLong(_cursor + 8);
          _cursor += 16;
          if (highMix != 0 || lowMix != 0) {
            _remaining--;
            return new Value(highMix, lowMix);
          }
        }
      }

      @Override
      public boolean hasNext() {
        return _remaining > 0;
      }
    };
  }

  public Iterator<Void> iterator(Value buffer) {
    return new Iterator<>() {
      private long _cursor = _address;
      private int _remaining = size();
      private boolean _returnZero = _containsZero;

      @Override
      public Void next() {
        if (_returnZero) {
          _returnZero = false;
          _remaining--;
          buffer.set(0, 0);
          return null;
        }
        while (true) {
          long highMix = UNSAFE.getLong(_cursor);
          long lowMix = UNSAFE.getLong(_cursor + 8);
          _cursor += 16;
          if (highMix != 0 || lowMix != 0) {
            _remaining--;
            buffer.set(highMix, lowMix);
            return null;
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
    OffHeap128BitSet another128BitSet = (OffHeap128BitSet) another;
    _containsZero |= another128BitSet._containsZero;
    long cursor = another128BitSet._address;
    int numValues = another128BitSet._sizeWithoutZero;
    for (int i = 0; i < numValues; i++) {
      long highMix;
      long lowMix;
      do {
        highMix = UNSAFE.getLong(cursor);
        lowMix = UNSAFE.getLong(cursor + 8);
        cursor += 16;
      } while (highMix == 0 && lowMix == 0);
      addMix(highMix, lowMix);
    }
  }

  @Override
  public byte[] serialize() {
    int size = size();
    byte[] bytes = new byte[Integer.BYTES + size << 4];
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    byteBuffer.putInt(size);
    Value buffer = new Value();
    Iterator<Void> iterator = iterator(buffer);
    while (iterator.hasNext()) {
      iterator.next();
      byteBuffer.putLong(buffer.getHigh());
      byteBuffer.putLong(buffer.getLow());
    }
    return bytes;
  }

  public static OffHeap128BitSet deserialize(ByteBuffer buffer) {
    int size = buffer.getInt();
    OffHeap128BitSet set = new OffHeap128BitSet(size);
    if (size > 0) {
      int startIndex = 0;
      int position = buffer.position();
      if (buffer.getLong() == 0 && buffer.getLong() == 0) {
        set._containsZero = true;
        startIndex = 1;
      } else {
        buffer.position(position);
      }
      for (int i = startIndex; i < size; i++) {
        set.addMix(buffer.getLong(), buffer.getLong());
      }
    }
    return set;
  }

  public static class Value {
    private long _high;
    private long _low;

    public Value(long high, long low) {
      _high = high;
      _low = low;
    }

    public Value() {
    }

    public long getHigh() {
      return _high;
    }

    public long getLow() {
      return _low;
    }

    public void set(long high, long low) {
      _high = high;
      _low = low;
    }

    @SuppressWarnings("EqualsDoesntCheckParameterClass")
    @Override
    public boolean equals(Object o) {
      Value value = (Value) o;
      return _high == value._high && _low == value._low;
    }

    @Override
    public int hashCode() {
      return (int) _low;
    }
  }
}
