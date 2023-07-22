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
package org.apache.pinot.segment.local.io.util;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexByteRange;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public final class PinotDataBitSet implements Closeable {
  private static final int[] NUM_BITS_SET = new int[1 << Byte.SIZE];
  private static final int[][] NTH_BIT_SET = new int[Byte.SIZE][1 << Byte.SIZE];
  private static final int[] FIRST_BIT_SET = NTH_BIT_SET[0];
  private static final int BYTE_MASK = 0xFF;

  static {
    for (int i = 0; i < (1 << Byte.SIZE); i++) {
      int numBitsSet = 0;
      for (int j = 0; j < Byte.SIZE; j++) {
        if ((i & (0x80 >>> j)) != 0) {
          NUM_BITS_SET[i]++;
          NTH_BIT_SET[numBitsSet][i] = j;
          numBitsSet++;
        }
      }
    }
  }

  /**
   * Returns the number of bits required to encode the value.
   * <p>NOTE: Use at least one bit even there is only one possible value
   * <p>Examples: (maximum value (binary format) -> number of bits)
   * <ul>
   *   <li>0(0) -> 1</li>
   *   <li>1(1) -> 1</li>
   *   <li>2(10) -> 2</li>
   *   <li>9(1001) -> 4</li>
   *   <li>113(1110001) -> 7</li>
   * </ul>
   *
   * @param maxValue Maximum possible value
   * @return Number of bits required to encode the value
   */
  public static int getNumBitsPerValue(int maxValue) {
    // Use at least one bit even there is only one possible value
    if (maxValue <= 1) {
      return 1;
    }
    int numBitsPerValue = Byte.SIZE;
    while (maxValue > 0xFF) {
      maxValue >>>= Byte.SIZE;
      numBitsPerValue += Byte.SIZE;
    }
    return numBitsPerValue - FIRST_BIT_SET[maxValue];
  }

  private final PinotDataBuffer _dataBuffer;

  public PinotDataBitSet(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;
  }

  public int readIntAndGetRanges(int index, int numBitsPerValue, long baseOffset,
      List<ForwardIndexByteRange> ranges) {
    long bitOffset = (long) index * numBitsPerValue;
    int byteOffset = (int) (bitOffset / Byte.SIZE);
    int bitOffsetInFirstByte = (int) (bitOffset % Byte.SIZE);

    // Initiated with the value in first byte
    ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.SIZE));
    int currentValue = _dataBuffer.getByte(byteOffset) & (BYTE_MASK >>> bitOffsetInFirstByte);

    int numBitsLeft = numBitsPerValue - (Byte.SIZE - bitOffsetInFirstByte);
    if (numBitsLeft <= 0) {
      // The value is inside the first byte
      return currentValue >>> -numBitsLeft;
    } else {
      // The value is in multiple bytes
      while (numBitsLeft > Byte.SIZE) {
        byteOffset++;
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.SIZE));
        currentValue = (currentValue << Byte.SIZE) | (_dataBuffer.getByte(byteOffset) & BYTE_MASK);
        numBitsLeft -= Byte.SIZE;
      }
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.SIZE));
      return (currentValue << numBitsLeft) | ((_dataBuffer.getByte(byteOffset + 1) & BYTE_MASK) >>> (Byte.SIZE
          - numBitsLeft));
    }
  }

  public int readInt(int index, int numBitsPerValue) {
    long bitOffset = (long) index * numBitsPerValue;
    int byteOffset = (int) (bitOffset / Byte.SIZE);
    int bitOffsetInFirstByte = (int) (bitOffset % Byte.SIZE);

    // Initiated with the value in first byte
    int currentValue = _dataBuffer.getByte(byteOffset) & (BYTE_MASK >>> bitOffsetInFirstByte);

    int numBitsLeft = numBitsPerValue - (Byte.SIZE - bitOffsetInFirstByte);
    if (numBitsLeft <= 0) {
      // The value is inside the first byte
      return currentValue >>> -numBitsLeft;
    } else {
      // The value is in multiple bytes
      while (numBitsLeft > Byte.SIZE) {
        byteOffset++;
        currentValue = (currentValue << Byte.SIZE) | (_dataBuffer.getByte(byteOffset) & BYTE_MASK);
        numBitsLeft -= Byte.SIZE;
      }
      return (currentValue << numBitsLeft) | ((_dataBuffer.getByte(byteOffset + 1) & BYTE_MASK) >>> (Byte.SIZE
          - numBitsLeft));
    }
  }

  public void readIntAndGetRanges(int startIndex, int numBitsPerValue, int length, int[] buffer, long baseOffset,
      List<ForwardIndexByteRange> ranges) {
    long startBitOffset = (long) startIndex * numBitsPerValue;
    int byteOffset = (int) (startBitOffset / Byte.SIZE);
    int bitOffsetInFirstByte = (int) (startBitOffset % Byte.SIZE);

    // Initiated with the value in first byte
    ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.SIZE));
    int currentValue = _dataBuffer.getByte(byteOffset) & (BYTE_MASK >>> bitOffsetInFirstByte);

    for (int i = 0; i < length; i++) {
      if (bitOffsetInFirstByte == Byte.SIZE) {
        bitOffsetInFirstByte = 0;
        byteOffset++;
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.SIZE));
        currentValue = _dataBuffer.getByte(byteOffset) & BYTE_MASK;
      }
      int numBitsLeft = numBitsPerValue - (Byte.SIZE - bitOffsetInFirstByte);
      if (numBitsLeft <= 0) {
        // The value is inside the first byte
        buffer[i] = currentValue >>> -numBitsLeft;
        bitOffsetInFirstByte = Byte.SIZE + numBitsLeft;
        currentValue = currentValue & (BYTE_MASK >>> bitOffsetInFirstByte);
      } else {
        // The value is in multiple bytes
        while (numBitsLeft > Byte.SIZE) {
          byteOffset++;
          ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.SIZE));
          currentValue = (currentValue << Byte.SIZE) | (_dataBuffer.getByte(byteOffset) & BYTE_MASK);
          numBitsLeft -= Byte.SIZE;
        }
        byteOffset++;
        int nextByte = _dataBuffer.getByte(byteOffset) & BYTE_MASK;
        buffer[i] = (currentValue << numBitsLeft) | (nextByte >>> (Byte.SIZE - numBitsLeft));
        bitOffsetInFirstByte = numBitsLeft;
        currentValue = nextByte & (BYTE_MASK >>> bitOffsetInFirstByte);
      }
    }
  }

  public void readInt(int startIndex, int numBitsPerValue, int length, int[] buffer) {
    long startBitOffset = (long) startIndex * numBitsPerValue;
    int byteOffset = (int) (startBitOffset / Byte.SIZE);
    int bitOffsetInFirstByte = (int) (startBitOffset % Byte.SIZE);

    // Initiated with the value in first byte
    int currentValue = _dataBuffer.getByte(byteOffset) & (BYTE_MASK >>> bitOffsetInFirstByte);

    for (int i = 0; i < length; i++) {
      if (bitOffsetInFirstByte == Byte.SIZE) {
        bitOffsetInFirstByte = 0;
        byteOffset++;
        currentValue = _dataBuffer.getByte(byteOffset) & BYTE_MASK;
      }
      int numBitsLeft = numBitsPerValue - (Byte.SIZE - bitOffsetInFirstByte);
      if (numBitsLeft <= 0) {
        // The value is inside the first byte
        buffer[i] = currentValue >>> -numBitsLeft;
        bitOffsetInFirstByte = Byte.SIZE + numBitsLeft;
        currentValue = currentValue & (BYTE_MASK >>> bitOffsetInFirstByte);
      } else {
        // The value is in multiple bytes
        while (numBitsLeft > Byte.SIZE) {
          byteOffset++;
          currentValue = (currentValue << Byte.SIZE) | (_dataBuffer.getByte(byteOffset) & BYTE_MASK);
          numBitsLeft -= Byte.SIZE;
        }
        byteOffset++;
        int nextByte = _dataBuffer.getByte(byteOffset) & BYTE_MASK;
        buffer[i] = (currentValue << numBitsLeft) | (nextByte >>> (Byte.SIZE - numBitsLeft));
        bitOffsetInFirstByte = numBitsLeft;
        currentValue = nextByte & (BYTE_MASK >>> bitOffsetInFirstByte);
      }
    }
  }

  public void writeInt(int index, int numBitsPerValue, int value) {
    long bitOffset = (long) index * numBitsPerValue;
    int byteOffset = (int) (bitOffset / Byte.SIZE);
    int bitOffsetInFirstByte = (int) (bitOffset % Byte.SIZE);

    int firstByte = _dataBuffer.getByte(byteOffset);

    int firstByteMask = BYTE_MASK >>> bitOffsetInFirstByte;
    int numBitsLeft = numBitsPerValue - (Byte.SIZE - bitOffsetInFirstByte);
    if (numBitsLeft <= 0) {
      // The value is inside the first byte
      firstByteMask &= BYTE_MASK << -numBitsLeft;
      _dataBuffer.putByte(byteOffset, (byte) ((firstByte & ~firstByteMask) | (value << -numBitsLeft)));
    } else {
      // The value is in multiple bytes
      _dataBuffer
          .putByte(byteOffset, (byte) ((firstByte & ~firstByteMask) | ((value >>> numBitsLeft) & firstByteMask)));
      while (numBitsLeft > Byte.SIZE) {
        numBitsLeft -= Byte.SIZE;
        byteOffset++;
        _dataBuffer.putByte(byteOffset, (byte) (value >> numBitsLeft));
      }
      byteOffset++;
      int lastByte = _dataBuffer.getByte(byteOffset);
      _dataBuffer.putByte(byteOffset,
          (byte) ((lastByte & (BYTE_MASK >>> numBitsLeft)) | (value << (Byte.SIZE - numBitsLeft))));
    }
  }

  public void writeInt(int startIndex, int numBitsPerValue, int length, int[] values) {
    long startBitOffset = (long) startIndex * numBitsPerValue;
    int byteOffset = (int) (startBitOffset / Byte.SIZE);
    int bitOffsetInFirstByte = (int) (startBitOffset % Byte.SIZE);

    int firstByte = _dataBuffer.getByte(byteOffset);

    for (int i = 0; i < length; i++) {
      int value = values[i];
      if (bitOffsetInFirstByte == Byte.SIZE) {
        bitOffsetInFirstByte = 0;
        byteOffset++;
        firstByte = _dataBuffer.getByte(byteOffset);
      }
      int firstByteMask = BYTE_MASK >>> bitOffsetInFirstByte;
      int numBitsLeft = numBitsPerValue - (Byte.SIZE - bitOffsetInFirstByte);
      if (numBitsLeft <= 0) {
        // The value is inside the first byte
        firstByteMask &= BYTE_MASK << -numBitsLeft;
        firstByte = ((firstByte & ~firstByteMask) | (value << -numBitsLeft));
        _dataBuffer.putByte(byteOffset, (byte) firstByte);
        bitOffsetInFirstByte = Byte.SIZE + numBitsLeft;
      } else {
        // The value is in multiple bytes
        _dataBuffer
            .putByte(byteOffset, (byte) ((firstByte & ~firstByteMask) | ((value >>> numBitsLeft) & firstByteMask)));
        while (numBitsLeft > Byte.SIZE) {
          numBitsLeft -= Byte.SIZE;
          byteOffset++;
          _dataBuffer.putByte(byteOffset, (byte) (value >> numBitsLeft));
        }
        byteOffset++;
        int lastByte = _dataBuffer.getByte(byteOffset);
        firstByte = (lastByte & (0xFF >>> numBitsLeft)) | (value << (Byte.SIZE - numBitsLeft));
        _dataBuffer.putByte(byteOffset, (byte) firstByte);
        bitOffsetInFirstByte = numBitsLeft;
      }
    }
  }

  public void setBit(int bitOffset) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInByte = bitOffset % Byte.SIZE;
    _dataBuffer.putByte(byteOffset, (byte) (_dataBuffer.getByte(byteOffset) | (0x80 >>> bitOffsetInByte)));
  }

  public void unsetBit(int bitOffset) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInByte = bitOffset % Byte.SIZE;
    _dataBuffer.putByte(byteOffset, (byte) (_dataBuffer.getByte(byteOffset) & (0xFF7F >>> bitOffsetInByte)));
  }

  public int getNextSetBitOffsetRanges(int bitOffset, long baseOffset, List<ForwardIndexByteRange> ranges) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInFirstByte = bitOffset % Byte.SIZE;
    ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.BYTES));
    int firstByte = (_dataBuffer.getByte(byteOffset) << bitOffsetInFirstByte) & BYTE_MASK;
    if (firstByte != 0) {
      return bitOffset + FIRST_BIT_SET[firstByte];
    }
    while (true) {
      byteOffset++;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.BYTES));
      int currentByte = _dataBuffer.getByte(byteOffset) & BYTE_MASK;
      if (currentByte != 0) {
        return (byteOffset * Byte.SIZE) | FIRST_BIT_SET[currentByte];
      }
    }
  }

  public int getNextSetBitOffset(int bitOffset) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInFirstByte = bitOffset % Byte.SIZE;
    int firstByte = (_dataBuffer.getByte(byteOffset) << bitOffsetInFirstByte) & BYTE_MASK;
    if (firstByte != 0) {
      return bitOffset + FIRST_BIT_SET[firstByte];
    }
    while (true) {
      byteOffset++;
      int currentByte = _dataBuffer.getByte(byteOffset) & BYTE_MASK;
      if (currentByte != 0) {
        return (byteOffset * Byte.SIZE) | FIRST_BIT_SET[currentByte];
      }
    }
  }

  public int getNextNthSetBitOffsetOffsetRanges(int bitOffset, int n, long baseOffset,
      List<ForwardIndexByteRange> ranges) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInFirstByte = bitOffset % Byte.SIZE;
    ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.BYTES));
    int firstByte = (_dataBuffer.getByte(byteOffset) << bitOffsetInFirstByte) & BYTE_MASK;
    int numBitsSet = NUM_BITS_SET[firstByte];
    if (numBitsSet >= n) {
      return bitOffset + NTH_BIT_SET[n - 1][firstByte];
    }
    while (true) {
      n -= numBitsSet;
      byteOffset++;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + byteOffset, Byte.BYTES));
      int currentByte = _dataBuffer.getByte(byteOffset) & BYTE_MASK;
      numBitsSet = NUM_BITS_SET[currentByte];
      if (numBitsSet >= n) {
        return (byteOffset * Byte.SIZE) | NTH_BIT_SET[n - 1][currentByte];
      }
    }
  }

  public int getNextNthSetBitOffset(int bitOffset, int n) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInFirstByte = bitOffset % Byte.SIZE;
    int firstByte = (_dataBuffer.getByte(byteOffset) << bitOffsetInFirstByte) & BYTE_MASK;
    int numBitsSet = NUM_BITS_SET[firstByte];
    if (numBitsSet >= n) {
      return bitOffset + NTH_BIT_SET[n - 1][firstByte];
    }
    while (true) {
      n -= numBitsSet;
      byteOffset++;
      int currentByte = _dataBuffer.getByte(byteOffset) & BYTE_MASK;
      numBitsSet = NUM_BITS_SET[currentByte];
      if (numBitsSet >= n) {
        return (byteOffset * Byte.SIZE) | NTH_BIT_SET[n - 1][currentByte];
      }
    }
  }

  @Override
  public void close() {
    // NOTE: DO NOT close the PinotDataBuffer here because it is tracked by the caller and might be reused later. The
    // caller is responsible of closing the PinotDataBuffer.
  }
}
