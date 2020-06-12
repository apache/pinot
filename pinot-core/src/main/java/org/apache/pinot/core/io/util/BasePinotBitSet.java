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
package org.apache.pinot.core.io.util;

import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public abstract class BasePinotBitSet implements PinotBitSet {
  private static final int[] NUM_BITS_SET = new int[1 << Byte.SIZE];
  private static final int[][] NTH_BIT_SET = new int[Byte.SIZE][1 << Byte.SIZE];
  private static final int[] FIRST_BIT_SET = NTH_BIT_SET[0];
  private static final int BYTE_MASK = 0xFF;

  protected PinotDataBuffer _dataBuffer;
  protected int _numBitsPerValue;

  public BasePinotBitSet(PinotDataBuffer dataBuffer, int numBitsPerValue) {
    _dataBuffer = dataBuffer;
    _numBitsPerValue = numBitsPerValue;
  }

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

  @Override
  public void writeInt(int index, int value) {
    long bitOffset = (long) index * _numBitsPerValue;
    int byteOffset = (int) (bitOffset / Byte.SIZE);
    int bitOffsetInFirstByte = (int) (bitOffset % Byte.SIZE);

    int firstByte = _dataBuffer.getByte(byteOffset);

    int firstByteMask = BYTE_MASK >>> bitOffsetInFirstByte;
    int numBitsLeft = _numBitsPerValue - (Byte.SIZE - bitOffsetInFirstByte);
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
        _dataBuffer.putByte(++byteOffset, (byte) (value >> numBitsLeft));
      }
      int lastByte = _dataBuffer.getByte(++byteOffset);
      _dataBuffer.putByte(byteOffset,
          (byte) ((lastByte & (BYTE_MASK >>> numBitsLeft)) | (value << (Byte.SIZE - numBitsLeft))));
    }
  }

  @Override
  public void writeInt(int startIndex, int length, int[] values) {
    long startBitOffset = (long) startIndex * _numBitsPerValue;
    int byteOffset = (int) (startBitOffset / Byte.SIZE);
    int bitOffsetInFirstByte = (int) (startBitOffset % Byte.SIZE);

    int firstByte = _dataBuffer.getByte(byteOffset);

    for (int i = 0; i < length; i++) {
      int value = values[i];
      if (bitOffsetInFirstByte == Byte.SIZE) {
        bitOffsetInFirstByte = 0;
        firstByte = _dataBuffer.getByte(++byteOffset);
      }
      int firstByteMask = BYTE_MASK >>> bitOffsetInFirstByte;
      int numBitsLeft = _numBitsPerValue - (Byte.SIZE - bitOffsetInFirstByte);
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
          _dataBuffer.putByte(++byteOffset, (byte) (value >> numBitsLeft));
        }
        int lastByte = _dataBuffer.getByte(++byteOffset);
        firstByte = (lastByte & (0xFF >>> numBitsLeft)) | (value << (Byte.SIZE - numBitsLeft));
        _dataBuffer.putByte(byteOffset, (byte) firstByte);
        bitOffsetInFirstByte = numBitsLeft;
      }
    }
  }

  // Helper functions used by multi-value reader and writer
  // for it's header bitmap structure

  public static void setBit(PinotDataBuffer dataBuffer, int bitOffset) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInByte = bitOffset % Byte.SIZE;
    dataBuffer.putByte(byteOffset, (byte) (dataBuffer.getByte(byteOffset) | (0x80 >>> bitOffsetInByte)));
  }

  public static void unsetBit(PinotDataBuffer dataBuffer, int bitOffset) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInByte = bitOffset % Byte.SIZE;
    dataBuffer.putByte(byteOffset, (byte) (dataBuffer.getByte(byteOffset) & (0xFF7F >>> bitOffsetInByte)));
  }

  public static int getNextSetBitOffset(PinotDataBuffer dataBuffer, int bitOffset) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInFirstByte = bitOffset % Byte.SIZE;
    int firstByte = (dataBuffer.getByte(byteOffset) << bitOffsetInFirstByte) & BYTE_MASK;
    if (firstByte != 0) {
      return bitOffset + FIRST_BIT_SET[firstByte];
    }
    while (true) {
      int currentByte = dataBuffer.getByte(++byteOffset) & BYTE_MASK;
      if (currentByte != 0) {
        return (byteOffset * Byte.SIZE) | FIRST_BIT_SET[currentByte];
      }
    }
  }

  public static int getNextNthSetBitOffset(PinotDataBuffer dataBuffer, int bitOffset, int n) {
    int byteOffset = bitOffset / Byte.SIZE;
    int bitOffsetInFirstByte = bitOffset % Byte.SIZE;
    int firstByte = (dataBuffer.getByte(byteOffset) << bitOffsetInFirstByte) & BYTE_MASK;
    int numBitsSet = NUM_BITS_SET[firstByte];
    if (numBitsSet >= n) {
      return bitOffset + NTH_BIT_SET[n - 1][firstByte];
    }
    while (true) {
      n -= numBitsSet;
      int currentByte = dataBuffer.getByte(++byteOffset) & BYTE_MASK;
      numBitsSet = NUM_BITS_SET[currentByte];
      if (numBitsSet >= n) {
        return (byteOffset * Byte.SIZE) | NTH_BIT_SET[n - 1][currentByte];
      }
    }
  }
}
