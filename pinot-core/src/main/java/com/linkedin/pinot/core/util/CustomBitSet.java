/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.util;

import java.nio.ByteBuffer;


/**
 *
 * Util class to store bit set, provides additional utility over java bit set by
 * allowing reading int from start bit to end bit
 */
public final class CustomBitSet {

  private final int nrBytes;
  private final ByteBuffer buf;
  private final static int[] bitCountArray = new int[256];
  private final static int IGNORED_ZEROS_COUNT = Integer.SIZE - Byte.SIZE;

  static {
    for (int i = 0; i < 256; i++) {
      bitCountArray[i] = Integer.bitCount(i);
    }
  }

  private CustomBitSet(final int nrBytes) {
    if (nrBytes < 1) {
      throw new IllegalArgumentException("need at least one byte");
    }
    this.nrBytes = nrBytes;
    buf = ByteBuffer.allocate(nrBytes);
  }

  private CustomBitSet(final int numBytes, final ByteBuffer buffer) {
    nrBytes = numBytes;
    this.buf = buffer;
  }

  public static CustomBitSet withByteBuffer(final int numBytes, ByteBuffer byteBuffer) {
    return new CustomBitSet(numBytes, byteBuffer);
  }

  public static CustomBitSet withByteLength(final int nrBytes) {
    return new CustomBitSet(nrBytes);
  }

  public static CustomBitSet withBitLength(final int nrBits) {
    return new CustomBitSet((nrBits - 1) / 8 + 1);
  }

  public void setBit(final int bitOffset) {
    if (bitOffset < 0)
      throw new IllegalArgumentException();

    final int byteToSet = bitOffset / 8;
    if (byteToSet > nrBytes)
      throw new IllegalArgumentException();
    byte b = buf.get(byteToSet);
    byte posBit = (byte) (1 << (7 - (bitOffset % 8)));
    // System.out.println("bitOffset:" + bitOffset + " posBit:" + posBit);
    b |= posBit;
    buf.put(byteToSet, b);
  }

  public void unsetBit(final int bitOffset) {
    if (bitOffset < 0) {
      throw new IllegalArgumentException();
    }
    final int byteToSet = bitOffset / 8;
    if (byteToSet > nrBytes) {
      throw new IllegalArgumentException();
    }
    final int offset = bitOffset % 8;
    byte b = buf.get(byteToSet);
    b &= ~(1 << (7 - offset));
    buf.put(byteToSet, b);
  }

  /**
   * reads the read between the start (inclusive) and end (exclusive)
   *
   * @return
   */
  public int readInt(int startBitIndex, int endBitIndex) {
    int mult = startBitIndex;
    int bytePosition = mult >>> 3;
    int startBitOffset = mult & 7;
    int sum = startBitOffset + (endBitIndex - startBitIndex);
    int endBitOffset = (8 - (sum & 7)) & 7;

    // int numberOfBytesUsed = (sum >>> 3) + ((sum & 7) != 0 ? 1 : 0);
    int numberOfBytesUsed = ((sum + 7) >>> 3);
    int i = -1;

    long number = 0;
    while (true) {
      number |= (buf.get(bytePosition)) & 0xFF;
      i++;
      bytePosition++;
      if (i == numberOfBytesUsed - 1) {
        break;
      }
      number <<= 8;
    }
    number >>= endBitOffset;
    number &= (0xFFFFFFFF >>> (32 - (endBitIndex - startBitIndex)));
    return (int) number;

  }

  public byte[] toByteArray() {
    return buf.array();
  }

  public String toString() {
    byte[] array = buf.array();
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (byte b : array) {
      sb.append(Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Finds the index of the Nth bit set after the startIndex, the bit at startIndex is excluded
   * @param startIndex
   * @param n
   * @return
   */
  public int findNthBitSetAfter(int startBitIndex, int n) {
    startBitIndex = startBitIndex + 1;
    int bytePosition = startBitIndex / 8;
    int bitPosition = startBitIndex % 8;
    if (bytePosition > nrBytes) {
      return -1;
    }
    int currentByte = (buf.get(bytePosition) << bitPosition) & 0xFF;
    int index = startBitIndex;
    if (bitCountArray[currentByte] < n) {
      int count = bitCountArray[currentByte];
      do {
        bytePosition = bytePosition + 1;
        if (bytePosition >= nrBytes) {
          return -1;
        }
        currentByte = buf.get(bytePosition) & 0xFF;
        if (bitCountArray[currentByte] + count >= n) {
          break;
        }
        count = count + bitCountArray[currentByte];
      } while (true);
      index = bytePosition * 8 - 1;
      for (int i = 0; i < n - count; i++) {
        index = nextSetBit(index);
      }
    } else {
      return Integer.numberOfLeadingZeros(currentByte) - IGNORED_ZEROS_COUNT + index;
    }
    return index;

  }

  public int nextSetBit(int index) {
    index = index + 1;
    int bytePosition = index / 8;
    int bitPosition = index % 8;

    if (bytePosition > nrBytes) {
      return -1;
    }

    // Assuming index 3
    // --- IGNORED_ZEROS_COUNT --
    //                             index
    //                               v
    // 00000000 00000000 00000000 00000010
    int currentByte = (buf.get(bytePosition) << bitPosition) & 0xFF;

    if (currentByte != 0) {
      return Integer.numberOfLeadingZeros(currentByte) - IGNORED_ZEROS_COUNT + index;
    }

    int bytesSkipped = 0;
    // Skip whole bytes
    while (currentByte == 0) {
      bytesSkipped++;

      if (bytePosition + bytesSkipped >= nrBytes) {
        return -1;
      }
      currentByte = buf.get(bytePosition + bytesSkipped) & 0xFF;
    }

    int zerosCount = Integer.numberOfLeadingZeros(currentByte) - IGNORED_ZEROS_COUNT;

    return zerosCount + (bytePosition + bytesSkipped) * 8;

  }

  public boolean isBitSet(int index) {
    final int byteToCheck = index >>> 3;
    assert (byteToCheck < nrBytes);
    byte b = buf.get(byteToCheck);
    //    System.out.println(Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
    final int offset = (7 - index % 8);
    return ((b & (1 << offset)) != 0);
  }

}
