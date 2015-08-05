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

import com.google.common.primitives.Ints;
import com.linkedin.pinot.core.indexsegment.utils.BitUtils;
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
      throw new IllegalArgumentException("CustomBitSet requires at least one byte of storage, asked for " + nrBytes);
    }
    this.nrBytes = nrBytes;
    buf = ByteBuffer.allocateDirect(nrBytes);
  }

  private CustomBitSet(final int nrBytes, final ByteBuffer buffer) {
    if (nrBytes < 1) {
      throw new IllegalArgumentException("CustomBitSet requires at least one byte of storage, asked for " + nrBytes);
    }
    if (buffer.capacity() < nrBytes) {
      throw new IllegalArgumentException("Requested bit set capacity is " + nrBytes +
          " bytes, but the underlying byte buffer has a capacity of " + buffer.capacity() +
          ", which is less than requested");
    }
    this.nrBytes = nrBytes;
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

  public void setBit(final long bitOffset) {
    if (bitOffset < 0) {
      throw new IllegalArgumentException("Negative bitOffset value " + bitOffset);
    }

    final int byteToSet = Ints.checkedCast(bitOffset / 8);
    if (byteToSet > nrBytes) {
      throw new IllegalArgumentException("bitOffset value " + bitOffset + " (byte offset " + byteToSet + ") exceeds buffer capacity of " + nrBytes + " bytes");
    }

    byte b = buf.get(byteToSet);
    byte posBit = (byte) (1 << (7 - (bitOffset % 8)));
    // System.out.println("bitOffset:" + bitOffset + " posBit:" + posBit);
    b |= posBit;
    buf.put(byteToSet, b);
  }

  public void unsetBit(final long bitOffset) {
    if (bitOffset < 0) {
      throw new IllegalArgumentException("Negative bitOffset value " + bitOffset);
    }
    final int byteToSet = Ints.checkedCast(bitOffset / 8);
    if (byteToSet > nrBytes) {
      throw new IllegalArgumentException("bitOffset value " + bitOffset + " (byte offset " + byteToSet + ") exceeds buffer capacity of " + nrBytes + " bytes");
    }

    final int offset = Ints.checkedCast(bitOffset % 8);
    byte b = buf.get(byteToSet);
    b &= ~(1 << (7 - offset));
    buf.put(byteToSet, b);
  }

  /**
   * reads the read between the start (inclusive) and end (exclusive)
   *
   * @return
   */
  public int readInt(long startBitIndex, long endBitIndex) {
    int bytePosition = Ints.checkedCast(startBitIndex >>> 3);
    int startBitOffset = Ints.checkedCast(startBitIndex & 7);
    long sum = startBitOffset + (endBitIndex - startBitIndex);
    int endBitOffset = Ints.checkedCast((8 - (sum & 7)) & 7);

    // int numberOfBytesUsed = (sum >>> 3) + ((sum & 7) != 0 ? 1 : 0);
    int numberOfBytesUsed = Ints.checkedCast((sum + 7) >>> 3);
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
    byte[] dst = new byte[buf.capacity()];
    buf.get(dst);
    return dst;
  }

  public String toString() {
    byte[] array = toByteArray();
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    for (byte b : array) {
      sb.append(Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Finds the index of the Nth bit set after the startBitIndex, the bit at startBitIndex is excluded
   * @param startBitIndex The index from which to start the search
   * @param n The
   * @return
   */
  public long findNthBitSetAfter(long startBitIndex, int n) {
    long searchStartBitIndex = startBitIndex + 1;

    int bytePosition = Ints.checkedCast(searchStartBitIndex / 8);
    int bitPosition = Ints.checkedCast(searchStartBitIndex % 8);
    if (bytePosition >= nrBytes) {
      return -1;
    }

    int currentByte = (buf.get(bytePosition) << bitPosition) & 0xFF;
    int numberOfBitsOnInCurrentByte = bitCountArray[currentByte];
    int numberOfBitsToSkip = n - 1;

    // Is the bit we're looking for in the current byte?
    if (n <= numberOfBitsOnInCurrentByte) {
      currentByte = BitUtils.turnOffNthLeftmostSetBits(currentByte, numberOfBitsToSkip);
      return Integer.numberOfLeadingZeros(currentByte) - IGNORED_ZEROS_COUNT + startBitIndex + 1;
    }

    // Skip whole bytes until we bit we're looking for is in the current byte
    while (numberOfBitsOnInCurrentByte <= numberOfBitsToSkip) {
      numberOfBitsToSkip -= numberOfBitsOnInCurrentByte;
      bytePosition++;
      if (bytePosition >= nrBytes) {
        return -1;
      }
      currentByte = buf.get(bytePosition) & 0xFF;
      numberOfBitsOnInCurrentByte = bitCountArray[currentByte];
    }

    long currentBitPosition = nextSetBit(bytePosition * 8);
    while (0 < numberOfBitsToSkip && currentBitPosition != -1) {
      currentBitPosition = nextSetBit(currentBitPosition + 1);
      numberOfBitsToSkip--;
    }
    return currentBitPosition;
  }

  /**
   * Obtains the index of the first bit set at the current index position or after.
   * @param index Index of the bit to search from, inclusive.
   * @return The index of the first bit set at or after the given index, or -1 if there are no bits set after the search
   * index.
   */
  public long nextSetBit(long index) {
    int bytePosition = Ints.checkedCast(index / 8);
    int bitPosition = Ints.checkedCast(index % 8);

    if (bytePosition >= nrBytes) {
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

  /**
   * Obtains the index of the first bit set after the current index position.
   * @param index Index of the bit to search from, exclusive.
   * @return The index of the first bit set after the given index, or -1 if there are no bits set after the search
   * index.
   */
  public long nextSetBitAfter(long index) {
    return nextSetBit(index + 1);
  }

  public boolean isBitSet(long index) {
    final int byteToCheck = Ints.checkedCast(index >>> 3);
    assert (byteToCheck < nrBytes);
    byte b = buf.get(byteToCheck);
    //    System.out.println(Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
    final int offset = Ints.checkedCast(7 - index % 8);
    return ((b & (1 << offset)) != 0);
  }

}
