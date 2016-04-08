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

import com.linkedin.pinot.core.indexsegment.utils.BitUtils;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.Closeable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 * Util class to store bit set, provides additional utility over java bit set by
 * allowing reading int from start bit to end bit
 */
public final class PinotDataCustomBitSet implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotDataCustomBitSet.class);

  private final int nrBytes;
  private PinotDataBuffer buf;
  private final static int[] bitCountArray = new int[256];
  private final static int IGNORED_ZEROS_COUNT = Integer.SIZE - Byte.SIZE;
  private final boolean ownsByteBuffer;

  static {
    for (int i = 0; i < 256; i++) {
      bitCountArray[i] = Integer.bitCount(i);
    }
  }

  private PinotDataCustomBitSet(final int nrBytes) {
    if (nrBytes < 1) {
      throw new IllegalArgumentException("CustomBitSet requires at least one byte of storage, asked for " + nrBytes);
    }
    this.nrBytes = nrBytes;
    // FIXME
    buf = PinotDataBuffer.allocateDirect(nrBytes);
    ownsByteBuffer = true;
  }

  private PinotDataCustomBitSet(final int nrBytes, final PinotDataBuffer buffer) {
    if (nrBytes < 1) {
      throw new IllegalArgumentException("CustomBitSet requires at least one byte of storage, asked for " + nrBytes);
    }
    if (nrBytes * 8 >= Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Requested bit set capacity is " + nrBytes
          + " bytes, but number of bits exceeds the Integer.MAX_VALUE and since we use int data type to return position, we will overflow int.");
    }
    if (buffer.size() < nrBytes) {
      throw new IllegalArgumentException("Requested bit set capacity is " + nrBytes +
          " bytes, but the underlying byte buffer has a capacity of " + buffer.size() +
          ", which is less than requested");
    }
    this.nrBytes = nrBytes;
    this.buf = buffer;
    ownsByteBuffer = false;
  }

  public static PinotDataCustomBitSet withDataBuffer(final int numBytes, PinotDataBuffer byteBuffer) {
    return new PinotDataCustomBitSet(numBytes, byteBuffer);
  }

  public static PinotDataCustomBitSet withByteLength(final int nrBytes) {
    return new PinotDataCustomBitSet(nrBytes);
  }

  public static PinotDataCustomBitSet withBitLength(final int nrBits) {
    return new PinotDataCustomBitSet((nrBits - 1) / 8 + 1);
  }

  public void setBit(final long bitOffset) {
    if (bitOffset < 0) {
      throw new IllegalArgumentException("Negative bitOffset value " + bitOffset);
    }

    final int byteToSet = (int)(bitOffset / 8);
    if (byteToSet > nrBytes) {
      throw new IllegalArgumentException("bitOffset value " + bitOffset + " (byte offset " + byteToSet + ") exceeds buffer capacity of " + nrBytes + " bytes");
    }

    byte b = buf.getByte(byteToSet);
    byte posBit = (byte) (1 << (7 - (bitOffset % 8)));
    // System.out.println("bitOffset:" + bitOffset + " posBit:" + posBit);
    b |= posBit;
    buf.putByte(byteToSet, b);
  }

  public void unsetBit(final long bitOffset) {
    if (bitOffset < 0) {
      throw new IllegalArgumentException("Negative bitOffset value " + bitOffset);
    }
    final int byteToSet = (int)(bitOffset / 8);
    if (byteToSet > nrBytes) {
      throw new IllegalArgumentException("bitOffset value " + bitOffset + " (byte offset " + byteToSet + ") exceeds buffer capacity of " + nrBytes + " bytes");
    }

    final int offset = (int)(bitOffset % 8);
    byte b = buf.getByte(byteToSet);
    b &= ~(1 << (7 - offset));
    buf.putByte(byteToSet, b);
  }

  /**
   * reads the read between the start (inclusive) and end (exclusive)
   *
   * @return
   */
  public int readInt(long startBitIndex, long endBitIndex) {
    int bitLength = (int) (endBitIndex - startBitIndex);
    if (bitLength < 16 && endBitIndex + 32 < nrBytes * 8L) {
      int bytePosition = (int) (startBitIndex / 8);
      int bitOffset = (int) (startBitIndex % 8);
      int shiftOffset = 32 - (bitOffset + bitLength);
      int intValue = buf.getInt(bytePosition);
      int bitMask = (1 << bitLength) - 1;
      return (intValue >> shiftOffset) & bitMask;
    } else {
      int bytePosition = (int) (startBitIndex >>> 3);
      int startBitOffset = (int) (startBitIndex & 7);
      int sum = startBitOffset + bitLength;
      int endBitOffset = (8 - (sum & 7)) & 7;

      // int numberOfBytesUsed = (sum >>> 3) + ((sum & 7) != 0 ? 1 : 0);
      int numberOfBytesUsed = (sum + 7) >>> 3;
      int i = -1;

      long number = 0;
      while (true) {
        number |= (buf.getByte(bytePosition)) & 0xFF;
        i++;
        bytePosition++;
        if (i == numberOfBytesUsed - 1) {
          break;
        }
        number <<= 8;
      }
      number >>= endBitOffset;
      number &= (0xFFFFFFFF >>> (32 - bitLength));
      return (int) number;
    }
  }

  public void writeInt(long startBitIndex, int bitLength, int value) {
    if (bitLength < 16 && startBitIndex + bitLength + 32 < nrBytes * 8L) {
      int bytePosition = (int) (startBitIndex / 8);
      int bitOffset = (int) (startBitIndex % 8);
      int shiftOffset = 32 - (bitOffset + bitLength);
      int intValue = buf.getInt(bytePosition);
      int bitMask = ((1 << bitLength) - 1) << shiftOffset;
      int updatedIntValue = (intValue & ~bitMask) | ((value << shiftOffset) & bitMask);
      buf.putInt(bytePosition, updatedIntValue);
    } else {
      for (int bitPos = bitLength - 1; bitPos >= 0; bitPos--) {
        if ((value & (1 << bitPos)) != 0) {
          setBit(startBitIndex + (bitLength - bitPos - 1));
        }
      }
    }
  }

  public byte[] toByteArray() {
    if (buf.size() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Can not convert Pinot data buffer of size > 2GB to byte array");
    }
    byte[] dst = new byte[(int)buf.size()];
    buf.copyTo(0, dst, 0, (int) buf.size());
    return dst;
  }

  @Override
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
  public int findNthBitSetAfter(int startBitIndex, int n) {
    int searchStartBitIndex = startBitIndex + 1;

    int bytePosition = (searchStartBitIndex / 8);
    int bitPosition = (searchStartBitIndex % 8);
    if (bytePosition >= nrBytes) {
      return -1;
    }

    int currentByte = (buf.getByte(bytePosition) << bitPosition) & 0xFF;
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
      currentByte = buf.getByte(bytePosition) & 0xFF;
      numberOfBitsOnInCurrentByte = bitCountArray[currentByte];
    }

    int currentBitPosition = nextSetBit(bytePosition * 8);
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
  public int nextSetBit(int index) {
    int bytePosition = (index / 8);
    int bitPosition = (index % 8);

    if (bytePosition >= nrBytes) {
      return -1;
    }

    // Assuming index 3
    // --- IGNORED_ZEROS_COUNT --
    //                             index
    //                               v
    // 00000000 00000000 00000000 00000010
    int currentByte = (buf.getByte(bytePosition) << bitPosition) & 0xFF;

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
      currentByte = buf.getByte(bytePosition + bytesSkipped) & 0xFF;
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
  public int nextSetBitAfter(int index) {
    return nextSetBit(index + 1);
  }

  public boolean isBitSet(int index) {
    final int byteToCheck = (int)(index >>> 3);
    assert (byteToCheck < nrBytes);
    byte b = buf.getByte(byteToCheck);
    //    System.out.println(Integer.toBinaryString((b & 0xFF) + 0x100).substring(1));
    final int offset = (int)(7 - index % 8);
    return ((b & (1 << offset)) != 0);
  }

  @Override
  public void close() {
    buf.close();
    buf = null;
  }
}
