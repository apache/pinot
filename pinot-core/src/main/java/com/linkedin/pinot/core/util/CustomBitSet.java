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

  public static CustomBitSet withByteBuffer(final int numBytes,
      ByteBuffer byteBuffer) {
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

}
