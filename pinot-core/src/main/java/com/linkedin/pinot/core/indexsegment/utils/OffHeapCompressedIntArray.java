/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.indexsegment.utils;

import com.linkedin.pinot.common.utils.MmapUtils;
import java.nio.ByteBuffer;


/**
 * Sept 15, 2012
 *
 */
public class OffHeapCompressedIntArray implements IntArray {
  private final ByteBuffer buf;
  private int capacity;
  private final int numOfBitsPerElement;
  private byte[] tempBuf;

  public OffHeapCompressedIntArray(int numOfElements, int numOfBitsPerElement) {
    this.numOfBitsPerElement = numOfBitsPerElement;
    capacity = numOfElements;
    long requiredBufferSize = getRequiredBufferSize(numOfElements, numOfBitsPerElement);
    assert (requiredBufferSize <= Integer.MAX_VALUE);
    buf = MmapUtils.allocateDirectByteBuffer((int) requiredBufferSize, null, this.getClass().getSimpleName() + " buf");
    tempBuf = getByteBuf();
  }

  public static int getRequiredBufferSize(int numOfElements, int numOfBitsPerElement) {
    return ((int) numOfElements * numOfBitsPerElement + 7) / 8;
  }

  public static int getNumOfBits(int dictionarySize) {
    int ret = (int) Math.ceil(Math.log(dictionarySize) / Math.log(2));
    if (ret == 0) {
      ret = 1;
    }
    return ret;
  }

  public OffHeapCompressedIntArray(int numOfElements, int numOfBitsPerElement, ByteBuffer byteBuffer) {
    this.numOfBitsPerElement = numOfBitsPerElement;
    capacity = numOfElements;
    buf = byteBuffer;
    tempBuf = getByteBuf();
  }

  /**
   * We can reuse this buffer for multiple calls
   */
  public byte[] getByteBuf() {
    return new byte[numOfBitsPerElement / 8 + (numOfBitsPerElement % 8 < 2 ? 1 : 2)];
  }

  /**
   * This method is not threadsafe
  * @param position
  * @param number
  */
  public void setInt(int position, int number) {
    addInt(position, number, tempBuf);
  }

  public void addInt(int position, int number, byte[] tempBuf) {
    int bytePosition = position * numOfBitsPerElement / 8;
    int startBitOffset = (position * numOfBitsPerElement) % 8;
    int endBitOffset = (8 - ((startBitOffset + numOfBitsPerElement) % 8)) % 8;
    int numberOfBytesUsed =
        (startBitOffset + numOfBitsPerElement) / 8 + ((startBitOffset + numOfBitsPerElement) % 8 != 0 ? 1 : 0);
    buf.position(bytePosition);
    buf.get(tempBuf, 0, numberOfBytesUsed);
    long newNumber = tempBuf[0];
    if (startBitOffset > 0) {
      newNumber >>= 8 - startBitOffset;
    }
    newNumber <<= numOfBitsPerElement;
    newNumber |= number;
    if (endBitOffset != 0) {
      newNumber <<= endBitOffset;
      newNumber |= tempBuf[numberOfBytesUsed - 1] & 0xFF >>> (8 - endBitOffset);
    }
    for (int i = numberOfBytesUsed - 1; i >= 0; i--) {
      tempBuf[i] = (byte) (newNumber & 0xFF);

      newNumber = newNumber >> 8;
    }
    buf.position(bytePosition);
    buf.put(tempBuf, 0, numberOfBytesUsed);
  }

  public int getInt(int position) {

    /*
     * int bytePosition = position * numOfBitsPerElement / 8; int startBitOffset
     * = (position * numOfBitsPerElement) % 8; int endBitOffset = (8 -
     * ((startBitOffset + numOfBitsPerElement) % 8)) % 8; int numberOfBytesUsed
     * = (startBitOffset + numOfBitsPerElement) / 8 + ((startBitOffset +
     * numOfBitsPerElement) % 8 != 0 ? 1 : 0);buf.position(bytePosition);
     */
    int mult = position * numOfBitsPerElement;
    int bytePosition = mult >>> 3;
    int startBitOffset = mult & 7;
    int sum = startBitOffset + numOfBitsPerElement;
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
    number &= (0xFFFFFFFF >>> (32 - numOfBitsPerElement));
    return (int) number;
  }

  public int size() {
    return capacity;
  }

  public ByteBuffer getStorage() {
    return buf;
  }

  public int getNumOfBitsPerElement() {
    return numOfBitsPerElement;
  }

}
