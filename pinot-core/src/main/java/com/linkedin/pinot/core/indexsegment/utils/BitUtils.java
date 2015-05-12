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
package com.linkedin.pinot.core.indexsegment.utils;

import java.nio.ByteBuffer;


/**
 *
 * @author Dhaval Patel<dpatel@linkedin.com
 * Aug 19, 2014
 */
public class BitUtils {
  static public long makeLong(byte b7, byte b6, byte b5, byte b4, byte b3, byte b2, byte b1, byte b0) {
    return ((((long) b7 & 0xff) << 56) | (((long) b6 & 0xff) << 48) | (((long) b5 & 0xff) << 40)
        | (((long) b4 & 0xff) << 32) | (((long) b3 & 0xff) << 24) | (((long) b2 & 0xff) << 16)
        | (((long) b1 & 0xff) << 8) | (((long) b0 & 0xff) << 0));
  }

  public static long getLong(ByteBuffer byteBuffer, int longIndex) {
    int index = longIndex * 8;
    if (index >= byteBuffer.limit()) {
      return 0L;
    }
    if (byteBuffer.limit() >= index + 8) {
      return makeLong(byteBuffer.get(index + 0), byteBuffer.get(index + 1), byteBuffer.get(index + 2),
          byteBuffer.get(index + 3), byteBuffer.get(index + 4), byteBuffer.get(index + 5), byteBuffer.get(index + 6),
          byteBuffer.get(index + 7));
    } else {
      byte[] buffer = new byte[8];
      byteBuffer.position(index);
      byteBuffer.get(buffer, 0, byteBuffer.limit() - index);
      return makeLong(buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7]);
    }
  }

  /**
   * Turns off the nth leftmost set bits, for example turning the 3 leftmost set bits of 11001010 would return 00000010.
   * @param value The value for which to turn off the leftmost set bits.
   * @param bitsToTurnOffCount The number of bits to turn off
   * @return The value with the nth leftmost bits turned off.
   */
  public static int turnOffNthLeftmostSetBits(int value, int bitsToTurnOffCount) {
    // Turn off the bitsToTurnOffCount rightmost bits to skip by flipping the bit order, turning off the leftmost bits
    // in a loop, then flipping the word back in place and counting the number of leading zeroes
    if (0 < bitsToTurnOffCount) {
      int reversedByte = (Integer.reverse(value) >>> 24) & 0xFF;
      for (int i = 0; i < bitsToTurnOffCount; i++) {
        reversedByte = reversedByte & (reversedByte - 1);
      }
      value = (Integer.reverse(reversedByte) >>> 24) & 0xFF;
    }

    return value;
  }
}
