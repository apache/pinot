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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


public class ValueReaderComparisons {
  private ValueReaderComparisons() {
  }

  private static int mismatch(PinotDataBuffer dataBuffer, long startOffset, int length, ByteBuffer buffer) {
    boolean littleEndian = dataBuffer.order() == ByteOrder.LITTLE_ENDIAN;
    if (littleEndian) {
      buffer.order(ByteOrder.LITTLE_ENDIAN);
    }
    int limit = Math.min(length, buffer.limit());
    int loopBound = limit & ~0x7;
    int i = 0;
    for (; i < loopBound; i += 8) {
      long ours = dataBuffer.getLong(startOffset + i);
      long theirs = buffer.getLong(i);
      if (ours != theirs) {
        long difference = ours ^ theirs;
        return i + ((littleEndian ? Long.numberOfTrailingZeros(difference) : Long.numberOfLeadingZeros(difference))
            >>> 3);
      }
    }
    for (; i < limit; i++) {
      byte ours = dataBuffer.getByte(startOffset + i);
      byte theirs = buffer.get(i);
      if (ours != theirs) {
        return i;
      }
    }
    return -1;
  }

  static int compareBytes(PinotDataBuffer dataBuffer, long startOffset, int length, byte[] bytes) {
    // can use MethodHandles.byteArrayViewVarHandle here after dropping JDK8
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int mismatchPosition = mismatch(dataBuffer, startOffset, length, buffer);
    if (mismatchPosition == -1) {
      return length - bytes.length;
    }
    // can use Byte.compareUnsigned here after dropping JDK8
    return (dataBuffer.getByte(startOffset + mismatchPosition) & 0xFF) - (bytes[mismatchPosition] & 0xFF);
  }

  static int compareUtf8Bytes(PinotDataBuffer dataBuffer, long startOffset, int length, boolean padded, byte[] bytes) {
    // can use MethodHandles.byteArrayViewVarHandle here after dropping JDK8
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int mismatchPosition = mismatch(dataBuffer, startOffset, length, buffer);
    if (mismatchPosition == -1) {
      if (padded && bytes.length < length) {
        // check if the stored string continues beyond the length of the parameter
        return dataBuffer.getByte(startOffset + bytes.length) == 0 ? 0 : 1;
      } else {
        // then we know the length precisely or know that the parameter is at least as long as we can store
        return length - bytes.length;
      }
    }
    // we know the position of the mismatch but need to do utf8 decoding before comparison
    // to respect collation rules
    return compareUtf8(dataBuffer, startOffset, buffer, mismatchPosition);
  }

  private static int compareUtf8(PinotDataBuffer ourBuffer, long ourStartOffset, ByteBuffer theirBuffer,
      int mismatchPosition) {
    char ours1 = '\ufffd';
    char ours2 = '\ufffd';
    char theirs1 = '\ufffd';
    char theirs2 = '\ufffd';

    // 1. seek backwards from mismatch position to find start of each utf8 sequence
    //    assuming we have valid UTF-8 and knowing that the content before mismatchPosition is
    //    identical, we will go back the same distance in each buffer
    while (mismatchPosition > 0 && isUtf8Continuation(theirBuffer.get(mismatchPosition))) {
      mismatchPosition--;
    }
    // 2. decode to get the 1 or 2 characters containing where the mismatch lies
    {
      long position = ourStartOffset + mismatchPosition;
      byte first = ourBuffer.getByte(position);
      int control = first & 0xF0;
      if (first >= 0) {
        ours1 = (char) (first & 0xFF);
      } else if (control < 0xE0) {
        ours1 = decode(first, ourBuffer.getByte(position + 1));
      } else if (control == 0xE0) {
        ours1 = decode(first, ourBuffer.getByte(position + 1), ourBuffer.getByte(position + 2));
      } else {
        int codepoint = decode(first, ourBuffer.getByte(position + 1), ourBuffer.getByte(position + 2),
            ourBuffer.getByte(position + 3));
        if (Character.isValidCodePoint(codepoint)) {
          ours1 = Character.highSurrogate(codepoint);
          ours2 = Character.lowSurrogate(codepoint);
        }
      }
    }
    {
      byte first = theirBuffer.get(mismatchPosition);
      int control = first & 0xF0;
      if (first >= 0) {
        theirs1 = (char) (first & 0xFF);
      } else if (control < 0xE0) {
        theirs1 = decode(first, theirBuffer.get(mismatchPosition + 1));
      } else if (control == 0xE0) {
        theirs1 = decode(first, theirBuffer.get(mismatchPosition + 1), theirBuffer.get(mismatchPosition + 2));
      } else {
        int codepoint = decode(first, theirBuffer.get(mismatchPosition + 1), theirBuffer.get(mismatchPosition + 2),
            theirBuffer.get(mismatchPosition + 3));
        if (Character.isValidCodePoint(codepoint)) {
          theirs1 = Character.highSurrogate(codepoint);
          theirs2 = Character.lowSurrogate(codepoint);
        }
      }
    }
    // 3. compare the first characters to differ
    return ours1 == theirs1 ? Character.compare(ours2, theirs2) : Character.compare(ours1, theirs1);
  }

  private static char decode(int b1, int b2) {
    return (char) (((b1 << 6) ^ b2) ^ (((byte) 0xC0 << 6) ^ ((byte) 0x80)));
  }

  private static char decode(int b1, int b2, int b3) {
    return (char) ((b1 << 12) ^ (b2 << 6) ^ (b3 ^ (((byte) 0xE0 << 12) ^ ((byte) 0x80 << 6) ^ ((byte) 0x80))));
  }

  private static int decode(int b1, int b2, int b3, int b4) {
    return ((b1 << 18) ^ (b2 << 12) ^ (b3 << 6) ^ (b4 ^ (((byte) 0xF0 << 18) ^ ((byte) 0x80 << 12) ^ ((byte) 0x80 << 6)
        ^ ((byte) 0x80))));
  }

  private static boolean isUtf8Continuation(byte value) {
    return (value & 0xC0) == 0x80;
  }
}
