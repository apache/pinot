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

import java.io.Closeable;
import java.io.IOException;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;


public abstract class PinotDataBitSetV2 implements Closeable {
  private static final int BYTE_MASK = 0xFF;
  static final int MAX_VALUES_UNPACKED_SINGLE_ALIGNED_READ = 16; // comes from 2-bit encoding

  private static final ThreadLocal<int[]> THREAD_LOCAL_DICT_IDS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  protected PinotDataBuffer _dataBuffer;
  protected int _numBitsPerValue;

  /**
   * Unpack single dictId at the given docId. This is efficient
   * because of simplified bitmath.
   * @param index docId
   * @return unpacked dictId
   */
  public abstract int readInt(int index);

  /**
   * Unpack dictIds for a contiguous range of docIds represented by startIndex
   * and length. This uses vectorization as much as possible for all the aligned
   * reads and also takes care of the small byte-sized window of unaligned read.
   * @param startIndex start docId
   * @param length length
   * @param out out array to store the unpacked dictIds
   */
  public abstract void readInt(int startIndex, int length, int[] out);

  /**
   * Unpack dictIds for an array of docIds[] which is not necessarily
   * contiguous. So there could be gaps in the array:
   * e.g: [1, 3, 7, 9, 11, 12]
   * The actual read is done by the previous API since that is efficient
   * as it exploits contiguity and uses vectorization. However, since
   * the out[] array has to be correctly populated with the unpacked dictId
   * for each docId, a post-processing step is needed after the bulk contiguous
   * read to correctly set the unpacked dictId into the out array throwing away
   * the unnecessary dictIds unpacked as part of contiguous read
   * @param docIds docIds array
   * @param docIdsStartIndex starting index in the docIds array
   * @param length length to read (number of docIds to read in the array)
   * @param out out array to store the unpacked dictIds
   * @param outpos starting index in the out array
   */
  public void readInt(int[] docIds, int docIdsStartIndex, int length, int[] out, int outpos) {
    int startDocId = docIds[docIdsStartIndex];
    int endDocId = docIds[docIdsStartIndex + length - 1];
    int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
    // do a contiguous bulk read
    readInt(startDocId, endDocId - startDocId + 1, dictIds);
    out[outpos] = dictIds[0];
    // set the unpacked dictId correctly. this is needed since there could
    // be gaps and some dictIds may have to be thrown/ignored.
    for (int i = 1; i < length; i++) {
      out[outpos + i] = dictIds[docIds[docIdsStartIndex + i] - startDocId];
    }
  }

  public static PinotDataBitSetV2 createBitSet(PinotDataBuffer pinotDataBuffer, int numBitsPerValue) {
    switch (numBitsPerValue) {
      case 2:
        return new Bit2Encoded(pinotDataBuffer, numBitsPerValue);
      case 4:
        return new Bit4Encoded(pinotDataBuffer, numBitsPerValue);
      case 8:
        return new Bit8Encoded(pinotDataBuffer, numBitsPerValue);
      case 16:
        return new Bit16Encoded(pinotDataBuffer, numBitsPerValue);
      case 32:
        return new RawInt(pinotDataBuffer, numBitsPerValue);
      default:
        throw new UnsupportedOperationException(numBitsPerValue + "not supported by PinotDataBitSetV2");
    }
  }

  public static class Bit2Encoded extends PinotDataBitSetV2 {
    Bit2Encoded(PinotDataBuffer dataBuffer, int numBits) {
      _dataBuffer = dataBuffer;
      _numBitsPerValue = numBits;
    }

    @Override
    public int readInt(int index) {
      long bitOffset = (long) index * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      int val = (int)_dataBuffer.getByte(byteOffset) & 0xff;
      bitOffset = bitOffset & 7;
      return  (val >>> (6 - bitOffset)) & 3;
    }

    @Override
    public void readInt(int startIndex, int length, int[] out) {
      long bitOffset = (long) startIndex * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      bitOffset = bitOffset & 7;
      int packed = 0;
      int i = 0;

      /*
       * Bytes are read as follows to get maximum vectorization
       *
       * [1 byte] - read from either the 2nd/4th/6th bit to 7th bit to unpack 1/2/3 integers
       * [chunks of 4 bytes] - read 4 bytes at a time to unpack 16 integers
       * [1 chunk of 2 bytes] - read 2 bytes to unpack 8 integers
       * [1 byte] - read the byte to unpack 4 integers
       * [1 byte] - unpack 1/2/3 integers from first 2/4/6 bits
       */

      // unaligned read within a byte
      if (bitOffset != 0) {
        packed = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        if (bitOffset == 2) {
          // unpack 3 integers from bits 2-7
          out[0] = (packed >>> 4) & 3;
          out[1] = (packed >>> 2) & 3;
          out[2] = packed & 3;
          i = 3;
          length -= 3;
        }
        else if (bitOffset == 4) {
          // unpack 2 integers from bits 4 to 7
          out[0] = (packed >>> 2) & 3;
          out[1] = packed & 3;
          i = 2;
          length -= 2;
        } else {
          // unpack integer from bits 6 to 7
          out[0] = packed & 3;
          i = 1;
          length -= 1;
        }
        byteOffset++;
      }

      // aligned reads at 4-byte boundary to unpack 16 integers
      while (length >= 16) {
        packed = _dataBuffer.getInt(byteOffset);
        out[i] = packed >>> 30;
        out[i + 1] = (packed >>> 28) & 3;
        out[i + 2] = (packed >>> 26) & 3;
        out[i + 3] = (packed >>> 24) & 3;
        out[i + 4] = (packed >>> 22) & 3;
        out[i + 5] = (packed >>> 20) & 3;
        out[i + 6] = (packed >>> 18) & 3;
        out[i + 7] = (packed >>> 16) & 3;
        out[i + 8] = (packed >>> 14) & 3;
        out[i + 9] = (packed >>> 12) & 3;
        out[i + 10] = (packed >>> 10) & 3;
        out[i + 11] = (packed >>> 8) & 3;
        out[i + 12] = (packed >>> 6) & 3;
        out[i + 13] = (packed >>> 4) & 3;
        out[i + 14] = (packed >>> 2) & 3;
        out[i + 15] = packed & 3;
        length -= 16;
        byteOffset += 4;
        i += 16;
      }

      if (length >= 8) {
        packed = (int)_dataBuffer.getShort(byteOffset) & 0xffff;
        out[i] = (packed >>> 14) & 3;
        out[i + 1] = (packed >>> 12) & 3;
        out[i + 2] = (packed >>> 10) & 3;
        out[i + 3] = (packed >>> 8) & 3;
        out[i + 4] = (packed >>> 6) & 3;
        out[i + 5] = (packed >>> 4) & 3;
        out[i + 6] = (packed >>> 2) & 3;
        out[i + 7] = packed & 3;
        length -= 8;
        byteOffset += 2;
        i += 8;
      }

      // aligned read at byte boundary to unpack 4 integers
      if (length >= 4) {
        packed = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        out[i] = packed >>> 6;
        out[i + 1] = (packed >>> 4) & 3;
        out[i + 2] = (packed >>> 2) & 3;
        out[i + 3] = packed & 3;
        length -= 4;
        byteOffset++;
        i += 4;
      }

      // handle spill-over

      if (length > 0) {
        // unpack from bits 0-1
        packed = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        out[i] = packed >>> 6;
        length--;
      }

      if (length > 0) {
        // unpack from bits 2-3
        out[i + 1] = (packed >>> 4) & 3;
        length--;
      }

      if (length > 0) {
        // unpack from bits 4-5
        out[i + 2] = (packed >>> 2) & 3;
        length--;
      }
    }
  }

  public static class Bit4Encoded extends PinotDataBitSetV2 {
    Bit4Encoded(PinotDataBuffer dataBuffer, int numBits) {
      _dataBuffer = dataBuffer;
      _numBitsPerValue = numBits;
    }

    @Override
    public int readInt(int index) {
      long bitOffset = (long) index * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      int val = (int)_dataBuffer.getByte(byteOffset) & 0xff;
      bitOffset = bitOffset & 7;
      return (bitOffset == 0) ? val >>> 4 : val & 0xf;
    }

    @Override
    public void readInt(int startIndex, int length, int[] out) {
      long bitOffset = (long) startIndex * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      bitOffset = bitOffset & 7;
      int packed = 0;
      int i = 0;

      /*
       * Bytes are read as follows to get maximum vectorization
       *
       * [1 byte] - read from the 4th bit to 7th bit to unpack 1 integer
       * [chunks of 4 bytes] - read 4 bytes at a time to unpack 8 integers
       * [1 chunk of 2 bytes] - read 2 bytes to unpack 4 integers
       * [1 byte] - unpack 1 integer from first 4 bits
       */

      // unaligned read within a byte from bits 4-7
      if (bitOffset != 0) {
        packed = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        out[0] = packed & 0xf;
        i = 1;
        byteOffset++;
        length--;
      }

      // aligned read at 4-byte boundary to unpack 8 integers
      while (length >= 8) {
        packed = _dataBuffer.getInt(byteOffset);
        out[i] = packed >>> 28;
        out[i + 1] = (packed >>> 24) & 0xf;
        out[i + 2] = (packed >>> 20) & 0xf;
        out[i + 3] = (packed >>> 16) & 0xf;
        out[i + 4] = (packed >>> 12) & 0xf;
        out[i + 5] = (packed >>> 8) & 0xf;
        out[i + 6] = (packed >>> 4) & 0xf;
        out[i + 7] = packed & 0xf;
        length -= 8;
        i += 8;
        byteOffset += 4;
      }

      // aligned read at 2-byte boundary to unpack 4 integers
      if (length >= 4) {
        packed = (int)_dataBuffer.getShort(byteOffset) & 0xffff;
        out[i] = (packed >>> 12) & 0xf;
        out[i + 1] = (packed >>> 8) & 0xf;
        out[i + 2] = (packed >>> 4) & 0xf;
        out[i + 3] = packed & 0xf;
        length -= 4;
        i += 4;
        byteOffset += 2;
      }

      // aligned read at byte boundary to unpack 2 integers
      if (length >= 2) {
        packed = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        out[i] = packed >>> 4;
        out[i + 1] = packed & 0xf;
        length -= 2;
        i += 2;
        byteOffset++;
      }

      // handle spill over -- unpack from bits 0-3
      if (length > 0) {
        packed = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        out[i] = packed >>> 4;
        length--;
      }
    }
  }

  public static class Bit8Encoded extends PinotDataBitSetV2 {
    Bit8Encoded(PinotDataBuffer dataBuffer, int numBits) {
      _dataBuffer = dataBuffer;
      _numBitsPerValue = numBits;
    }

    @Override
    public int readInt(int index) {
      long bitOffset = (long) index * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      return ((int)_dataBuffer.getByte(byteOffset)) & 0xff;
    }

    @Override
    public void readInt(int startIndex, int length, int[] out) {
      long bitOffset = (long) startIndex * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      int i = 0;
      int packed = 0;

      /*
       * Bytes are read as follows to get maximum vectorization
       *
       * [chunks of 4 bytes] - read 4 bytes at a time to unpack 4 integers
       * [1 chunk of 2 bytes] - read 2 bytes to unpack 4 integers
       * [1 byte] - unpack 1 integer from first 4 bits
       */

      // aligned read at 4-byte boundary to unpack 4 integers
      while (length >= 4) {
        packed = _dataBuffer.getInt(byteOffset);
        out[i] = packed >>> 24;
        out[i + 1] = (packed >>> 16) & 0xff;
        out[i + 2] = (packed >>> 8) & 0xff;
        out[i + 3] = packed & 0xff;
        length -= 4;
        byteOffset += 4;
        i += 4;
      }

      // aligned read at 2-byte boundary to unpack 2 integers
      if (length >= 2) {
        packed = (int)_dataBuffer.getShort(byteOffset) & 0xffff;
        out[i] = (packed >>> 8) & 0xff;
        out[i + 1] = packed & 0xff;
        length -= 2;
        byteOffset += 2;
        i += 2;
      }

      // handle spill over at byte boundary to unpack 1 integer
      if (length > 0) {
        out[i] = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        length--;
      }
    }
  }

  public static class Bit16Encoded extends PinotDataBitSetV2 {
    Bit16Encoded(PinotDataBuffer dataBuffer, int numBits) {
      _dataBuffer = dataBuffer;
      _numBitsPerValue = numBits;
    }

    @Override
    public int readInt(int index) {
      long bitOffset = (long) index * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      return ((int)_dataBuffer.getShort(byteOffset)) & 0xffff;
    }

    @Override
    public void readInt(int startIndex, int length, int[] out) {
      long bitOffset = (long) startIndex * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      int i = 0;
      int packed;

      /*
       * Bytes are read as follows to get maximum vectorization
       *
       * [chunks of 4 bytes] - read 4 bytes at a time to unpack 2 integers
       * [1 chunk of 2 bytes] - read 2 bytes to unpack 1 integer
       */

      // aligned reads at 4-byte boundary to unpack 2 integers
      while (length >= 2) {
        packed = _dataBuffer.getInt(byteOffset);
        out[i] = packed >>> 16;
        out[i + 1] = packed & 0xffff;
        length -= 2;
        i += 2;
        byteOffset += 4;
      }

      // handle spill over at 2-byte boundary to unpack 1 integer
      if (length > 0) {
        out[i] = (int)_dataBuffer.getShort(byteOffset) & 0xffff;
        length--;
      }
    }
  }

  public static class RawInt extends PinotDataBitSetV2 {
    RawInt(PinotDataBuffer dataBuffer, int numBits) {
      _dataBuffer = dataBuffer;
      _numBitsPerValue = numBits;
    }

    @Override
    public int readInt(int index) {
      return _dataBuffer.getInt(index * Integer.BYTES);
    }

    @Override
    public void readInt(int startIndex, int length, int[] out) {
      int byteOffset = startIndex * Integer.BYTES;
      for (int i = 0; i < length; i++) {
        out[i] = _dataBuffer.getInt(byteOffset);
        byteOffset += 4;
      }
    }
  }

  protected void writeInt(int index, int value) {
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

  @Override
  public void close()
      throws IOException {
    _dataBuffer.close();
  }
}