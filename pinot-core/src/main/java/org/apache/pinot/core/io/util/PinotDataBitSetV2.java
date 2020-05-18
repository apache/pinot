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

  private static final ThreadLocal<int[]> THREAD_LOCAL_DICT_IDS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  protected PinotDataBuffer _dataBuffer;
  protected int _numBitsPerValue;

  public abstract int readInt(int index);

  public abstract void readInt(int startIndex, int length, int[] out);

  public abstract void readInt(int[] rows, int rowsStartIndex, int length, int[] out, int outpos);

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
      if (bitOffset == 0) {
        return val >>> 6;
      }
      if (bitOffset == 2) {
        return (val >>> 4) & 3;
      }
      if (bitOffset == 4) {
        return (val >>> 2) & 3;
      }
      if (bitOffset == 6) {
        return val & 3;
      }
      throw new IllegalStateException("Unexpected bit offset for 2 bit-encoding for index: " + index);
    }

    @Override
    public void readInt(int startIndex, int length, int[] out) {
      long bitOffset = (long) startIndex * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      bitOffset = bitOffset & 7;
      int val;
      int i = 0;
      if (bitOffset != 0) {
        val = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        if (bitOffset == 2) {
          out[0] = (val >>> 4) & 3;
          out[1] = (val >>> 2) & 3;
          out[2] = val & 3;
          i = 3;
        }
        else if (bitOffset == 4) {
          out[0] = (val >>> 2) & 3;
          out[1] = val & 3;
          i = 2;
        } else {
          out[0] = val & 3;
          i = 1;
        }
        byteOffset++;
      }
      while (i < length) {
        val = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        out[i] = val >>> 6;
        out[i + 1] = (val >>> 4) & 3;
        out[i + 2] = (val >>> 2) & 3;
        out[i + 3] = val & 3;
        i += 4;
      }
    }

    @Override
    public void readInt(int[] docIds, int docIdsStartIndex, int length, int[] out, int outpos) {
      int startDocId = docIds[docIdsStartIndex];
      int endDocId = docIds[docIdsStartIndex + length - 1];
      long bitOffset = (long) startDocId * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      for (int i = startDocId; i <= endDocId; i += 16) {
        int val = _dataBuffer.getInt(byteOffset);
        dictIds[i] = val >>> 30;
        dictIds[i + 1] = (val >>> 28) & 3;
        dictIds[i + 2] = (val >>> 26) & 3;
        dictIds[i + 3] = (val >>> 24) & 3;
        dictIds[i + 4] = (val >>> 22) & 3;
        dictIds[i + 5] = (val >>> 20) & 3;
        dictIds[i + 6] = (val >>> 18) & 3;
        dictIds[i + 7] = (val >>> 16) & 3;
        dictIds[i + 8] = (val >>> 14) & 3;
        dictIds[i + 9] = (val >>> 12) & 3;
        dictIds[i + 10] = (val >>> 10) & 3;
        dictIds[i + 11] = (val >>> 8) & 3;
        dictIds[i + 12] = (val >>> 6) & 3;
        dictIds[i + 13] = (val >>> 4) & 3;
        dictIds[i + 14] = (val >>> 2) & 3;
        dictIds[i + 15] = val & 3;
        byteOffset += 4;
      }
      for (int i = 0; i < length; i++) {
        out[outpos + i] = dictIds[docIds[docIdsStartIndex + i]];
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
      int val;
      int i = 0;
      if (bitOffset != 0) {
        val = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        out[0] = val & 0xf;
        i = 1;
        byteOffset++;
      }
      while (i < length){
        val = (int)_dataBuffer.getByte(byteOffset) & 0xff;
        out[i] = val >>> 4;
        out[i + 1] = val & 0xf;
        byteOffset++;
        i += 2;
      }
    }

    @Override
    public void readInt(int[] docIds, int docIdsStartIndex, int length, int[] out, int outpos) {
      int startDocId = docIds[docIdsStartIndex];
      int endDocId = docIds[docIdsStartIndex + length - 1];
      long bitOffset = (long) startDocId * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      for (int i = startDocId; i <= endDocId; i += 8) {
        int val = _dataBuffer.getInt(byteOffset);
        dictIds[i] = val >>> 28;
        dictIds[i + 1] = (val >>> 24) & 0xf;
        dictIds[i + 2] = (val >>> 20) & 0xf;
        dictIds[i + 3] = (val >>> 16) & 0xf;
        dictIds[i + 4] = (val >>> 12) & 0xf;
        dictIds[i + 5] = (val >>> 8) & 0xf;
        dictIds[i + 6] = (val >>> 4) & 0xf;
        dictIds[i + 7] = val & 0xf;
        byteOffset += 4;
      }
      for (int i = 0; i < length; i++) {
        out[outpos + i] = dictIds[docIds[docIdsStartIndex + i]];
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
      // due to sign-extension, this will result in -ve value
      int val = _dataBuffer.getByte(byteOffset);
      // use the mask to get +ve for the 8 bits
      return val & 0xff;
    }

    @Override
    public void readInt(int startIndex, int length, int[] out) {
      long bitOffset = (long) startIndex * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      for (int i = 0; i < length; i++) {
        int val = _dataBuffer.getByte(byteOffset);
        out[i] = val & 0xff;
        byteOffset++;
      }
    }

    @Override
    public void readInt(int[] docIds, int docIdsStartIndex, int length, int[] out, int outpos) {
      int startDocId = docIds[docIdsStartIndex];
      int endDocId = docIds[docIdsStartIndex + length - 1];
      long bitOffset = (long) startDocId * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      for (int i = startDocId; i <= endDocId; i += 4) {
        int val = _dataBuffer.getInt(byteOffset);
        dictIds[i] = val >>> 24;
        dictIds[i + 1] = (val >>> 16) & 0xff;
        dictIds[i + 2] = (val >>> 8) & 0xff;
        dictIds[i + 1] = val & 0xff;
        byteOffset += 4;
      }
      for (int i = 0; i < length; i++) {
        out[outpos + i] = dictIds[docIds[docIdsStartIndex + i]];
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
      // due to sign-extension, this will result in -ve value
      int val = _dataBuffer.getShort(byteOffset);
      // use the mask to get +ve value for the 16 bits
      return val & 0xffff;
    }

    @Override
    public void readInt(int startIndex, int length, int[] out) {
      long bitOffset = (long) startIndex * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      for (int i = 0; i < length; i++) {
        // due to sign-extension, this will result in -ve value
        int val = _dataBuffer.getShort(byteOffset);
        // use the mask to get positive value for the 16 bits
        out[i] = val & 0xffff;
        byteOffset += 2;
      }
    }

    @Override
    public void readInt(int[] docIds, int docIdsStartIndex, int length, int[] out, int outpos) {
      int startDocId = docIds[docIdsStartIndex];
      int endDocId = docIds[docIdsStartIndex + length - 1];
      long bitOffset = (long) startDocId * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      for (int i = startDocId; i <= endDocId; i += 2) {
        int val = _dataBuffer.getInt(byteOffset);
        dictIds[i] = val >>> 16;
        dictIds[i + 1] = val & 0xffff;
        byteOffset += 4;
      }
      for (int i = 0; i < length; i++) {
        out[outpos + i] = dictIds[docIds[docIdsStartIndex + i]];
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

    @Override
    public void readInt(int[] docIds, int docIdsStartIndex, int length, int[] out, int outpos) {
      int startDocId = docIds[docIdsStartIndex];
      int endDocId = docIds[docIdsStartIndex + length - 1];
      long bitOffset = (long) startDocId * _numBitsPerValue;
      int byteOffset = (int) (bitOffset / Byte.SIZE);
      int[] dictIds = THREAD_LOCAL_DICT_IDS.get();
      for (int i = startDocId; i <= endDocId; i++) {
        dictIds[i] = _dataBuffer.getInt(byteOffset);
        byteOffset += 4;
      }
      for (int i = 0; i < length; i++) {
        out[outpos + i] = dictIds[docIds[docIdsStartIndex + i]];
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
        _dataBuffer.putByte(++byteOffset, (byte) (value >> numBitsLeft));
      }
      int lastByte = _dataBuffer.getByte(++byteOffset);
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
        firstByte = _dataBuffer.getByte(++byteOffset);
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