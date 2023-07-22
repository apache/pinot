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
package org.apache.pinot.segment.local.io.reader.impl;

import java.util.List;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexByteRange;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/**
 * Int reader for bit-compressed data.
 */
public abstract class FixedBitIntReader {
  final PinotDataBuffer _dataBuffer;

  private FixedBitIntReader(PinotDataBuffer dataBuffer) {
    _dataBuffer = dataBuffer;
  }

  /**
   * Returns the value at the given index.
   */
  public abstract int read(int index);

  public abstract int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges);

  /**
   * Returns the value at the given index. This method does not check the boundary of the data buffer, and assume there
   * are enough bytes left in the data buffer. Use this method when the index is not pointing to the last two values in
   * the data buffer.
   */
  public abstract int readUnchecked(int index);

  /**
   * Reads 32 values starting from the given index. The index must be multiple of 32, and all 32 values must be included
   * in the data buffer, i.e. {@code index + 32 <= numValues}.
   */
  public abstract void read32(int index, int[] out, int outPos);

  public static FixedBitIntReader getReader(PinotDataBuffer dataBuffer, int numBitsPerValue) {
    switch (numBitsPerValue) {
      case 1:
        return new Bit1Reader(dataBuffer);
      case 2:
        return new Bit2Reader(dataBuffer);
      case 3:
        return new Bit3Reader(dataBuffer);
      case 4:
        return new Bit4Reader(dataBuffer);
      case 5:
        return new Bit5Reader(dataBuffer);
      case 6:
        return new Bit6Reader(dataBuffer);
      case 7:
        return new Bit7Reader(dataBuffer);
      case 8:
        return new Bit8Reader(dataBuffer);
      case 9:
        return new Bit9Reader(dataBuffer);
      case 10:
        return new Bit10Reader(dataBuffer);
      case 11:
        return new Bit11Reader(dataBuffer);
      case 12:
        return new Bit12Reader(dataBuffer);
      case 13:
        return new Bit13Reader(dataBuffer);
      case 14:
        return new Bit14Reader(dataBuffer);
      case 15:
        return new Bit15Reader(dataBuffer);
      case 16:
        return new Bit16Reader(dataBuffer);
      case 17:
        return new Bit17Reader(dataBuffer);
      case 18:
        return new Bit18Reader(dataBuffer);
      case 19:
        return new Bit19Reader(dataBuffer);
      case 20:
        return new Bit20Reader(dataBuffer);
      case 21:
        return new Bit21Reader(dataBuffer);
      case 22:
        return new Bit22Reader(dataBuffer);
      case 23:
        return new Bit23Reader(dataBuffer);
      case 24:
        return new Bit24Reader(dataBuffer);
      case 25:
        return new Bit25Reader(dataBuffer);
      case 26:
        return new Bit26Reader(dataBuffer);
      case 27:
        return new Bit27Reader(dataBuffer);
      case 28:
        return new Bit28Reader(dataBuffer);
      case 29:
        return new Bit29Reader(dataBuffer);
      case 30:
        return new Bit30Reader(dataBuffer);
      case 31:
        return new Bit31Reader(dataBuffer);
      default:
        throw new IllegalStateException();
    }
  }

  private static class Bit1Reader extends FixedBitIntReader {

    private Bit1Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      int offset = index >>> 3;
      int bitOffsetInByte = index & 0x7;
      return (_dataBuffer.getByte(offset) >>> (7 - bitOffsetInByte)) & 0x1;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      int offset = index >>> 3;
      int bitOffsetInByte = index & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Byte.BYTES));
      return (_dataBuffer.getByte(offset) >>> (7 - bitOffsetInByte)) & 0x1;
    }

    @Override
    public int readUnchecked(int index) {
      int offset = index >>> 3;
      int bitOffsetInByte = index & 0x7;
      return (_dataBuffer.getByte(offset) >>> (7 - bitOffsetInByte)) & 0x1;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      int offset = index >>> 3;
      int i0 = _dataBuffer.getInt(offset);
      out[outPos] = i0 >>> 31;
      out[outPos + 1] = (i0 >>> 30) & 0x1;
      out[outPos + 2] = (i0 >>> 29) & 0x1;
      out[outPos + 3] = (i0 >>> 28) & 0x1;
      out[outPos + 4] = (i0 >>> 27) & 0x1;
      out[outPos + 5] = (i0 >>> 26) & 0x1;
      out[outPos + 6] = (i0 >>> 25) & 0x1;
      out[outPos + 7] = (i0 >>> 24) & 0x1;
      out[outPos + 8] = (i0 >>> 23) & 0x1;
      out[outPos + 9] = (i0 >>> 22) & 0x1;
      out[outPos + 10] = (i0 >>> 21) & 0x1;
      out[outPos + 11] = (i0 >>> 20) & 0x1;
      out[outPos + 12] = (i0 >>> 19) & 0x1;
      out[outPos + 13] = (i0 >>> 18) & 0x1;
      out[outPos + 14] = (i0 >>> 17) & 0x1;
      out[outPos + 15] = (i0 >>> 16) & 0x1;
      out[outPos + 16] = (i0 >>> 15) & 0x1;
      out[outPos + 17] = (i0 >>> 14) & 0x1;
      out[outPos + 18] = (i0 >>> 13) & 0x1;
      out[outPos + 19] = (i0 >>> 12) & 0x1;
      out[outPos + 20] = (i0 >>> 11) & 0x1;
      out[outPos + 21] = (i0 >>> 10) & 0x1;
      out[outPos + 22] = (i0 >>> 9) & 0x1;
      out[outPos + 23] = (i0 >>> 8) & 0x1;
      out[outPos + 24] = (i0 >>> 7) & 0x1;
      out[outPos + 25] = (i0 >>> 6) & 0x1;
      out[outPos + 26] = (i0 >>> 5) & 0x1;
      out[outPos + 27] = (i0 >>> 4) & 0x1;
      out[outPos + 28] = (i0 >>> 3) & 0x1;
      out[outPos + 29] = (i0 >>> 2) & 0x1;
      out[outPos + 30] = (i0 >>> 1) & 0x1;
      out[outPos + 31] = i0 & 0x1;
    }
  }

  private static class Bit2Reader extends FixedBitIntReader {

    private Bit2Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      int offset = index >>> 2;
      int bitOffsetInByte = (index << 1) & 0x7;
      return (_dataBuffer.getByte(offset) >>> (6 - bitOffsetInByte)) & 0x3;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      int offset = index >>> 2;
      int bitOffsetInByte = (index << 1) & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Byte.BYTES));
      return (_dataBuffer.getByte(offset) >>> (7 - bitOffsetInByte)) & 0x1;
    }

    @Override
    public int readUnchecked(int index) {
      int offset = index >>> 2;
      int bitOffsetInByte = (index << 1) & 0x7;
      return (_dataBuffer.getByte(offset) >>> (6 - bitOffsetInByte)) & 0x3;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      int offset = index >>> 2;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      out[outPos] = i0 >>> 30;
      out[outPos + 1] = (i0 >>> 28) & 0x3;
      out[outPos + 2] = (i0 >>> 26) & 0x3;
      out[outPos + 3] = (i0 >>> 24) & 0x3;
      out[outPos + 4] = (i0 >>> 22) & 0x3;
      out[outPos + 5] = (i0 >>> 20) & 0x3;
      out[outPos + 6] = (i0 >>> 18) & 0x3;
      out[outPos + 7] = (i0 >>> 16) & 0x3;
      out[outPos + 8] = (i0 >>> 14) & 0x3;
      out[outPos + 9] = (i0 >>> 12) & 0x3;
      out[outPos + 10] = (i0 >>> 10) & 0x3;
      out[outPos + 11] = (i0 >>> 8) & 0x3;
      out[outPos + 12] = (i0 >>> 6) & 0x3;
      out[outPos + 13] = (i0 >>> 4) & 0x3;
      out[outPos + 14] = (i0 >>> 2) & 0x3;
      out[outPos + 15] = i0 & 0x3;
      out[outPos + 16] = i1 >>> 30;
      out[outPos + 17] = (i1 >>> 28) & 0x3;
      out[outPos + 18] = (i1 >>> 26) & 0x3;
      out[outPos + 19] = (i1 >>> 24) & 0x3;
      out[outPos + 20] = (i1 >>> 22) & 0x3;
      out[outPos + 21] = (i1 >>> 20) & 0x3;
      out[outPos + 22] = (i1 >>> 18) & 0x3;
      out[outPos + 23] = (i1 >>> 16) & 0x3;
      out[outPos + 24] = (i1 >>> 14) & 0x3;
      out[outPos + 25] = (i1 >>> 12) & 0x3;
      out[outPos + 26] = (i1 >>> 10) & 0x3;
      out[outPos + 27] = (i1 >>> 8) & 0x3;
      out[outPos + 28] = (i1 >>> 6) & 0x3;
      out[outPos + 29] = (i1 >>> 4) & 0x3;
      out[outPos + 30] = (i1 >>> 2) & 0x3;
      out[outPos + 31] = i1 & 0x3;
    }
  }

  private static class Bit3Reader extends FixedBitIntReader {

    private Bit3Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 3;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Byte.BYTES));
      int valueInFirstByte = _dataBuffer.getByte(offset) & (0xff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 5;
      if (numBitsLeft <= 0) {
        return valueInFirstByte >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 1, Byte.BYTES));
        return (valueInFirstByte << numBitsLeft) | ((_dataBuffer.getByte(offset + 1) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 3;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstByte = _dataBuffer.getByte(offset) & (0xff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 5;
      if (numBitsLeft <= 0) {
        return valueInFirstByte >>> -numBitsLeft;
      } else {
        return (valueInFirstByte << numBitsLeft) | ((_dataBuffer.getByte(offset + 1) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 3;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (13 - bitOffsetInFirstByte)) & 0x7;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      int offset = (index >>> 3) * 3;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      out[outPos] = i0 >>> 29;
      out[outPos + 1] = (i0 >>> 26) & 0x7;
      out[outPos + 2] = (i0 >>> 23) & 0x7;
      out[outPos + 3] = (i0 >>> 20) & 0x7;
      out[outPos + 4] = (i0 >>> 17) & 0x7;
      out[outPos + 5] = (i0 >>> 14) & 0x7;
      out[outPos + 6] = (i0 >>> 11) & 0x7;
      out[outPos + 7] = (i0 >>> 8) & 0x7;
      out[outPos + 8] = (i0 >>> 5) & 0x7;
      out[outPos + 9] = (i0 >>> 2) & 0x7;
      out[outPos + 10] = ((i0 & 0x3) << 1) | (i1 >>> 31);
      out[outPos + 11] = (i1 >>> 28) & 0x7;
      out[outPos + 12] = (i1 >>> 25) & 0x7;
      out[outPos + 13] = (i1 >>> 22) & 0x7;
      out[outPos + 14] = (i1 >>> 19) & 0x7;
      out[outPos + 15] = (i1 >>> 16) & 0x7;
      out[outPos + 16] = (i1 >>> 13) & 0x7;
      out[outPos + 17] = (i1 >>> 10) & 0x7;
      out[outPos + 18] = (i1 >>> 7) & 0x7;
      out[outPos + 19] = (i1 >>> 4) & 0x7;
      out[outPos + 20] = (i1 >>> 1) & 0x7;
      out[outPos + 21] = ((i1 & 0x1) << 2) | (i2 >>> 30);
      out[outPos + 22] = (i2 >>> 27) & 0x7;
      out[outPos + 23] = (i2 >>> 24) & 0x7;
      out[outPos + 24] = (i2 >>> 21) & 0x7;
      out[outPos + 25] = (i2 >>> 18) & 0x7;
      out[outPos + 26] = (i2 >>> 15) & 0x7;
      out[outPos + 27] = (i2 >>> 12) & 0x7;
      out[outPos + 28] = (i2 >>> 9) & 0x7;
      out[outPos + 29] = (i2 >>> 6) & 0x7;
      out[outPos + 30] = (i2 >>> 3) & 0x7;
      out[outPos + 31] = i2 & 0x7;
    }
  }

  private static class Bit4Reader extends FixedBitIntReader {

    private Bit4Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      int offset = index >>> 1;
      int bitOffsetInByte = (index << 2) & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Byte.BYTES));
      return (_dataBuffer.getByte(offset) >>> (4 - bitOffsetInByte)) & 0xf;
    }

    @Override
    public int read(int index) {
      int offset = index >>> 1;
      int bitOffsetInByte = (index << 2) & 0x7;
      return (_dataBuffer.getByte(offset) >>> (4 - bitOffsetInByte)) & 0xf;
    }

    @Override
    public int readUnchecked(int index) {
      int offset = index >>> 1;
      int bitOffsetInByte = (index << 2) & 0x7;
      return (_dataBuffer.getByte(offset) >>> (4 - bitOffsetInByte)) & 0xf;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      int offset = index >>> 1;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      out[outPos] = i0 >>> 28;
      out[outPos + 1] = (i0 >>> 24) & 0xf;
      out[outPos + 2] = (i0 >>> 20) & 0xf;
      out[outPos + 3] = (i0 >>> 16) & 0xf;
      out[outPos + 4] = (i0 >>> 12) & 0xf;
      out[outPos + 5] = (i0 >>> 8) & 0xf;
      out[outPos + 6] = (i0 >>> 4) & 0xf;
      out[outPos + 7] = i0 & 0xf;
      out[outPos + 8] = i1 >>> 28;
      out[outPos + 9] = (i1 >>> 24) & 0xf;
      out[outPos + 10] = (i1 >>> 20) & 0xf;
      out[outPos + 11] = (i1 >>> 16) & 0xf;
      out[outPos + 12] = (i1 >>> 12) & 0xf;
      out[outPos + 13] = (i1 >>> 8) & 0xf;
      out[outPos + 14] = (i1 >>> 4) & 0xf;
      out[outPos + 15] = i1 & 0xf;
      out[outPos + 16] = i2 >>> 28;
      out[outPos + 17] = (i2 >>> 24) & 0xf;
      out[outPos + 18] = (i2 >>> 20) & 0xf;
      out[outPos + 19] = (i2 >>> 16) & 0xf;
      out[outPos + 20] = (i2 >>> 12) & 0xf;
      out[outPos + 21] = (i2 >>> 8) & 0xf;
      out[outPos + 22] = (i2 >>> 4) & 0xf;
      out[outPos + 23] = i2 & 0xf;
      out[outPos + 24] = i3 >>> 28;
      out[outPos + 25] = (i3 >>> 24) & 0xf;
      out[outPos + 26] = (i3 >>> 20) & 0xf;
      out[outPos + 27] = (i3 >>> 16) & 0xf;
      out[outPos + 28] = (i3 >>> 12) & 0xf;
      out[outPos + 29] = (i3 >>> 8) & 0xf;
      out[outPos + 30] = (i3 >>> 4) & 0xf;
      out[outPos + 31] = i3 & 0xf;
    }
  }

  private static class Bit5Reader extends FixedBitIntReader {

    private Bit5Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 5;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstByte = _dataBuffer.getByte(offset) & (0xff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 3;
      if (numBitsLeft <= 0) {
        return valueInFirstByte >>> -numBitsLeft;
      } else {
        return (valueInFirstByte << numBitsLeft) | ((_dataBuffer.getByte(offset + 1) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 5;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Byte.BYTES));
      int valueInFirstByte = _dataBuffer.getByte(offset) & (0xff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 3;
      if (numBitsLeft <= 0) {
        return valueInFirstByte >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 1, Byte.BYTES));
        return (valueInFirstByte << numBitsLeft) | ((_dataBuffer.getByte(offset + 1) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 5;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (11 - bitOffsetInFirstByte)) & 0x1f;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      int offset = (index >>> 3) * 5;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      out[outPos] = i0 >>> 27;
      out[outPos + 1] = (i0 >>> 22) & 0x1f;
      out[outPos + 2] = (i0 >>> 17) & 0x1f;
      out[outPos + 3] = (i0 >>> 12) & 0x1f;
      out[outPos + 4] = (i0 >>> 7) & 0x1f;
      out[outPos + 5] = (i0 >>> 2) & 0x1f;
      out[outPos + 6] = ((i0 & 0x3) << 3) | (i1 >>> 29);
      out[outPos + 7] = (i1 >>> 24) & 0x1f;
      out[outPos + 8] = (i1 >>> 19) & 0x1f;
      out[outPos + 9] = (i1 >>> 14) & 0x1f;
      out[outPos + 10] = (i1 >>> 9) & 0x1f;
      out[outPos + 11] = (i1 >>> 4) & 0x1f;
      out[outPos + 12] = ((i1 & 0xf) << 1) | (i2 >>> 31);
      out[outPos + 13] = (i2 >>> 26) & 0x1f;
      out[outPos + 14] = (i2 >>> 21) & 0x1f;
      out[outPos + 15] = (i2 >>> 16) & 0x1f;
      out[outPos + 16] = (i2 >>> 11) & 0x1f;
      out[outPos + 17] = (i2 >>> 6) & 0x1f;
      out[outPos + 18] = (i2 >>> 1) & 0x1f;
      out[outPos + 19] = ((i2 & 0x1) << 4) | (i3 >>> 28);
      out[outPos + 20] = (i3 >>> 23) & 0x1f;
      out[outPos + 21] = (i3 >>> 18) & 0x1f;
      out[outPos + 22] = (i3 >>> 13) & 0x1f;
      out[outPos + 23] = (i3 >>> 8) & 0x1f;
      out[outPos + 24] = (i3 >>> 3) & 0x1f;
      out[outPos + 25] = ((i3 & 0x7) << 2) | (i4 >>> 30);
      out[outPos + 26] = (i4 >>> 25) & 0x1f;
      out[outPos + 27] = (i4 >>> 20) & 0x1f;
      out[outPos + 28] = (i4 >>> 15) & 0x1f;
      out[outPos + 29] = (i4 >>> 10) & 0x1f;
      out[outPos + 30] = (i4 >>> 5) & 0x1f;
      out[outPos + 31] = i4 & 0x1f;
    }
  }

  private static class Bit6Reader extends FixedBitIntReader {

    private Bit6Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 6;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstByte = _dataBuffer.getByte(offset) & (0xff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 2;
      if (numBitsLeft <= 0) {
        return valueInFirstByte >>> -numBitsLeft;
      } else {
        return (valueInFirstByte << numBitsLeft) | ((_dataBuffer.getByte(offset + 1) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 6;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Byte.BYTES));
      int valueInFirstByte = _dataBuffer.getByte(offset) & (0xff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 2;
      if (numBitsLeft <= 0) {
        return valueInFirstByte >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 1, Byte.BYTES));
        return (valueInFirstByte << numBitsLeft) | ((_dataBuffer.getByte(offset + 1) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 6;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (10 - bitOffsetInFirstByte)) & 0x3f;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      int offset = (index >>> 3) * 6;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      out[outPos] = (i0 >>> 26) & 0x3f;
      out[outPos + 1] = (i0 >>> 20) & 0x3f;
      out[outPos + 2] = (i0 >>> 14) & 0x3f;
      out[outPos + 3] = (i0 >>> 8) & 0x3f;
      out[outPos + 4] = (i0 >>> 2) & 0x3f;
      out[outPos + 5] = ((i0 & 0x3) << 4) | (i1 >>> 28);
      out[outPos + 6] = (i1 >>> 22) & 0x3f;
      out[outPos + 7] = (i1 >>> 16) & 0x3f;
      out[outPos + 8] = (i1 >>> 10) & 0x3f;
      out[outPos + 9] = (i1 >>> 4) & 0x3f;
      out[outPos + 10] = ((i1 & 0xf) << 2) | (i2 >>> 30);
      out[outPos + 11] = (i2 >>> 24) & 0x3f;
      out[outPos + 12] = (i2 >>> 18) & 0x3f;
      out[outPos + 13] = (i2 >>> 12) & 0x3f;
      out[outPos + 14] = (i2 >>> 6) & 0x3f;
      out[outPos + 15] = i2 & 0x3f;
      out[outPos + 16] = (i3 >>> 26) & 0x3f;
      out[outPos + 17] = (i3 >>> 20) & 0x3f;
      out[outPos + 18] = (i3 >>> 14) & 0x3f;
      out[outPos + 19] = (i3 >>> 8) & 0x3f;
      out[outPos + 20] = (i3 >>> 2) & 0x3f;
      out[outPos + 21] = ((i3 & 0x3) << 4) | (i4 >>> 28);
      out[outPos + 22] = (i4 >>> 22) & 0x3f;
      out[outPos + 23] = (i4 >>> 16) & 0x3f;
      out[outPos + 24] = (i4 >>> 10) & 0x3f;
      out[outPos + 25] = (i4 >>> 4) & 0x3f;
      out[outPos + 26] = ((i4 & 0xf) << 2) | (i5 >>> 30);
      out[outPos + 27] = (i5 >>> 24) & 0x3f;
      out[outPos + 28] = (i5 >>> 18) & 0x3f;
      out[outPos + 29] = (i5 >>> 12) & 0x3f;
      out[outPos + 30] = (i5 >>> 6) & 0x3f;
      out[outPos + 31] = i5 & 0x3f;
    }
  }

  private static class Bit7Reader extends FixedBitIntReader {

    private Bit7Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 7;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstByte = _dataBuffer.getByte(offset) & (0xff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 1;
      if (numBitsLeft <= 0) {
        return valueInFirstByte >>> -numBitsLeft;
      } else {
        return (valueInFirstByte << numBitsLeft) | ((_dataBuffer.getByte(offset + 1) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 7;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Byte.BYTES));
      int valueInFirstByte = _dataBuffer.getByte(offset) & (0xff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 1;
      if (numBitsLeft <= 0) {
        return valueInFirstByte >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 1, Byte.BYTES));
        return (valueInFirstByte << numBitsLeft) | ((_dataBuffer.getByte(offset + 1) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 7;
      int offset = (int) (bitOffset >>> 3);
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (9 - bitOffsetInFirstByte)) & 0x7f;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      int offset = (index >>> 3) * 7;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      out[outPos] = (i0 >>> 25) & 0x7f;
      out[outPos + 1] = (i0 >>> 18) & 0x7f;
      out[outPos + 2] = (i0 >>> 11) & 0x7f;
      out[outPos + 3] = (i0 >>> 4) & 0x7f;
      out[outPos + 4] = ((i0 & 0xf) << 3) | (i1 >>> 29);
      out[outPos + 5] = (i1 >>> 22) & 0x7f;
      out[outPos + 6] = (i1 >>> 15) & 0x7f;
      out[outPos + 7] = (i1 >>> 8) & 0x7f;
      out[outPos + 8] = (i1 >>> 1) & 0x7f;
      out[outPos + 9] = ((i1 & 0x1) << 6) | (i2 >>> 26);
      out[outPos + 10] = (i2 >>> 19) & 0x7f;
      out[outPos + 11] = (i2 >>> 12) & 0x7f;
      out[outPos + 12] = (i2 >>> 5) & 0x7f;
      out[outPos + 13] = ((i2 & 0x1f) << 2) | (i3 >>> 30);
      out[outPos + 14] = (i3 >>> 23) & 0x7f;
      out[outPos + 15] = (i3 >>> 16) & 0x7f;
      out[outPos + 16] = (i3 >>> 9) & 0x7f;
      out[outPos + 17] = (i3 >>> 2) & 0x7f;
      out[outPos + 18] = ((i3 & 0x3) << 5) | (i4 >>> 27);
      out[outPos + 19] = (i4 >>> 20) & 0x7f;
      out[outPos + 20] = (i4 >>> 13) & 0x7f;
      out[outPos + 21] = (i4 >>> 6) & 0x7f;
      out[outPos + 22] = ((i4 & 0x3f) << 1) | (i5 >>> 31);
      out[outPos + 23] = (i5 >>> 24) & 0x7f;
      out[outPos + 24] = (i5 >>> 17) & 0x7f;
      out[outPos + 25] = (i5 >>> 10) & 0x7f;
      out[outPos + 26] = (i5 >>> 3) & 0x7f;
      out[outPos + 27] = ((i5 & 0x7) << 4) | (i6 >>> 28);
      out[outPos + 28] = (i6 >>> 21) & 0x7f;
      out[outPos + 29] = (i6 >>> 14) & 0x7f;
      out[outPos + 30] = (i6 >>> 7) & 0x7f;
      out[outPos + 31] = i6 & 0x7f;
    }
  }

  private static class Bit8Reader extends FixedBitIntReader {

    private Bit8Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      return _dataBuffer.getByte(index) & 0xff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + index, Byte.BYTES));
      return _dataBuffer.getByte(index) & 0xff;
    }

    @Override
    public int readUnchecked(int index) {
      return _dataBuffer.getByte(index) & 0xff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      int i0 = _dataBuffer.getInt(index);
      int i1 = _dataBuffer.getInt(index + 4);
      int i2 = _dataBuffer.getInt(index + 8);
      int i3 = _dataBuffer.getInt(index + 12);
      int i4 = _dataBuffer.getInt(index + 16);
      int i5 = _dataBuffer.getInt(index + 20);
      int i6 = _dataBuffer.getInt(index + 24);
      int i7 = _dataBuffer.getInt(index + 28);
      out[outPos] = i0 >>> 24;
      out[outPos + 1] = (i0 >>> 16) & 0xff;
      out[outPos + 2] = (i0 >>> 8) & 0xff;
      out[outPos + 3] = i0 & 0xff;
      out[outPos + 4] = i1 >>> 24;
      out[outPos + 5] = (i1 >>> 16) & 0xff;
      out[outPos + 6] = (i1 >>> 8) & 0xff;
      out[outPos + 7] = i1 & 0xff;
      out[outPos + 8] = i2 >>> 24;
      out[outPos + 9] = (i2 >>> 16) & 0xff;
      out[outPos + 10] = (i2 >>> 8) & 0xff;
      out[outPos + 11] = i2 & 0xff;
      out[outPos + 12] = i3 >>> 24;
      out[outPos + 13] = (i3 >>> 16) & 0xff;
      out[outPos + 14] = (i3 >>> 8) & 0xff;
      out[outPos + 15] = i3 & 0xff;
      out[outPos + 16] = i4 >>> 24;
      out[outPos + 17] = (i4 >>> 16) & 0xff;
      out[outPos + 18] = (i4 >>> 8) & 0xff;
      out[outPos + 19] = i4 & 0xff;
      out[outPos + 20] = i5 >>> 24;
      out[outPos + 21] = (i5 >>> 16) & 0xff;
      out[outPos + 22] = (i5 >>> 8) & 0xff;
      out[outPos + 23] = i5 & 0xff;
      out[outPos + 24] = i6 >>> 24;
      out[outPos + 25] = (i6 >>> 16) & 0xff;
      out[outPos + 26] = (i6 >>> 8) & 0xff;
      out[outPos + 27] = i6 & 0xff;
      out[outPos + 28] = i7 >>> 24;
      out[outPos + 29] = (i7 >>> 16) & 0xff;
      out[outPos + 30] = (i7 >>> 8) & 0xff;
      out[outPos + 31] = i7 & 0xff;
    }
  }

  private static class Bit9Reader extends FixedBitIntReader {

    private Bit9Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 9;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (7 - bitOffsetInFirstByte)) & 0x1ff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 9;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES));
      return (_dataBuffer.getShort(offset) >>> (7 - bitOffsetInFirstByte)) & 0x1ff;
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 9;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (7 - bitOffsetInFirstByte)) & 0x1ff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 9;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      out[outPos] = i0 >>> 23;
      out[outPos + 1] = (i0 >>> 14) & 0x1ff;
      out[outPos + 2] = (i0 >>> 5) & 0x1ff;
      out[outPos + 3] = ((i0 & 0x1f) << 4) | (i1 >>> 28);
      out[outPos + 4] = (i1 >>> 19) & 0x1ff;
      out[outPos + 5] = (i1 >>> 10) & 0x1ff;
      out[outPos + 6] = (i1 >>> 1) & 0x1ff;
      out[outPos + 7] = ((i1 & 0x1) << 8) | (i2 >>> 24);
      out[outPos + 8] = (i2 >>> 15) & 0x1ff;
      out[outPos + 9] = (i2 >>> 6) & 0x1ff;
      out[outPos + 10] = ((i2 & 0x3f) << 3) | (i3 >>> 29);
      out[outPos + 11] = (i3 >>> 20) & 0x1ff;
      out[outPos + 12] = (i3 >>> 11) & 0x1ff;
      out[outPos + 13] = (i3 >>> 2) & 0x1ff;
      out[outPos + 14] = ((i3 & 0x3) << 7) | (i4 >>> 25);
      out[outPos + 15] = (i4 >>> 16) & 0x1ff;
      out[outPos + 16] = (i4 >>> 7) & 0x1ff;
      out[outPos + 17] = ((i4 & 0x7f) << 2) | (i5 >>> 30);
      out[outPos + 18] = (i5 >>> 21) & 0x1ff;
      out[outPos + 19] = (i5 >>> 12) & 0x1ff;
      out[outPos + 20] = (i5 >>> 3) & 0x1ff;
      out[outPos + 21] = ((i5 & 0x7) << 6) | (i6 >>> 26);
      out[outPos + 22] = (i6 >>> 17) & 0x1ff;
      out[outPos + 23] = (i6 >>> 8) & 0x1ff;
      out[outPos + 24] = ((i6 & 0xff) << 1) | (i7 >>> 31);
      out[outPos + 25] = (i7 >>> 22) & 0x1ff;
      out[outPos + 26] = (i7 >>> 13) & 0x1ff;
      out[outPos + 27] = (i7 >>> 4) & 0x1ff;
      out[outPos + 28] = ((i7 & 0xf) << 5) | (i8 >>> 27);
      out[outPos + 29] = (i8 >>> 18) & 0x1ff;
      out[outPos + 30] = (i8 >>> 9) & 0x1ff;
      out[outPos + 31] = i8 & 0x1ff;
    }
  }

  private static class Bit10Reader extends FixedBitIntReader {

    private Bit10Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 10;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (6 - bitOffsetInFirstByte)) & 0x3ff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 10;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES));
      return (_dataBuffer.getShort(offset) >>> (6 - bitOffsetInFirstByte)) & 0x3ff;
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 10;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (6 - bitOffsetInFirstByte)) & 0x3ff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 10;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      out[outPos] = i0 >>> 22;
      out[outPos + 1] = (i0 >>> 12) & 0x3ff;
      out[outPos + 2] = (i0 >>> 2) & 0x3ff;
      out[outPos + 3] = ((i0 & 0x3) << 8) | (i1 >>> 24);
      out[outPos + 4] = (i1 >>> 14) & 0x3ff;
      out[outPos + 5] = (i1 >>> 4) & 0x3ff;
      out[outPos + 6] = ((i1 & 0xf) << 6) | (i2 >>> 26);
      out[outPos + 7] = (i2 >>> 16) & 0x3ff;
      out[outPos + 8] = (i2 >>> 6) & 0x3ff;
      out[outPos + 9] = ((i2 & 0x3f) << 4) | (i3 >>> 28);
      out[outPos + 10] = (i3 >>> 18) & 0x3ff;
      out[outPos + 11] = (i3 >>> 8) & 0x3ff;
      out[outPos + 12] = ((i3 & 0xff) << 2) | (i4 >>> 30);
      out[outPos + 13] = (i4 >>> 20) & 0x3ff;
      out[outPos + 14] = (i4 >>> 10) & 0x3ff;
      out[outPos + 15] = i4 & 0x3ff;
      out[outPos + 16] = i5 >>> 22;
      out[outPos + 17] = (i5 >>> 12) & 0x3ff;
      out[outPos + 18] = (i5 >>> 2) & 0x3ff;
      out[outPos + 19] = ((i5 & 0x3) << 8) | (i6 >>> 24);
      out[outPos + 20] = (i6 >>> 14) & 0x3ff;
      out[outPos + 21] = (i6 >>> 4) & 0x3ff;
      out[outPos + 22] = ((i6 & 0xf) << 6) | (i7 >>> 26);
      out[outPos + 23] = (i7 >>> 16) & 0x3ff;
      out[outPos + 24] = (i7 >>> 6) & 0x3ff;
      out[outPos + 25] = ((i7 & 0x3f) << 4) | (i8 >>> 28);
      out[outPos + 26] = (i8 >>> 18) & 0x3ff;
      out[outPos + 27] = (i8 >>> 8) & 0x3ff;
      out[outPos + 28] = ((i8 & 0xff) << 2) | (i9 >>> 30);
      out[outPos + 29] = (i9 >>> 20) & 0x3ff;
      out[outPos + 30] = (i9 >>> 10) & 0x3ff;
      out[outPos + 31] = i9 & 0x3ff;
    }
  }

  private static class Bit11Reader extends FixedBitIntReader {

    private Bit11Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 11;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstShort = _dataBuffer.getShort(offset) & (0xffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 5;
      if (numBitsLeft <= 0) {
        return valueInFirstShort >>> -numBitsLeft;
      } else {
        return (valueInFirstShort << numBitsLeft) | ((_dataBuffer.getByte(offset + 2) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 11;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES));
      int valueInFirstShort = _dataBuffer.getShort(offset) & (0xffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 5;
      if (numBitsLeft <= 0) {
        return valueInFirstShort >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 2, Short.BYTES));
        return (valueInFirstShort << numBitsLeft) | ((_dataBuffer.getByte(offset + 2) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 11;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (21 - bitOffsetInFirstByte)) & 0x7ff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 11;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      out[outPos] = i0 >>> 21;
      out[outPos + 1] = (i0 >>> 10) & 0x7ff;
      out[outPos + 2] = ((i0 & 0x3ff) << 1) | (i1 >>> 31);
      out[outPos + 3] = (i1 >>> 20) & 0x7ff;
      out[outPos + 4] = (i1 >>> 9) & 0x7ff;
      out[outPos + 5] = ((i1 & 0x1ff) << 2) | (i2 >>> 30);
      out[outPos + 6] = (i2 >>> 19) & 0x7ff;
      out[outPos + 7] = (i2 >>> 8) & 0x7ff;
      out[outPos + 8] = ((i2 & 0xff) << 3) | (i3 >>> 29);
      out[outPos + 9] = (i3 >>> 18) & 0x7ff;
      out[outPos + 10] = (i3 >>> 7) & 0x7ff;
      out[outPos + 11] = ((i3 & 0x7f) << 4) | (i4 >>> 28);
      out[outPos + 12] = (i4 >>> 17) & 0x7ff;
      out[outPos + 13] = (i4 >>> 6) & 0x7ff;
      out[outPos + 14] = ((i4 & 0x3f) << 5) | (i5 >>> 27);
      out[outPos + 15] = (i5 >>> 16) & 0x7ff;
      out[outPos + 16] = (i5 >>> 5) & 0x7ff;
      out[outPos + 17] = ((i5 & 0x1f) << 6) | (i6 >>> 26);
      out[outPos + 18] = (i6 >>> 15) & 0x7ff;
      out[outPos + 19] = (i6 >>> 4) & 0x7ff;
      out[outPos + 20] = ((i6 & 0xf) << 7) | (i7 >>> 25);
      out[outPos + 21] = (i7 >>> 14) & 0x7ff;
      out[outPos + 22] = (i7 >>> 3) & 0x7ff;
      out[outPos + 23] = ((i7 & 0x7) << 8) | (i8 >>> 24);
      out[outPos + 24] = (i8 >>> 13) & 0x7ff;
      out[outPos + 25] = (i8 >>> 2) & 0x7ff;
      out[outPos + 26] = ((i8 & 0x3) << 9) | (i9 >>> 23);
      out[outPos + 27] = (i9 >>> 12) & 0x7ff;
      out[outPos + 28] = (i9 >>> 1) & 0x7ff;
      out[outPos + 29] = ((i9 & 0x1) << 10) | (i10 >>> 22);
      out[outPos + 30] = (i10 >>> 11) & 0x7ff;
      out[outPos + 31] = i10 & 0x7ff;
    }
  }

  private static class Bit12Reader extends FixedBitIntReader {

    private Bit12Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 12;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (4 - bitOffsetInFirstByte)) & 0xfff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 12;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES));
      return (_dataBuffer.getShort(offset) >>> (4 - bitOffsetInFirstByte)) & 0xfff;
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 12;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getShort(offset) >>> (4 - bitOffsetInFirstByte)) & 0xfff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 12;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      out[outPos] = i0 >>> 20;
      out[outPos + 1] = (i0 >>> 8) & 0xfff;
      out[outPos + 2] = ((i0 & 0xff) << 4) | (i1 >>> 28);
      out[outPos + 3] = (i1 >>> 16) & 0xfff;
      out[outPos + 4] = (i1 >>> 4) & 0xfff;
      out[outPos + 5] = ((i1 & 0xf) << 8) | (i2 >>> 24);
      out[outPos + 6] = (i2 >>> 12) & 0xfff;
      out[outPos + 7] = i2 & 0xfff;
      out[outPos + 8] = i3 >>> 20;
      out[outPos + 9] = (i3 >>> 8) & 0xfff;
      out[outPos + 10] = ((i3 & 0xff) << 4) | (i4 >>> 28);
      out[outPos + 11] = (i4 >>> 16) & 0xfff;
      out[outPos + 12] = (i4 >>> 4) & 0xfff;
      out[outPos + 13] = ((i4 & 0xf) << 8) | (i5 >>> 24);
      out[outPos + 14] = (i5 >>> 12) & 0xfff;
      out[outPos + 15] = i5 & 0xfff;
      out[outPos + 16] = i6 >>> 20;
      out[outPos + 17] = (i6 >>> 8) & 0xfff;
      out[outPos + 18] = ((i6 & 0xff) << 4) | (i7 >>> 28);
      out[outPos + 19] = (i7 >>> 16) & 0xfff;
      out[outPos + 20] = (i7 >>> 4) & 0xfff;
      out[outPos + 21] = ((i7 & 0xf) << 8) | (i8 >>> 24);
      out[outPos + 22] = (i8 >>> 12) & 0xfff;
      out[outPos + 23] = i8 & 0xfff;
      out[outPos + 24] = i9 >>> 20;
      out[outPos + 25] = (i9 >>> 8) & 0xfff;
      out[outPos + 26] = ((i9 & 0xff) << 4) | (i10 >>> 28);
      out[outPos + 27] = (i10 >>> 16) & 0xfff;
      out[outPos + 28] = (i10 >>> 4) & 0xfff;
      out[outPos + 29] = ((i10 & 0xf) << 8) | (i11 >>> 24);
      out[outPos + 30] = (i11 >>> 12) & 0xfff;
      out[outPos + 31] = i11 & 0xfff;
    }
  }

  private static class Bit13Reader extends FixedBitIntReader {

    private Bit13Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 13;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstShort = _dataBuffer.getShort(offset) & (0xffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 3;
      if (numBitsLeft <= 0) {
        return valueInFirstShort >>> -numBitsLeft;
      } else {
        return (valueInFirstShort << numBitsLeft) | ((_dataBuffer.getByte(offset + 2) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 13;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES));
      int valueInFirstShort = _dataBuffer.getShort(offset) & (0xffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 3;
      if (numBitsLeft <= 0) {
        return valueInFirstShort >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 2, Byte.BYTES));
        return (valueInFirstShort << numBitsLeft) | ((_dataBuffer.getByte(offset + 2) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 13;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (19 - bitOffsetInFirstByte)) & 0x1fff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 13;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      out[outPos] = i0 >>> 19;
      out[outPos + 1] = (i0 >>> 6) & 0x1fff;
      out[outPos + 2] = ((i0 & 0x3f) << 7) | (i1 >>> 25);
      out[outPos + 3] = (i1 >>> 12) & 0x1fff;
      out[outPos + 4] = ((i1 & 0xfff) << 1) | (i2 >>> 31);
      out[outPos + 5] = (i2 >>> 18) & 0x1fff;
      out[outPos + 6] = (i2 >>> 5) & 0x1fff;
      out[outPos + 7] = ((i2 & 0x1f) << 8) | (i3 >>> 24);
      out[outPos + 8] = (i3 >>> 11) & 0x1fff;
      out[outPos + 9] = ((i3 & 0x7ff) << 2) | (i4 >>> 30);
      out[outPos + 10] = (i4 >>> 17) & 0x1fff;
      out[outPos + 11] = (i4 >>> 4) & 0x1fff;
      out[outPos + 12] = ((i4 & 0xf) << 9) | (i5 >>> 23);
      out[outPos + 13] = (i5 >>> 10) & 0x1fff;
      out[outPos + 14] = ((i5 & 0x3ff) << 3) | (i6 >>> 29);
      out[outPos + 15] = (i6 >>> 16) & 0x1fff;
      out[outPos + 16] = (i6 >>> 3) & 0x1fff;
      out[outPos + 17] = ((i6 & 0x7) << 10) | (i7 >>> 22);
      out[outPos + 18] = (i7 >>> 9) & 0x1fff;
      out[outPos + 19] = ((i7 & 0x1ff) << 4) | (i8 >>> 28);
      out[outPos + 20] = (i8 >>> 15) & 0x1fff;
      out[outPos + 21] = (i8 >>> 2) & 0x1fff;
      out[outPos + 22] = ((i8 & 0x3) << 11) | (i9 >>> 21);
      out[outPos + 23] = (i9 >>> 8) & 0x1fff;
      out[outPos + 24] = ((i9 & 0xff) << 5) | (i10 >>> 27);
      out[outPos + 25] = (i10 >>> 14) & 0x1fff;
      out[outPos + 26] = (i10 >>> 1) & 0x1fff;
      out[outPos + 27] = ((i10 & 0x1) << 12) | (i11 >>> 20);
      out[outPos + 28] = (i11 >>> 7) & 0x1fff;
      out[outPos + 29] = ((i11 & 0x7f) << 6) | (i12 >>> 26);
      out[outPos + 30] = (i12 >>> 13) & 0x1fff;
      out[outPos + 31] = i12 & 0x1fff;
    }
  }

  private static class Bit14Reader extends FixedBitIntReader {

    private Bit14Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 14;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstShort = _dataBuffer.getShort(offset) & (0xffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 2;
      if (numBitsLeft <= 0) {
        return valueInFirstShort >>> -numBitsLeft;
      } else {
        return (valueInFirstShort << numBitsLeft) | ((_dataBuffer.getByte(offset + 2) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 14;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES));
      int valueInFirstShort = _dataBuffer.getShort(offset) & (0xffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 2;
      if (numBitsLeft <= 0) {
        return valueInFirstShort >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 2, Byte.BYTES));
        return (valueInFirstShort << numBitsLeft) | ((_dataBuffer.getByte(offset + 2) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 14;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (18 - bitOffsetInFirstByte)) & 0x3fff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 14;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      out[outPos] = i0 >>> 18;
      out[outPos + 1] = (i0 >>> 4) & 0x3fff;
      out[outPos + 2] = ((i0 & 0xf) << 10) | (i1 >>> 22);
      out[outPos + 3] = (i1 >>> 8) & 0x3fff;
      out[outPos + 4] = ((i1 & 0xff) << 6) | (i2 >>> 26);
      out[outPos + 5] = (i2 >>> 12) & 0x3fff;
      out[outPos + 6] = ((i2 & 0xfff) << 2) | (i3 >>> 30);
      out[outPos + 7] = (i3 >>> 16) & 0x3fff;
      out[outPos + 8] = (i3 >>> 2) & 0x3fff;
      out[outPos + 9] = ((i3 & 0x3) << 12) | (i4 >>> 20);
      out[outPos + 10] = (i4 >>> 6) & 0x3fff;
      out[outPos + 11] = ((i4 & 0x3f) << 8) | (i5 >>> 24);
      out[outPos + 12] = (i5 >>> 10) & 0x3fff;
      out[outPos + 13] = ((i5 & 0x3ff) << 4) | (i6 >>> 28);
      out[outPos + 14] = (i6 >>> 14) & 0x3fff;
      out[outPos + 15] = i6 & 0x3fff;
      out[outPos + 16] = i7 >>> 18;
      out[outPos + 17] = (i7 >>> 4) & 0x3fff;
      out[outPos + 18] = ((i7 & 0xf) << 10) | (i8 >>> 22);
      out[outPos + 19] = (i8 >>> 8) & 0x3fff;
      out[outPos + 20] = ((i8 & 0xff) << 6) | (i9 >>> 26);
      out[outPos + 21] = (i9 >>> 12) & 0x3fff;
      out[outPos + 22] = ((i9 & 0xfff) << 2) | (i10 >>> 30);
      out[outPos + 23] = (i10 >>> 16) & 0x3fff;
      out[outPos + 24] = (i10 >>> 2) & 0x3fff;
      out[outPos + 25] = ((i10 & 0x3) << 12) | (i11 >>> 20);
      out[outPos + 26] = (i11 >>> 6) & 0x3fff;
      out[outPos + 27] = ((i11 & 0x3f) << 8) | (i12 >>> 24);
      out[outPos + 28] = (i12 >>> 10) & 0x3fff;
      out[outPos + 29] = ((i12 & 0x3ff) << 4) | (i13 >>> 28);
      out[outPos + 30] = (i13 >>> 14) & 0x3fff;
      out[outPos + 31] = i13 & 0x3fff;
    }
  }

  private static class Bit15Reader extends FixedBitIntReader {

    private Bit15Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 0xf;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstShort = _dataBuffer.getShort(offset) & (0xffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 1;
      if (numBitsLeft <= 0) {
        return valueInFirstShort >>> -numBitsLeft;
      } else {
        return (valueInFirstShort << numBitsLeft) | ((_dataBuffer.getByte(offset + 2) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 0xf;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES));
      int valueInFirstShort = _dataBuffer.getShort(offset) & (0xffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 1;
      if (numBitsLeft <= 0) {
        return valueInFirstShort >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Byte.BYTES));
        return (valueInFirstShort << numBitsLeft) | ((_dataBuffer.getByte(offset + 2) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 15;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (17 - bitOffsetInFirstByte)) & 0x7fff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 15;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      out[outPos] = i0 >>> 17;
      out[outPos + 1] = (i0 >>> 2) & 0x7fff;
      out[outPos + 2] = ((i0 & 0x3) << 13) | (i1 >>> 19);
      out[outPos + 3] = (i1 >>> 4) & 0x7fff;
      out[outPos + 4] = ((i1 & 0xf) << 11) | (i2 >>> 21);
      out[outPos + 5] = (i2 >>> 6) & 0x7fff;
      out[outPos + 6] = ((i2 & 0x3f) << 9) | (i3 >>> 23);
      out[outPos + 7] = (i3 >>> 8) & 0x7fff;
      out[outPos + 8] = ((i3 & 0xff) << 7) | (i4 >>> 25);
      out[outPos + 9] = (i4 >>> 10) & 0x7fff;
      out[outPos + 10] = ((i4 & 0x3ff) << 5) | (i5 >>> 27);
      out[outPos + 11] = (i5 >>> 12) & 0x7fff;
      out[outPos + 12] = ((i5 & 0xfff) << 3) | (i6 >>> 29);
      out[outPos + 13] = (i6 >>> 14) & 0x7fff;
      out[outPos + 14] = ((i6 & 0x3fff) << 1) | (i7 >>> 31);
      out[outPos + 15] = (i7 >>> 16) & 0x7fff;
      out[outPos + 16] = (i7 >>> 1) & 0x7fff;
      out[outPos + 17] = ((i7 & 0x1) << 14) | (i8 >>> 18);
      out[outPos + 18] = (i8 >>> 3) & 0x7fff;
      out[outPos + 19] = ((i8 & 0x7) << 12) | (i9 >>> 20);
      out[outPos + 20] = (i9 >>> 5) & 0x7fff;
      out[outPos + 21] = ((i9 & 0x1f) << 10) | (i10 >>> 22);
      out[outPos + 22] = (i10 >>> 7) & 0x7fff;
      out[outPos + 23] = ((i10 & 0x7f) << 8) | (i11 >>> 24);
      out[outPos + 24] = (i11 >>> 9) & 0x7fff;
      out[outPos + 25] = ((i11 & 0x1ff) << 6) | (i12 >>> 26);
      out[outPos + 26] = (i12 >>> 11) & 0x7fff;
      out[outPos + 27] = ((i12 & 0x7ff) << 4) | (i13 >>> 28);
      out[outPos + 28] = (i13 >>> 13) & 0x7fff;
      out[outPos + 29] = ((i13 & 0x1fff) << 2) | (i14 >>> 30);
      out[outPos + 30] = (i14 >>> 15) & 0x7fff;
      out[outPos + 31] = i14 & 0x7fff;
    }
  }

  private static class Bit16Reader extends FixedBitIntReader {

    private Bit16Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      return _dataBuffer.getShort((long) index << 1) & 0xffff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + ((long) index << 1), Short.BYTES));
      return _dataBuffer.getShort((long) index << 1) & 0xffff;
    }

    @Override
    public int readUnchecked(int index) {
      return _dataBuffer.getShort((long) index << 1) & 0xffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) index << 1;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      out[outPos] = i0 >>> 16;
      out[outPos + 1] = i0 & 0xffff;
      out[outPos + 2] = i1 >>> 16;
      out[outPos + 3] = i1 & 0xffff;
      out[outPos + 4] = i2 >>> 16;
      out[outPos + 5] = i2 & 0xffff;
      out[outPos + 6] = i3 >>> 16;
      out[outPos + 7] = i3 & 0xffff;
      out[outPos + 8] = i4 >>> 16;
      out[outPos + 9] = i4 & 0xffff;
      out[outPos + 10] = i5 >>> 16;
      out[outPos + 11] = i5 & 0xffff;
      out[outPos + 12] = i6 >>> 16;
      out[outPos + 13] = i6 & 0xffff;
      out[outPos + 14] = i7 >>> 16;
      out[outPos + 15] = i7 & 0xffff;
      out[outPos + 16] = i8 >>> 16;
      out[outPos + 17] = i8 & 0xffff;
      out[outPos + 18] = i9 >>> 16;
      out[outPos + 19] = i9 & 0xffff;
      out[outPos + 20] = i10 >>> 16;
      out[outPos + 21] = i10 & 0xffff;
      out[outPos + 22] = i11 >>> 16;
      out[outPos + 23] = i11 & 0xffff;
      out[outPos + 24] = i12 >>> 16;
      out[outPos + 25] = i12 & 0xffff;
      out[outPos + 26] = i13 >>> 16;
      out[outPos + 27] = i13 & 0xffff;
      out[outPos + 28] = i14 >>> 16;
      out[outPos + 29] = i14 & 0xffff;
      out[outPos + 30] = i15 >>> 16;
      out[outPos + 31] = i15 & 0xffff;
    }
  }

  private static class Bit17Reader extends FixedBitIntReader {

    private Bit17Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 17;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) >>> (7
          - bitOffsetInFirstByte)) & 0x1ffff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 17;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES + Byte.BYTES));
      return (((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) >>> (7
          - bitOffsetInFirstByte)) & 0x1ffff;
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 17;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (15 - bitOffsetInFirstByte)) & 0x1ffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 17;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      out[outPos] = i0 >>> 15;
      out[outPos + 1] = ((i0 & 0x7fff) << 2) | (i1 >>> 30);
      out[outPos + 2] = (i1 >>> 13) & 0x1ffff;
      out[outPos + 3] = ((i1 & 0x1fff) << 4) | (i2 >>> 28);
      out[outPos + 4] = (i2 >>> 11) & 0x1ffff;
      out[outPos + 5] = ((i2 & 0x7ff) << 6) | (i3 >>> 26);
      out[outPos + 6] = (i3 >>> 9) & 0x1ffff;
      out[outPos + 7] = ((i3 & 0x1ff) << 8) | (i4 >>> 24);
      out[outPos + 8] = (i4 >>> 7) & 0x1ffff;
      out[outPos + 9] = ((i4 & 0x7f) << 10) | (i5 >>> 22);
      out[outPos + 10] = (i5 >>> 5) & 0x1ffff;
      out[outPos + 11] = ((i5 & 0x1f) << 12) | (i6 >>> 20);
      out[outPos + 12] = (i6 >>> 3) & 0x1ffff;
      out[outPos + 13] = ((i6 & 0x7) << 14) | (i7 >>> 18);
      out[outPos + 14] = (i7 >>> 1) & 0x1ffff;
      out[outPos + 15] = ((i7 & 0x1) << 16) | (i8 >>> 16);
      out[outPos + 16] = ((i8 & 0xffff) << 1) | (i9 >>> 31);
      out[outPos + 17] = (i9 >>> 14) & 0x1ffff;
      out[outPos + 18] = ((i9 & 0x3fff) << 3) | (i10 >>> 29);
      out[outPos + 19] = (i10 >>> 12) & 0x1ffff;
      out[outPos + 20] = ((i10 & 0xfff) << 5) | (i11 >>> 27);
      out[outPos + 21] = (i11 >>> 10) & 0x1ffff;
      out[outPos + 22] = ((i11 & 0x3ff) << 7) | (i12 >>> 25);
      out[outPos + 23] = (i12 >>> 8) & 0x1ffff;
      out[outPos + 24] = ((i12 & 0xff) << 9) | (i13 >>> 23);
      out[outPos + 25] = (i13 >>> 6) & 0x1ffff;
      out[outPos + 26] = ((i13 & 0x3f) << 11) | (i14 >>> 21);
      out[outPos + 27] = (i14 >>> 4) & 0x1ffff;
      out[outPos + 28] = ((i14 & 0xf) << 13) | (i15 >>> 19);
      out[outPos + 29] = (i15 >>> 2) & 0x1ffff;
      out[outPos + 30] = ((i15 & 0x3) << 0xf) | (i16 >>> 17);
      out[outPos + 31] = i16 & 0x1ffff;
    }
  }

  private static class Bit18Reader extends FixedBitIntReader {

    private Bit18Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 18;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) >>> (6
          - bitOffsetInFirstByte)) & 0x3ffff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 18;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES + Byte.BYTES));
      return (((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) >>> (6
          - bitOffsetInFirstByte)) & 0x3ffff;
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 18;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (14 - bitOffsetInFirstByte)) & 0x3ffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 18;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      out[outPos] = i0 >>> 14;
      out[outPos + 1] = ((i0 & 0x3fff) << 4) | (i1 >>> 28);
      out[outPos + 2] = (i1 >>> 10) & 0x3ffff;
      out[outPos + 3] = ((i1 & 0x3ff) << 8) | (i2 >>> 24);
      out[outPos + 4] = (i2 >>> 6) & 0x3ffff;
      out[outPos + 5] = ((i2 & 0x3f) << 12) | (i3 >>> 20);
      out[outPos + 6] = (i3 >>> 2) & 0x3ffff;
      out[outPos + 7] = ((i3 & 0x3) << 16) | (i4 >>> 16);
      out[outPos + 8] = ((i4 & 0xffff) << 2) | (i5 >>> 30);
      out[outPos + 9] = (i5 >>> 12) & 0x3ffff;
      out[outPos + 10] = ((i5 & 0xfff) << 6) | (i6 >>> 26);
      out[outPos + 11] = (i6 >>> 8) & 0x3ffff;
      out[outPos + 12] = ((i6 & 0xff) << 10) | (i7 >>> 22);
      out[outPos + 13] = (i7 >>> 4) & 0x3ffff;
      out[outPos + 14] = ((i7 & 0xf) << 14) | (i8 >>> 18);
      out[outPos + 15] = i8 & 0x3ffff;
      out[outPos + 16] = i9 >>> 14;
      out[outPos + 17] = ((i9 & 0x3fff) << 4) | (i10 >>> 28);
      out[outPos + 18] = (i10 >>> 10) & 0x3ffff;
      out[outPos + 19] = ((i10 & 0x3ff) << 8) | (i11 >>> 24);
      out[outPos + 20] = (i11 >>> 6) & 0x3ffff;
      out[outPos + 21] = ((i11 & 0x3f) << 12) | (i12 >>> 20);
      out[outPos + 22] = (i12 >>> 2) & 0x3ffff;
      out[outPos + 23] = ((i12 & 0x3) << 16) | (i13 >>> 16);
      out[outPos + 24] = ((i13 & 0xffff) << 2) | (i14 >>> 30);
      out[outPos + 25] = (i14 >>> 12) & 0x3ffff;
      out[outPos + 26] = ((i14 & 0xfff) << 6) | (i15 >>> 26);
      out[outPos + 27] = (i15 >>> 8) & 0x3ffff;
      out[outPos + 28] = ((i15 & 0xff) << 10) | (i16 >>> 22);
      out[outPos + 29] = (i16 >>> 4) & 0x3ffff;
      out[outPos + 30] = ((i16 & 0xf) << 14) | (i17 >>> 18);
      out[outPos + 31] = i17 & 0x3ffff;
    }
  }

  private static class Bit19Reader extends FixedBitIntReader {

    private Bit19Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 19;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirst3Bytes =
          ((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) & (0xffffff
              >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 5;
      if (numBitsLeft <= 0) {
        return valueInFirst3Bytes >>> -numBitsLeft;
      } else {
        return (valueInFirst3Bytes << numBitsLeft) | ((_dataBuffer.getByte(offset + 3) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 19;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES + Byte.BYTES));
      int valueInFirst3Bytes =
          ((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) & (0xffffff
              >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 5;
      if (numBitsLeft <= 0) {
        return valueInFirst3Bytes >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 3, Byte.BYTES));
        return (valueInFirst3Bytes << numBitsLeft) | ((_dataBuffer.getByte(offset + 3) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 19;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (13 - bitOffsetInFirstByte)) & 0x7ffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 19;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      out[outPos] = i0 >>> 13;
      out[outPos + 1] = ((i0 & 0x1fff) << 6) | (i1 >>> 26);
      out[outPos + 2] = (i1 >>> 7) & 0x7ffff;
      out[outPos + 3] = ((i1 & 0x7f) << 12) | (i2 >>> 20);
      out[outPos + 4] = (i2 >>> 1) & 0x7ffff;
      out[outPos + 5] = ((i2 & 0x1) << 18) | (i3 >>> 14);
      out[outPos + 6] = ((i3 & 0x3fff) << 5) | (i4 >>> 27);
      out[outPos + 7] = (i4 >>> 8) & 0x7ffff;
      out[outPos + 8] = ((i4 & 0xff) << 11) | (i5 >>> 21);
      out[outPos + 9] = (i5 >>> 2) & 0x7ffff;
      out[outPos + 10] = ((i5 & 0x3) << 17) | (i6 >>> 15);
      out[outPos + 11] = ((i6 & 0x7fff) << 4) | (i7 >>> 28);
      out[outPos + 12] = (i7 >>> 9) & 0x7ffff;
      out[outPos + 13] = ((i7 & 0x1ff) << 10) | (i8 >>> 22);
      out[outPos + 14] = (i8 >>> 3) & 0x7ffff;
      out[outPos + 15] = ((i8 & 0x7) << 16) | (i9 >>> 16);
      out[outPos + 16] = ((i9 & 0xffff) << 3) | (i10 >>> 29);
      out[outPos + 17] = (i10 >>> 10) & 0x7ffff;
      out[outPos + 18] = ((i10 & 0x3ff) << 9) | (i11 >>> 23);
      out[outPos + 19] = (i11 >>> 4) & 0x7ffff;
      out[outPos + 20] = ((i11 & 0xf) << 15) | (i12 >>> 17);
      out[outPos + 21] = ((i12 & 0x1ffff) << 2) | (i13 >>> 30);
      out[outPos + 22] = (i13 >>> 11) & 0x7ffff;
      out[outPos + 23] = ((i13 & 0x7ff) << 8) | (i14 >>> 24);
      out[outPos + 24] = (i14 >>> 5) & 0x7ffff;
      out[outPos + 25] = ((i14 & 0x1f) << 14) | (i15 >>> 18);
      out[outPos + 26] = ((i15 & 0x3ffff) << 1) | (i16 >>> 31);
      out[outPos + 27] = (i16 >>> 12) & 0x7ffff;
      out[outPos + 28] = ((i16 & 0xfff) << 7) | (i17 >>> 25);
      out[outPos + 29] = (i17 >>> 6) & 0x7ffff;
      out[outPos + 30] = ((i17 & 0x3f) << 13) | (i18 >>> 19);
      out[outPos + 31] = i18 & 0x7ffff;
    }
  }

  private static class Bit20Reader extends FixedBitIntReader {

    private Bit20Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 20;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) >>> (4
          - bitOffsetInFirstByte)) & 0xfffff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 20;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES + Byte.BYTES));
      return (((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) >>> (4
          - bitOffsetInFirstByte)) & 0xfffff;
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 20;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (12 - bitOffsetInFirstByte)) & 0xfffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 20;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      out[outPos] = i0 >>> 12;
      out[outPos + 1] = ((i0 & 0xfff) << 8) | (i1 >>> 24);
      out[outPos + 2] = (i1 >>> 4) & 0xfffff;
      out[outPos + 3] = ((i1 & 0xf) << 16) | (i2 >>> 16);
      out[outPos + 4] = ((i2 & 0xffff) << 4) | (i3 >>> 28);
      out[outPos + 5] = (i3 >>> 8) & 0xfffff;
      out[outPos + 6] = ((i3 & 0xff) << 12) | (i4 >>> 20);
      out[outPos + 7] = i4 & 0xfffff;
      out[outPos + 8] = i5 >>> 12;
      out[outPos + 9] = ((i5 & 0xfff) << 8) | (i6 >>> 24);
      out[outPos + 10] = (i6 >>> 4) & 0xfffff;
      out[outPos + 11] = ((i6 & 0xf) << 16) | (i7 >>> 16);
      out[outPos + 12] = ((i7 & 0xffff) << 4) | (i8 >>> 28);
      out[outPos + 13] = (i8 >>> 8) & 0xfffff;
      out[outPos + 14] = ((i8 & 0xff) << 12) | (i9 >>> 20);
      out[outPos + 15] = i9 & 0xfffff;
      out[outPos + 16] = i10 >>> 12;
      out[outPos + 17] = ((i10 & 0xfff) << 8) | (i11 >>> 24);
      out[outPos + 18] = (i11 >>> 4) & 0xfffff;
      out[outPos + 19] = ((i11 & 0xf) << 16) | (i12 >>> 16);
      out[outPos + 20] = ((i12 & 0xffff) << 4) | (i13 >>> 28);
      out[outPos + 21] = (i13 >>> 8) & 0xfffff;
      out[outPos + 22] = ((i13 & 0xff) << 12) | (i14 >>> 20);
      out[outPos + 23] = i14 & 0xfffff;
      out[outPos + 24] = i15 >>> 12;
      out[outPos + 25] = ((i15 & 0xfff) << 8) | (i16 >>> 24);
      out[outPos + 26] = (i16 >>> 4) & 0xfffff;
      out[outPos + 27] = ((i16 & 0xf) << 16) | (i17 >>> 16);
      out[outPos + 28] = ((i17 & 0xffff) << 4) | (i18 >>> 28);
      out[outPos + 29] = (i18 >>> 8) & 0xfffff;
      out[outPos + 30] = ((i18 & 0xff) << 12) | (i19 >>> 20);
      out[outPos + 31] = i19 & 0xfffff;
    }
  }

  private static class Bit21Reader extends FixedBitIntReader {

    private Bit21Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 21;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirst3Bytes =
          ((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) & (0xffffff
              >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 3;
      if (numBitsLeft <= 0) {
        return valueInFirst3Bytes >>> -numBitsLeft;
      } else {
        return (valueInFirst3Bytes << numBitsLeft) | ((_dataBuffer.getByte(offset + 3) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 21;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES + Byte.BYTES));
      int valueInFirst3Bytes =
          ((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) & (0xffffff
              >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 3;
      if (numBitsLeft <= 0) {
        return valueInFirst3Bytes >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 3, Byte.BYTES));
        return (valueInFirst3Bytes << numBitsLeft) | ((_dataBuffer.getByte(offset + 3) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 21;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (11 - bitOffsetInFirstByte)) & 0x1fffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 21;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      out[outPos] = i0 >>> 11;
      out[outPos + 1] = ((i0 & 0x7ff) << 10) | (i1 >>> 22);
      out[outPos + 2] = (i1 >>> 1) & 0x1fffff;
      out[outPos + 3] = ((i1 & 0x1) << 20) | (i2 >>> 12);
      out[outPos + 4] = ((i2 & 0xfff) << 9) | (i3 >>> 23);
      out[outPos + 5] = (i3 >>> 2) & 0x1fffff;
      out[outPos + 6] = ((i3 & 0x3) << 19) | (i4 >>> 13);
      out[outPos + 7] = ((i4 & 0x1fff) << 8) | (i5 >>> 24);
      out[outPos + 8] = (i5 >>> 3) & 0x1fffff;
      out[outPos + 9] = ((i5 & 0x7) << 18) | (i6 >>> 14);
      out[outPos + 10] = ((i6 & 0x3fff) << 7) | (i7 >>> 25);
      out[outPos + 11] = (i7 >>> 4) & 0x1fffff;
      out[outPos + 12] = ((i7 & 0xf) << 17) | (i8 >>> 15);
      out[outPos + 13] = ((i8 & 0x7fff) << 6) | (i9 >>> 26);
      out[outPos + 14] = (i9 >>> 5) & 0x1fffff;
      out[outPos + 15] = ((i9 & 0x1f) << 16) | (i10 >>> 16);
      out[outPos + 16] = ((i10 & 0xffff) << 5) | (i11 >>> 27);
      out[outPos + 17] = (i11 >>> 6) & 0x1fffff;
      out[outPos + 18] = ((i11 & 0x3f) << 15) | (i12 >>> 17);
      out[outPos + 19] = ((i12 & 0x1ffff) << 4) | (i13 >>> 28);
      out[outPos + 20] = (i13 >>> 7) & 0x1fffff;
      out[outPos + 21] = ((i13 & 0x7f) << 14) | (i14 >>> 18);
      out[outPos + 22] = ((i14 & 0x3ffff) << 3) | (i15 >>> 29);
      out[outPos + 23] = (i15 >>> 8) & 0x1fffff;
      out[outPos + 24] = ((i15 & 0xff) << 13) | (i16 >>> 19);
      out[outPos + 25] = ((i16 & 0x7ffff) << 2) | (i17 >>> 30);
      out[outPos + 26] = (i17 >>> 9) & 0x1fffff;
      out[outPos + 27] = ((i17 & 0x1ff) << 12) | (i18 >>> 20);
      out[outPos + 28] = ((i18 & 0xfffff) << 1) | (i19 >>> 31);
      out[outPos + 29] = (i19 >>> 10) & 0x1fffff;
      out[outPos + 30] = ((i19 & 0x3ff) << 11) | (i20 >>> 21);
      out[outPos + 31] = i20 & 0x1fffff;
    }
  }

  private static class Bit22Reader extends FixedBitIntReader {

    private Bit22Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 22;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirst3Bytes =
          ((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) & (0xffffff
              >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 2;
      if (numBitsLeft <= 0) {
        return valueInFirst3Bytes >>> -numBitsLeft;
      } else {
        return (valueInFirst3Bytes << numBitsLeft) | ((_dataBuffer.getByte(offset + 3) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 22;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES + Byte.BYTES));
      int valueInFirst3Bytes =
          ((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) & (0xffffff
              >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 2;
      if (numBitsLeft <= 0) {
        return valueInFirst3Bytes >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 3, Byte.BYTES));
        return (valueInFirst3Bytes << numBitsLeft) | ((_dataBuffer.getByte(offset + 3) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 22;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (10 - bitOffsetInFirstByte)) & 0x3fffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 22;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      out[outPos] = i0 >>> 10;
      out[outPos + 1] = ((i0 & 0x3ff) << 12) | (i1 >>> 20);
      out[outPos + 2] = ((i1 & 0xfffff) << 2) | (i2 >>> 30);
      out[outPos + 3] = (i2 >>> 8) & 0x3fffff;
      out[outPos + 4] = ((i2 & 0xff) << 14) | (i3 >>> 18);
      out[outPos + 5] = ((i3 & 0x3ffff) << 4) | (i4 >>> 28);
      out[outPos + 6] = (i4 >>> 6) & 0x3fffff;
      out[outPos + 7] = ((i4 & 0x3f) << 16) | (i5 >>> 16);
      out[outPos + 8] = ((i5 & 0xffff) << 6) | (i6 >>> 26);
      out[outPos + 9] = (i6 >>> 4) & 0x3fffff;
      out[outPos + 10] = ((i6 & 0xf) << 18) | (i7 >>> 14);
      out[outPos + 11] = ((i7 & 0x3fff) << 8) | (i8 >>> 24);
      out[outPos + 12] = (i8 >>> 2) & 0x3fffff;
      out[outPos + 13] = ((i8 & 0x3) << 20) | (i9 >>> 12);
      out[outPos + 14] = ((i9 & 0xfff) << 10) | (i10 >>> 22);
      out[outPos + 15] = i10 & 0x3fffff;
      out[outPos + 16] = i11 >>> 10;
      out[outPos + 17] = ((i11 & 0x3ff) << 12) | (i12 >>> 20);
      out[outPos + 18] = ((i12 & 0xfffff) << 2) | (i13 >>> 30);
      out[outPos + 19] = (i13 >>> 8) & 0x3fffff;
      out[outPos + 20] = ((i13 & 0xff) << 14) | (i14 >>> 18);
      out[outPos + 21] = ((i14 & 0x3ffff) << 4) | (i15 >>> 28);
      out[outPos + 22] = (i15 >>> 6) & 0x3fffff;
      out[outPos + 23] = ((i15 & 0x3f) << 16) | (i16 >>> 16);
      out[outPos + 24] = ((i16 & 0xffff) << 6) | (i17 >>> 26);
      out[outPos + 25] = (i17 >>> 4) & 0x3fffff;
      out[outPos + 26] = ((i17 & 0xf) << 18) | (i18 >>> 14);
      out[outPos + 27] = ((i18 & 0x3fff) << 8) | (i19 >>> 24);
      out[outPos + 28] = (i19 >>> 2) & 0x3fffff;
      out[outPos + 29] = ((i19 & 0x3) << 20) | (i20 >>> 12);
      out[outPos + 30] = ((i20 & 0xfff) << 10) | (i21 >>> 22);
      out[outPos + 31] = i21 & 0x3fffff;
    }
  }

  private static class Bit23Reader extends FixedBitIntReader {

    private Bit23Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 23;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirst3Bytes =
          ((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) & (0xffffff
              >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 1;
      if (numBitsLeft <= 0) {
        return valueInFirst3Bytes >>> -numBitsLeft;
      } else {
        return (valueInFirst3Bytes << numBitsLeft) | ((_dataBuffer.getByte(offset + 3) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 23;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES + Byte.BYTES));
      int valueInFirst3Bytes =
          ((_dataBuffer.getShort(offset) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff)) & (0xffffff
              >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 1;
      if (numBitsLeft <= 0) {
        return valueInFirst3Bytes >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 3, Byte.BYTES));
        return (valueInFirst3Bytes << numBitsLeft) | ((_dataBuffer.getByte(offset + 3) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 23;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (9 - bitOffsetInFirstByte)) & 0x7fffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 23;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      int i22 = _dataBuffer.getInt(offset + 88);
      out[outPos] = i0 >>> 9;
      out[outPos + 1] = ((i0 & 0x1ff) << 14) | (i1 >>> 18);
      out[outPos + 2] = ((i1 & 0x3ffff) << 5) | (i2 >>> 27);
      out[outPos + 3] = (i2 >>> 4) & 0x7fffff;
      out[outPos + 4] = ((i2 & 0xf) << 19) | (i3 >>> 13);
      out[outPos + 5] = ((i3 & 0x1fff) << 10) | (i4 >>> 22);
      out[outPos + 6] = ((i4 & 0x3fffff) << 1) | (i5 >>> 31);
      out[outPos + 7] = (i5 >>> 8) & 0x7fffff;
      out[outPos + 8] = ((i5 & 0xff) << 15) | (i6 >>> 17);
      out[outPos + 9] = ((i6 & 0x1ffff) << 6) | (i7 >>> 26);
      out[outPos + 10] = (i7 >>> 3) & 0x7fffff;
      out[outPos + 11] = ((i7 & 0x7) << 20) | (i8 >>> 12);
      out[outPos + 12] = ((i8 & 0xfff) << 11) | (i9 >>> 21);
      out[outPos + 13] = ((i9 & 0x1fffff) << 2) | (i10 >>> 30);
      out[outPos + 14] = (i10 >>> 7) & 0x7fffff;
      out[outPos + 15] = ((i10 & 0x7f) << 16) | (i11 >>> 16);
      out[outPos + 16] = ((i11 & 0xffff) << 7) | (i12 >>> 25);
      out[outPos + 17] = (i12 >>> 2) & 0x7fffff;
      out[outPos + 18] = ((i12 & 0x3) << 21) | (i13 >>> 11);
      out[outPos + 19] = ((i13 & 0x7ff) << 12) | (i14 >>> 20);
      out[outPos + 20] = ((i14 & 0xfffff) << 3) | (i15 >>> 29);
      out[outPos + 21] = (i15 >>> 6) & 0x7fffff;
      out[outPos + 22] = ((i15 & 0x3f) << 17) | (i16 >>> 15);
      out[outPos + 23] = ((i16 & 0x7fff) << 8) | (i17 >>> 24);
      out[outPos + 24] = (i17 >>> 1) & 0x7fffff;
      out[outPos + 25] = ((i17 & 0x1) << 22) | (i18 >>> 10);
      out[outPos + 26] = ((i18 & 0x3ff) << 13) | (i19 >>> 19);
      out[outPos + 27] = ((i19 & 0x7ffff) << 4) | (i20 >>> 28);
      out[outPos + 28] = (i20 >>> 5) & 0x7fffff;
      out[outPos + 29] = ((i20 & 0x1f) << 18) | (i21 >>> 14);
      out[outPos + 30] = ((i21 & 0x3fff) << 9) | (i22 >>> 23);
      out[outPos + 31] = i22 & 0x7fffff;
    }
  }

  private static class Bit24Reader extends FixedBitIntReader {

    private Bit24Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long offset = (long) index * 3;
      return ((_dataBuffer.getShort(offset) & 0xffff) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff);
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long offset = (long) index * 3;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Short.BYTES + Byte.BYTES));
      return ((_dataBuffer.getShort(offset) & 0xffff) << 8) | (_dataBuffer.getByte(offset + 2) & 0xff);
    }

    @Override
    public int readUnchecked(int index) {
      long offset = (long) index * 3;
      return _dataBuffer.getInt(offset) >>> 8;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) index * 3;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      int i22 = _dataBuffer.getInt(offset + 88);
      int i23 = _dataBuffer.getInt(offset + 92);
      out[outPos] = i0 >>> 8;
      out[outPos + 1] = ((i0 & 0xff) << 16) | (i1 >>> 16);
      out[outPos + 2] = ((i1 & 0xffff) << 8) | (i2 >>> 24);
      out[outPos + 3] = i2 & 0xffffff;
      out[outPos + 4] = i3 >>> 8;
      out[outPos + 5] = ((i3 & 0xff) << 16) | (i4 >>> 16);
      out[outPos + 6] = ((i4 & 0xffff) << 8) | (i5 >>> 24);
      out[outPos + 7] = i5 & 0xffffff;
      out[outPos + 8] = i6 >>> 8;
      out[outPos + 9] = ((i6 & 0xff) << 16) | (i7 >>> 16);
      out[outPos + 10] = ((i7 & 0xffff) << 8) | (i8 >>> 24);
      out[outPos + 11] = i8 & 0xffffff;
      out[outPos + 12] = i9 >>> 8;
      out[outPos + 13] = ((i9 & 0xff) << 16) | (i10 >>> 16);
      out[outPos + 14] = ((i10 & 0xffff) << 8) | (i11 >>> 24);
      out[outPos + 15] = i11 & 0xffffff;
      out[outPos + 16] = i12 >>> 8;
      out[outPos + 17] = ((i12 & 0xff) << 16) | (i13 >>> 16);
      out[outPos + 18] = ((i13 & 0xffff) << 8) | (i14 >>> 24);
      out[outPos + 19] = i14 & 0xffffff;
      out[outPos + 20] = i15 >>> 8;
      out[outPos + 21] = ((i15 & 0xff) << 16) | (i16 >>> 16);
      out[outPos + 22] = ((i16 & 0xffff) << 8) | (i17 >>> 24);
      out[outPos + 23] = i17 & 0xffffff;
      out[outPos + 24] = i18 >>> 8;
      out[outPos + 25] = ((i18 & 0xff) << 16) | (i19 >>> 16);
      out[outPos + 26] = ((i19 & 0xffff) << 8) | (i20 >>> 24);
      out[outPos + 27] = i20 & 0xffffff;
      out[outPos + 28] = i21 >>> 8;
      out[outPos + 29] = ((i21 & 0xff) << 16) | (i22 >>> 16);
      out[outPos + 30] = ((i22 & 0xffff) << 8) | (i23 >>> 24);
      out[outPos + 31] = i23 & 0xffffff;
    }
  }

  private static class Bit25Reader extends FixedBitIntReader {

    private Bit25Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 25;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (7 - bitOffsetInFirstByte)) & 0x1ffffff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 25;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Integer.BYTES));
      return (_dataBuffer.getInt(offset) >>> (7 - bitOffsetInFirstByte)) & 0x1ffffff;
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 25;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (7 - bitOffsetInFirstByte)) & 0x1ffffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 25;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      int i22 = _dataBuffer.getInt(offset + 88);
      int i23 = _dataBuffer.getInt(offset + 92);
      int i24 = _dataBuffer.getInt(offset + 96);
      out[outPos] = i0 >>> 7;
      out[outPos + 1] = ((i0 & 0x7f) << 18) | (i1 >>> 14);
      out[outPos + 2] = ((i1 & 0x3fff) << 11) | (i2 >>> 21);
      out[outPos + 3] = ((i2 & 0x1fffff) << 4) | (i3 >>> 28);
      out[outPos + 4] = (i3 >>> 3) & 0x1ffffff;
      out[outPos + 5] = ((i3 & 0x7) << 22) | (i4 >>> 10);
      out[outPos + 6] = ((i4 & 0x3ff) << 15) | (i5 >>> 17);
      out[outPos + 7] = ((i5 & 0x1ffff) << 8) | (i6 >>> 24);
      out[outPos + 8] = ((i6 & 0xffffff) << 1) | (i7 >>> 31);
      out[outPos + 9] = (i7 >>> 6) & 0x1ffffff;
      out[outPos + 10] = ((i7 & 0x3f) << 19) | (i8 >>> 13);
      out[outPos + 11] = ((i8 & 0x1fff) << 12) | (i9 >>> 20);
      out[outPos + 12] = ((i9 & 0xfffff) << 5) | (i10 >>> 27);
      out[outPos + 13] = (i10 >>> 2) & 0x1ffffff;
      out[outPos + 14] = ((i10 & 0x3) << 23) | (i11 >>> 9);
      out[outPos + 15] = ((i11 & 0x1ff) << 16) | (i12 >>> 16);
      out[outPos + 16] = ((i12 & 0xffff) << 9) | (i13 >>> 23);
      out[outPos + 17] = ((i13 & 0x7fffff) << 2) | (i14 >>> 30);
      out[outPos + 18] = (i14 >>> 5) & 0x1ffffff;
      out[outPos + 19] = ((i14 & 0x1f) << 20) | (i15 >>> 12);
      out[outPos + 20] = ((i15 & 0xfff) << 13) | (i16 >>> 19);
      out[outPos + 21] = ((i16 & 0x7ffff) << 6) | (i17 >>> 26);
      out[outPos + 22] = (i17 >>> 1) & 0x1ffffff;
      out[outPos + 23] = ((i17 & 0x1) << 24) | (i18 >>> 8);
      out[outPos + 24] = ((i18 & 0xff) << 17) | (i19 >>> 15);
      out[outPos + 25] = ((i19 & 0x7fff) << 10) | (i20 >>> 22);
      out[outPos + 26] = ((i20 & 0x3fffff) << 3) | (i21 >>> 29);
      out[outPos + 27] = (i21 >>> 4) & 0x1ffffff;
      out[outPos + 28] = ((i21 & 0xf) << 21) | (i22 >>> 11);
      out[outPos + 29] = ((i22 & 0x7ff) << 14) | (i23 >>> 18);
      out[outPos + 30] = ((i23 & 0x3ffff) << 7) | (i24 >>> 25);
      out[outPos + 31] = i24 & 0x1ffffff;
    }
  }

  private static class Bit26Reader extends FixedBitIntReader {

    private Bit26Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 26;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (6 - bitOffsetInFirstByte)) & 0x3ffffff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 26;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Integer.BYTES));
      return (_dataBuffer.getInt(offset) >>> (6 - bitOffsetInFirstByte)) & 0x3ffffff;
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 26;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (6 - bitOffsetInFirstByte)) & 0x3ffffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 26;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      int i22 = _dataBuffer.getInt(offset + 88);
      int i23 = _dataBuffer.getInt(offset + 92);
      int i24 = _dataBuffer.getInt(offset + 96);
      int i25 = _dataBuffer.getInt(offset + 100);
      out[outPos] = i0 >>> 6;
      out[outPos + 1] = ((i0 & 0x3f) << 20) | (i1 >>> 12);
      out[outPos + 2] = ((i1 & 0xfff) << 14) | (i2 >>> 18);
      out[outPos + 3] = ((i2 & 0x3ffff) << 8) | (i3 >>> 24);
      out[outPos + 4] = ((i3 & 0xffffff) << 2) | (i4 >>> 30);
      out[outPos + 5] = (i4 >>> 4) & 0x3ffffff;
      out[outPos + 6] = ((i4 & 0xf) << 22) | (i5 >>> 10);
      out[outPos + 7] = ((i5 & 0x3ff) << 16) | (i6 >>> 16);
      out[outPos + 8] = ((i6 & 0xffff) << 10) | (i7 >>> 22);
      out[outPos + 9] = ((i7 & 0x3fffff) << 4) | (i8 >>> 28);
      out[outPos + 10] = (i8 >>> 2) & 0x3ffffff;
      out[outPos + 11] = ((i8 & 0x3) << 24) | (i9 >>> 8);
      out[outPos + 12] = ((i9 & 0xff) << 18) | (i10 >>> 14);
      out[outPos + 13] = ((i10 & 0x3fff) << 12) | (i11 >>> 20);
      out[outPos + 14] = ((i11 & 0xfffff) << 6) | (i12 >>> 26);
      out[outPos + 15] = i12 & 0x3ffffff;
      out[outPos + 16] = i13 >>> 6;
      out[outPos + 17] = ((i13 & 0x3f) << 20) | (i14 >>> 12);
      out[outPos + 18] = ((i14 & 0xfff) << 14) | (i15 >>> 18);
      out[outPos + 19] = ((i15 & 0x3ffff) << 8) | (i16 >>> 24);
      out[outPos + 20] = ((i16 & 0xffffff) << 2) | (i17 >>> 30);
      out[outPos + 21] = (i17 >>> 4) & 0x3ffffff;
      out[outPos + 22] = ((i17 & 0xf) << 22) | (i18 >>> 10);
      out[outPos + 23] = ((i18 & 0x3ff) << 16) | (i19 >>> 16);
      out[outPos + 24] = ((i19 & 0xffff) << 10) | (i20 >>> 22);
      out[outPos + 25] = ((i20 & 0x3fffff) << 4) | (i21 >>> 28);
      out[outPos + 26] = (i21 >>> 2) & 0x3ffffff;
      out[outPos + 27] = ((i21 & 0x3) << 24) | (i22 >>> 8);
      out[outPos + 28] = ((i22 & 0xff) << 18) | (i23 >>> 14);
      out[outPos + 29] = ((i23 & 0x3fff) << 12) | (i24 >>> 20);
      out[outPos + 30] = ((i24 & 0xfffff) << 6) | (i25 >>> 26);
      out[outPos + 31] = i25 & 0x3ffffff;
    }
  }

  private static class Bit27Reader extends FixedBitIntReader {

    private Bit27Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 27;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstInt = _dataBuffer.getInt(offset) & (0xffffffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 5;
      if (numBitsLeft <= 0) {
        return valueInFirstInt >>> -numBitsLeft;
      } else {
        return (valueInFirstInt << numBitsLeft) | ((_dataBuffer.getByte(offset + 4) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 27;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Integer.BYTES));
      int valueInFirstInt = _dataBuffer.getInt(offset) & (0xffffffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 5;
      if (numBitsLeft <= 0) {
        return valueInFirstInt >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 4, Byte.BYTES));
        return (valueInFirstInt << numBitsLeft) | ((_dataBuffer.getByte(offset + 4) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 27;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (int) (_dataBuffer.getLong(offset) >>> (37 - bitOffsetInFirstByte)) & 0x7ffffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 27;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      int i22 = _dataBuffer.getInt(offset + 88);
      int i23 = _dataBuffer.getInt(offset + 92);
      int i24 = _dataBuffer.getInt(offset + 96);
      int i25 = _dataBuffer.getInt(offset + 100);
      int i26 = _dataBuffer.getInt(offset + 104);
      out[outPos] = i0 >>> 5;
      out[outPos + 1] = ((i0 & 0x1f) << 22) | (i1 >>> 10);
      out[outPos + 2] = ((i1 & 0x3ff) << 17) | (i2 >>> 15);
      out[outPos + 3] = ((i2 & 0x7fff) << 12) | (i3 >>> 20);
      out[outPos + 4] = ((i3 & 0xfffff) << 7) | (i4 >>> 25);
      out[outPos + 5] = ((i4 & 0x1ffffff) << 2) | (i5 >>> 30);
      out[outPos + 6] = (i5 >>> 3) & 0x7ffffff;
      out[outPos + 7] = ((i5 & 0x7) << 24) | (i6 >>> 8);
      out[outPos + 8] = ((i6 & 0xff) << 19) | (i7 >>> 13);
      out[outPos + 9] = ((i7 & 0x1fff) << 14) | (i8 >>> 18);
      out[outPos + 10] = ((i8 & 0x3ffff) << 9) | (i9 >>> 23);
      out[outPos + 11] = ((i9 & 0x7fffff) << 4) | (i10 >>> 28);
      out[outPos + 12] = (i10 >>> 1) & 0x7ffffff;
      out[outPos + 13] = ((i10 & 0x1) << 26) | (i11 >>> 6);
      out[outPos + 14] = ((i11 & 0x3f) << 21) | (i12 >>> 11);
      out[outPos + 15] = ((i12 & 0x7ff) << 16) | (i13 >>> 16);
      out[outPos + 16] = ((i13 & 0xffff) << 11) | (i14 >>> 21);
      out[outPos + 17] = ((i14 & 0x1fffff) << 6) | (i15 >>> 26);
      out[outPos + 18] = ((i15 & 0x3ffffff) << 1) | (i16 >>> 31);
      out[outPos + 19] = (i16 >>> 4) & 0x7ffffff;
      out[outPos + 20] = ((i16 & 0xf) << 23) | (i17 >>> 9);
      out[outPos + 21] = ((i17 & 0x1ff) << 18) | (i18 >>> 14);
      out[outPos + 22] = ((i18 & 0x3fff) << 13) | (i19 >>> 19);
      out[outPos + 23] = ((i19 & 0x7ffff) << 8) | (i20 >>> 24);
      out[outPos + 24] = ((i20 & 0xffffff) << 3) | (i21 >>> 29);
      out[outPos + 25] = (i21 >>> 2) & 0x7ffffff;
      out[outPos + 26] = ((i21 & 0x3) << 25) | (i22 >>> 7);
      out[outPos + 27] = ((i22 & 0x7f) << 20) | (i23 >>> 12);
      out[outPos + 28] = ((i23 & 0xfff) << 15) | (i24 >>> 17);
      out[outPos + 29] = ((i24 & 0x1ffff) << 10) | (i25 >>> 22);
      out[outPos + 30] = ((i25 & 0x3fffff) << 5) | (i26 >>> 27);
      out[outPos + 31] = i26 & 0x7ffffff;
    }
  }

  private static class Bit28Reader extends FixedBitIntReader {

    private Bit28Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 28;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (4 - bitOffsetInFirstByte)) & 0xfffffff;
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 28;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Integer.BYTES));
      return (_dataBuffer.getInt(offset) >>> (4 - bitOffsetInFirstByte)) & 0xfffffff;
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 28;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (_dataBuffer.getInt(offset) >>> (4 - bitOffsetInFirstByte)) & 0xfffffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 28;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      int i22 = _dataBuffer.getInt(offset + 88);
      int i23 = _dataBuffer.getInt(offset + 92);
      int i24 = _dataBuffer.getInt(offset + 96);
      int i25 = _dataBuffer.getInt(offset + 100);
      int i26 = _dataBuffer.getInt(offset + 104);
      int i27 = _dataBuffer.getInt(offset + 108);
      out[outPos] = i0 >>> 4;
      out[outPos + 1] = ((i0 & 0xf) << 24) | (i1 >>> 8);
      out[outPos + 2] = ((i1 & 0xff) << 20) | (i2 >>> 12);
      out[outPos + 3] = ((i2 & 0xfff) << 16) | (i3 >>> 16);
      out[outPos + 4] = ((i3 & 0xffff) << 12) | (i4 >>> 20);
      out[outPos + 5] = ((i4 & 0xfffff) << 8) | (i5 >>> 24);
      out[outPos + 6] = ((i5 & 0xffffff) << 4) | (i6 >>> 28);
      out[outPos + 7] = i6 & 0xfffffff;
      out[outPos + 8] = i7 >>> 4;
      out[outPos + 9] = ((i7 & 0xf) << 24) | (i8 >>> 8);
      out[outPos + 10] = ((i8 & 0xff) << 20) | (i9 >>> 12);
      out[outPos + 11] = ((i9 & 0xfff) << 16) | (i10 >>> 16);
      out[outPos + 12] = ((i10 & 0xffff) << 12) | (i11 >>> 20);
      out[outPos + 13] = ((i11 & 0xfffff) << 8) | (i12 >>> 24);
      out[outPos + 14] = ((i12 & 0xffffff) << 4) | (i13 >>> 28);
      out[outPos + 15] = i13 & 0xfffffff;
      out[outPos + 16] = i14 >>> 4;
      out[outPos + 17] = ((i14 & 0xf) << 24) | (i15 >>> 8);
      out[outPos + 18] = ((i15 & 0xff) << 20) | (i16 >>> 12);
      out[outPos + 19] = ((i16 & 0xfff) << 16) | (i17 >>> 16);
      out[outPos + 20] = ((i17 & 0xffff) << 12) | (i18 >>> 20);
      out[outPos + 21] = ((i18 & 0xfffff) << 8) | (i19 >>> 24);
      out[outPos + 22] = ((i19 & 0xffffff) << 4) | (i20 >>> 28);
      out[outPos + 23] = i20 & 0xfffffff;
      out[outPos + 24] = i21 >>> 4;
      out[outPos + 25] = ((i21 & 0xf) << 24) | (i22 >>> 8);
      out[outPos + 26] = ((i22 & 0xff) << 20) | (i23 >>> 12);
      out[outPos + 27] = ((i23 & 0xfff) << 16) | (i24 >>> 16);
      out[outPos + 28] = ((i24 & 0xffff) << 12) | (i25 >>> 20);
      out[outPos + 29] = ((i25 & 0xfffff) << 8) | (i26 >>> 24);
      out[outPos + 30] = ((i26 & 0xffffff) << 4) | (i27 >>> 28);
      out[outPos + 31] = i27 & 0xfffffff;
    }
  }

  private static class Bit29Reader extends FixedBitIntReader {

    private Bit29Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 29;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstInt = _dataBuffer.getInt(offset) & (0xffffffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 3;
      if (numBitsLeft <= 0) {
        return valueInFirstInt >>> -numBitsLeft;
      } else {
        return (valueInFirstInt << numBitsLeft) | ((_dataBuffer.getByte(offset + 4) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 29;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Integer.BYTES));
      int valueInFirstInt = _dataBuffer.getInt(offset) & (0xffffffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 3;
      if (numBitsLeft <= 0) {
        return valueInFirstInt >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 4, Byte.BYTES));
        return (valueInFirstInt << numBitsLeft) | ((_dataBuffer.getByte(offset + 4) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 29;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (int) (_dataBuffer.getLong(offset) >>> (35 - bitOffsetInFirstByte)) & 0x1fffffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 29;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      int i22 = _dataBuffer.getInt(offset + 88);
      int i23 = _dataBuffer.getInt(offset + 92);
      int i24 = _dataBuffer.getInt(offset + 96);
      int i25 = _dataBuffer.getInt(offset + 100);
      int i26 = _dataBuffer.getInt(offset + 104);
      int i27 = _dataBuffer.getInt(offset + 108);
      int i28 = _dataBuffer.getInt(offset + 112);
      out[outPos] = i0 >>> 3;
      out[outPos + 1] = ((i0 & 0x7) << 26) | (i1 >>> 6);
      out[outPos + 2] = ((i1 & 0x3f) << 23) | (i2 >>> 9);
      out[outPos + 3] = ((i2 & 0x1ff) << 20) | (i3 >>> 12);
      out[outPos + 4] = ((i3 & 0xfff) << 17) | (i4 >>> 15);
      out[outPos + 5] = ((i4 & 0x7fff) << 14) | (i5 >>> 18);
      out[outPos + 6] = ((i5 & 0x3ffff) << 11) | (i6 >>> 21);
      out[outPos + 7] = ((i6 & 0x1fffff) << 8) | (i7 >>> 24);
      out[outPos + 8] = ((i7 & 0xffffff) << 5) | (i8 >>> 27);
      out[outPos + 9] = ((i8 & 0x7ffffff) << 2) | (i9 >>> 30);
      out[outPos + 10] = (i9 >>> 1) & 0x1fffffff;
      out[outPos + 11] = ((i9 & 0x1) << 28) | (i10 >>> 4);
      out[outPos + 12] = ((i10 & 0xf) << 25) | (i11 >>> 7);
      out[outPos + 13] = ((i11 & 0x7f) << 22) | (i12 >>> 10);
      out[outPos + 14] = ((i12 & 0x3ff) << 19) | (i13 >>> 13);
      out[outPos + 15] = ((i13 & 0x1fff) << 16) | (i14 >>> 16);
      out[outPos + 16] = ((i14 & 0xffff) << 13) | (i15 >>> 19);
      out[outPos + 17] = ((i15 & 0x7ffff) << 10) | (i16 >>> 22);
      out[outPos + 18] = ((i16 & 0x3fffff) << 7) | (i17 >>> 25);
      out[outPos + 19] = ((i17 & 0x1ffffff) << 4) | (i18 >>> 28);
      out[outPos + 20] = ((i18 & 0xfffffff) << 1) | (i19 >>> 31);
      out[outPos + 21] = (i19 >>> 2) & 0x1fffffff;
      out[outPos + 22] = ((i19 & 0x3) << 27) | (i20 >>> 5);
      out[outPos + 23] = ((i20 & 0x1f) << 24) | (i21 >>> 8);
      out[outPos + 24] = ((i21 & 0xff) << 21) | (i22 >>> 11);
      out[outPos + 25] = ((i22 & 0x7ff) << 18) | (i23 >>> 14);
      out[outPos + 26] = ((i23 & 0x3fff) << 15) | (i24 >>> 17);
      out[outPos + 27] = ((i24 & 0x1ffff) << 12) | (i25 >>> 20);
      out[outPos + 28] = ((i25 & 0xfffff) << 9) | (i26 >>> 23);
      out[outPos + 29] = ((i26 & 0x7fffff) << 6) | (i27 >>> 26);
      out[outPos + 30] = ((i27 & 0x3ffffff) << 3) | (i28 >>> 29);
      out[outPos + 31] = i28 & 0x1fffffff;
    }
  }

  private static class Bit30Reader extends FixedBitIntReader {

    private Bit30Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 30;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstInt = _dataBuffer.getInt(offset) & (0xffffffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 2;
      if (numBitsLeft <= 0) {
        return valueInFirstInt >>> -numBitsLeft;
      } else {
        return (valueInFirstInt << numBitsLeft) | ((_dataBuffer.getByte(offset + 4) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 30;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Integer.BYTES));
      int valueInFirstInt = _dataBuffer.getInt(offset) & (0xffffffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 2;
      if (numBitsLeft <= 0) {
        return valueInFirstInt >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 4, Byte.BYTES));
        return (valueInFirstInt << numBitsLeft) | ((_dataBuffer.getByte(offset + 4) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 30;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (int) (_dataBuffer.getLong(offset) >>> (34 - bitOffsetInFirstByte)) & 0x3fffffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 30;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      int i22 = _dataBuffer.getInt(offset + 88);
      int i23 = _dataBuffer.getInt(offset + 92);
      int i24 = _dataBuffer.getInt(offset + 96);
      int i25 = _dataBuffer.getInt(offset + 100);
      int i26 = _dataBuffer.getInt(offset + 104);
      int i27 = _dataBuffer.getInt(offset + 108);
      int i28 = _dataBuffer.getInt(offset + 112);
      int i29 = _dataBuffer.getInt(offset + 116);
      out[outPos] = i0 >>> 2;
      out[outPos + 1] = ((i0 & 0x3) << 28) | (i1 >>> 4);
      out[outPos + 2] = ((i1 & 0xf) << 26) | (i2 >>> 6);
      out[outPos + 3] = ((i2 & 0x3f) << 24) | (i3 >>> 8);
      out[outPos + 4] = ((i3 & 0xff) << 22) | (i4 >>> 10);
      out[outPos + 5] = ((i4 & 0x3ff) << 20) | (i5 >>> 12);
      out[outPos + 6] = ((i5 & 0xfff) << 18) | (i6 >>> 14);
      out[outPos + 7] = ((i6 & 0x3fff) << 16) | (i7 >>> 16);
      out[outPos + 8] = ((i7 & 0xffff) << 14) | (i8 >>> 18);
      out[outPos + 9] = ((i8 & 0x3ffff) << 12) | (i9 >>> 20);
      out[outPos + 10] = ((i9 & 0xfffff) << 10) | (i10 >>> 22);
      out[outPos + 11] = ((i10 & 0x3fffff) << 8) | (i11 >>> 24);
      out[outPos + 12] = ((i11 & 0xffffff) << 6) | (i12 >>> 26);
      out[outPos + 13] = ((i12 & 0x3ffffff) << 4) | (i13 >>> 28);
      out[outPos + 14] = ((i13 & 0xfffffff) << 2) | (i14 >>> 30);
      out[outPos + 15] = i14 & 0x3fffffff;
      out[outPos + 16] = i15 >>> 2;
      out[outPos + 17] = ((i15 & 0x3) << 28) | (i16 >>> 4);
      out[outPos + 18] = ((i16 & 0xf) << 26) | (i17 >>> 6);
      out[outPos + 19] = ((i17 & 0x3f) << 24) | (i18 >>> 8);
      out[outPos + 20] = ((i18 & 0xff) << 22) | (i19 >>> 10);
      out[outPos + 21] = ((i19 & 0x3ff) << 20) | (i20 >>> 12);
      out[outPos + 22] = ((i20 & 0xfff) << 18) | (i21 >>> 14);
      out[outPos + 23] = ((i21 & 0x3fff) << 16) | (i22 >>> 16);
      out[outPos + 24] = ((i22 & 0xffff) << 14) | (i23 >>> 18);
      out[outPos + 25] = ((i23 & 0x3ffff) << 12) | (i24 >>> 20);
      out[outPos + 26] = ((i24 & 0xfffff) << 10) | (i25 >>> 22);
      out[outPos + 27] = ((i25 & 0x3fffff) << 8) | (i26 >>> 24);
      out[outPos + 28] = ((i26 & 0xffffff) << 6) | (i27 >>> 26);
      out[outPos + 29] = ((i27 & 0x3ffffff) << 4) | (i28 >>> 28);
      out[outPos + 30] = ((i28 & 0xfffffff) << 2) | (i29 >>> 30);
      out[outPos + 31] = i29 & 0x3fffffff;
    }
  }

  private static class Bit31Reader extends FixedBitIntReader {

    private Bit31Reader(PinotDataBuffer dataBuffer) {
      super(dataBuffer);
    }

    @Override
    public int read(int index) {
      long bitOffset = (long) index * 31;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      int valueInFirstInt = _dataBuffer.getInt(offset) & (0xffffffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 1;
      if (numBitsLeft <= 0) {
        return valueInFirstInt >>> -numBitsLeft;
      } else {
        return (valueInFirstInt << numBitsLeft) | ((_dataBuffer.getByte(offset + 4) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readAndGetRanges(int index, long baseOffset, List<ForwardIndexByteRange> ranges) {
      long bitOffset = (long) index * 31;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset, Integer.BYTES));
      int valueInFirstInt = _dataBuffer.getInt(offset) & (0xffffffff >>> bitOffsetInFirstByte);
      int numBitsLeft = bitOffsetInFirstByte - 1;
      if (numBitsLeft <= 0) {
        return valueInFirstInt >>> -numBitsLeft;
      } else {
        ranges.add(ForwardIndexByteRange.newByteRange(baseOffset + offset + 4, Byte.BYTES));
        return (valueInFirstInt << numBitsLeft) | ((_dataBuffer.getByte(offset + 4) & 0xff) >>> (8 - numBitsLeft));
      }
    }

    @Override
    public int readUnchecked(int index) {
      long bitOffset = (long) index * 31;
      long offset = bitOffset >>> 3;
      int bitOffsetInFirstByte = (int) bitOffset & 0x7;
      return (int) (_dataBuffer.getLong(offset) >>> (33 - bitOffsetInFirstByte)) & 0x7fffffff;
    }

    @Override
    public void read32(int index, int[] out, int outPos) {
      assert index % 32 == 0;
      long offset = (long) (index >>> 3) * 31;
      int i0 = _dataBuffer.getInt(offset);
      int i1 = _dataBuffer.getInt(offset + 4);
      int i2 = _dataBuffer.getInt(offset + 8);
      int i3 = _dataBuffer.getInt(offset + 12);
      int i4 = _dataBuffer.getInt(offset + 16);
      int i5 = _dataBuffer.getInt(offset + 20);
      int i6 = _dataBuffer.getInt(offset + 24);
      int i7 = _dataBuffer.getInt(offset + 28);
      int i8 = _dataBuffer.getInt(offset + 32);
      int i9 = _dataBuffer.getInt(offset + 36);
      int i10 = _dataBuffer.getInt(offset + 40);
      int i11 = _dataBuffer.getInt(offset + 44);
      int i12 = _dataBuffer.getInt(offset + 48);
      int i13 = _dataBuffer.getInt(offset + 52);
      int i14 = _dataBuffer.getInt(offset + 56);
      int i15 = _dataBuffer.getInt(offset + 60);
      int i16 = _dataBuffer.getInt(offset + 64);
      int i17 = _dataBuffer.getInt(offset + 68);
      int i18 = _dataBuffer.getInt(offset + 72);
      int i19 = _dataBuffer.getInt(offset + 76);
      int i20 = _dataBuffer.getInt(offset + 80);
      int i21 = _dataBuffer.getInt(offset + 84);
      int i22 = _dataBuffer.getInt(offset + 88);
      int i23 = _dataBuffer.getInt(offset + 92);
      int i24 = _dataBuffer.getInt(offset + 96);
      int i25 = _dataBuffer.getInt(offset + 100);
      int i26 = _dataBuffer.getInt(offset + 104);
      int i27 = _dataBuffer.getInt(offset + 108);
      int i28 = _dataBuffer.getInt(offset + 112);
      int i29 = _dataBuffer.getInt(offset + 116);
      int i30 = _dataBuffer.getInt(offset + 120);
      out[outPos] = i0 >>> 1;
      out[outPos + 1] = ((i0 & 0x1) << 30) | (i1 >>> 2);
      out[outPos + 2] = ((i1 & 0x3) << 29) | (i2 >>> 3);
      out[outPos + 3] = ((i2 & 0x7) << 28) | (i3 >>> 4);
      out[outPos + 4] = ((i3 & 0xf) << 27) | (i4 >>> 5);
      out[outPos + 5] = ((i4 & 0x1f) << 26) | (i5 >>> 6);
      out[outPos + 6] = ((i5 & 0x3f) << 25) | (i6 >>> 7);
      out[outPos + 7] = ((i6 & 0x7f) << 24) | (i7 >>> 8);
      out[outPos + 8] = ((i7 & 0xff) << 23) | (i8 >>> 9);
      out[outPos + 9] = ((i8 & 0x1ff) << 22) | (i9 >>> 10);
      out[outPos + 10] = ((i9 & 0x3ff) << 21) | (i10 >>> 11);
      out[outPos + 11] = ((i10 & 0x7ff) << 20) | (i11 >>> 12);
      out[outPos + 12] = ((i11 & 0xfff) << 19) | (i12 >>> 13);
      out[outPos + 13] = ((i12 & 0x1fff) << 18) | (i13 >>> 14);
      out[outPos + 14] = ((i13 & 0x3fff) << 17) | (i14 >>> 15);
      out[outPos + 15] = ((i14 & 0x7fff) << 16) | (i15 >>> 16);
      out[outPos + 16] = ((i15 & 0xffff) << 15) | (i16 >>> 17);
      out[outPos + 17] = ((i16 & 0x1ffff) << 14) | (i17 >>> 18);
      out[outPos + 18] = ((i17 & 0x3ffff) << 13) | (i18 >>> 19);
      out[outPos + 19] = ((i18 & 0x7ffff) << 12) | (i19 >>> 20);
      out[outPos + 20] = ((i19 & 0xfffff) << 11) | (i20 >>> 21);
      out[outPos + 21] = ((i20 & 0x1fffff) << 10) | (i21 >>> 22);
      out[outPos + 22] = ((i21 & 0x3fffff) << 9) | (i22 >>> 23);
      out[outPos + 23] = ((i22 & 0x7fffff) << 8) | (i23 >>> 24);
      out[outPos + 24] = ((i23 & 0xffffff) << 7) | (i24 >>> 25);
      out[outPos + 25] = ((i24 & 0x1ffffff) << 6) | (i25 >>> 26);
      out[outPos + 26] = ((i25 & 0x3ffffff) << 5) | (i26 >>> 27);
      out[outPos + 27] = ((i26 & 0x7ffffff) << 4) | (i27 >>> 28);
      out[outPos + 28] = ((i27 & 0xfffffff) << 3) | (i28 >>> 29);
      out[outPos + 29] = ((i28 & 0x1fffffff) << 2) | (i29 >>> 30);
      out[outPos + 30] = ((i29 & 0x3fffffff) << 1) | (i30 >>> 31);
      out[outPos + 31] = i30 & 0x7fffffff;
    }
  }
}
