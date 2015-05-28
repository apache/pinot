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

/**
 * Sept 15, 2012
 *
 */
public class HeapCompressedIntArray implements IntArray {
  static final int BLOCK_SIZE = 64; // 32 = int, 64 = long
  static final int BLOCK_BITS = 6; // The #bits representing BLOCK_SIZE
  static final int MOD_MASK = BLOCK_SIZE - 1; // x % BLOCK_SIZE

  /**
   * Values are stores contiguously in the blocks array.
   */
  private final long[] blocks;
  /**
   * A right-aligned mask of width BitsPerValue used by {@link #getInt(int)}.
   */
  private final long maskRight;
  /**
   * Optimization: Saves one lookup in {@link #getInt(int)}.
   */
  private final int bpvMinusBlockSize;
  private final int bitsPerValue;
  private final int valueCount;

  /**
   * Creates an array with the internal structures adjusted for the given limits
   * and initialized to 0.
   *
   * @param valueCount
   *          the number of elements.
   * @param bitsPerValue
   *          the number of bits available for any given value.
   */
  public HeapCompressedIntArray(int valueCount, int bitsPerValue) {
    // NOTE: block-size was previously calculated as
    // valueCount * bitsPerValue / BLOCK_SIZE + 1
    // due to memory layout requirements dictated by non-branching code
    this(new long[size(valueCount, bitsPerValue)], valueCount, bitsPerValue);
  }

  /**
   * <p>Creates an array backed by the given blocks. </p>
   *
   * Note: The blocks are used directly, so changes to the given block will
   * affect the Packed64-structure.
   *
   * @param blocks
   *          used as the internal backing array. Not that the last element
   *          cannot be addressed directly.
   * @param valueCount
   *          the number of values.
   * @param bitsPerValue
   *          the number of bits available for any given value.
   */
  public HeapCompressedIntArray(long[] blocks, int valueCount, int bitsPerValue) {
    this.blocks = blocks;
    this.valueCount = valueCount;
    this.bitsPerValue = bitsPerValue;
    maskRight = ~0L << (BLOCK_SIZE - bitsPerValue) >>> (BLOCK_SIZE - bitsPerValue);
    bpvMinusBlockSize = bitsPerValue - BLOCK_SIZE;
  }

  public int size() {
    return valueCount;
  }

  public static int size(int valueCount, int bitsPerValue) {
    final long totBitCount = (long) valueCount * bitsPerValue;
    return (int) (totBitCount / 64 + ((totBitCount % 64 == 0) ? 0 : 1));
  }

  /**
   * @param index
   *          the position of the value.
   * @return the value at the given index.
   */
  public int getInt(final int index) {
    // The abstract index in a bit stream
    final long majorBitPos = (long) index * bitsPerValue;
    // The index in the backing long-array
    final int elementPos = (int) (majorBitPos >>> BLOCK_BITS);
    // The number of value-bits in the second long
    final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

    if (endBits <= 0) { // Single block
      return (int) ((blocks[elementPos] >>> -endBits) & maskRight);
    }
    // Two blocks
    return (int) (((blocks[elementPos] << endBits) | (blocks[elementPos + 1] >>> (BLOCK_SIZE - endBits))) & maskRight);
  }

  public void setInt(final int index, final int val) {
    final long value = val;
    // The abstract index in a contiguous bit stream
    final long majorBitPos = (long) index * bitsPerValue;
    // The index in the backing long-array
    final int elementPos = (int) (majorBitPos >>> BLOCK_BITS); // / BLOCK_SIZE
    // The number of value-bits in the second long
    final long endBits = (majorBitPos & MOD_MASK) + bpvMinusBlockSize;

    if (endBits <= 0) { // Single block
      blocks[elementPos] = blocks[elementPos] & ~(maskRight << -endBits) | (value << -endBits);
      return;
    }
    // Two blocks
    blocks[elementPos] = blocks[elementPos] & ~(maskRight >>> endBits) | (value >>> endBits);
    blocks[elementPos + 1] = blocks[elementPos + 1] & (~0L >>> endBits) | (value << (BLOCK_SIZE - endBits));
  }

  public long[] getBlocks() {
    return blocks;
  }

  public int getBitsPerValue() {
    return bitsPerValue;
  }

  public int getApproximateSizeInBits() {
    int bits = blocks.length * 64;
    return bits;
  }

}
