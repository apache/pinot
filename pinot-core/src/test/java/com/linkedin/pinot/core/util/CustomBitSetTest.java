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
import java.util.BitSet;
import java.util.Iterator;
import java.util.Random;
import java.util.TreeSet;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CustomBitSetTest {
  @Test
  public void testSetBit() {
    final int LENGTH = 256;
    CustomBitSet customBitSet = CustomBitSet.withBitLength(LENGTH);
    for (int i = 0; i < LENGTH; i++) {
      if (i % 2 == 0) {
        customBitSet.setBit(i);
      }
    }
    for (int i = 0; i < LENGTH; i++) {
      if (i % 2 == 0) {
        Assert.assertTrue(customBitSet.isBitSet(i));
      } else {
        Assert.assertFalse(customBitSet.isBitSet(i));
      }
    }
  }

  @Test
  public void testFindNthBitSet() {
    final int LENGTH = 256;
    CustomBitSet customBitSet = CustomBitSet.withBitLength(LENGTH);
    customBitSet.setBit(0);
    customBitSet.setBit(100);
    customBitSet.setBit(250);
    System.out.println(customBitSet.findNthBitSetAfter(0, 1));
    System.out.println(customBitSet.findNthBitSetAfter(100, 1));

  }

  @Test
  public void testTurnOffNthLeftmostBits() {
    for(int value = 0; value < 256; ++value) {
      for(int bitsToTurnOff = 0; bitsToTurnOff < 8; ++bitsToTurnOff) {
        byte[] values = new byte[1];
        values[0] = (byte) value;
        BitSet bitSet = BitSet.valueOf(values);

        // Turn off the bits in the bitSet
        int bitsToTurnOffInBitSet = bitsToTurnOff;
        while (0 < bitsToTurnOffInBitSet && !bitSet.isEmpty()) {
          bitSet.flip(bitSet.previousSetBit(7));
          bitsToTurnOffInBitSet--;
        }

        values = bitSet.toByteArray();
        int actual = BitUtils.turnOffNthLeftmostSetBits(value, bitsToTurnOff);
        if (values.length == 0) {
          Assert.assertEquals(actual, 0, "Value " + Integer.toBinaryString(value) + " with " + bitsToTurnOff +
              " bits to turn off => " + Integer.toBinaryString(actual));
        } else {
          Assert.assertEquals(actual, values[0] & 0xFF, "Value " + Integer.toBinaryString(value) + " with " +
              bitsToTurnOff + " bits to turn off => " + Integer.toBinaryString(actual));
        }
      }
    }
  }

  @Test
  public void testFindNthBitSetRandom() {
    final int ITERATIONS = 100000;
    final int LENGTH = 256;
    final int MAX_BITS_ON = 32;

    Random random = new Random();
    for (int i = 0; i < ITERATIONS; i++) {
      CustomBitSet customBitSet = CustomBitSet.withBitLength(LENGTH);
      TreeSet<Integer> bitsOn = new TreeSet<Integer>();
      int bitsToTurnOn = random.nextInt(MAX_BITS_ON);
      while (bitsOn.size() < bitsToTurnOn) {
        bitsOn.add(random.nextInt(LENGTH));
      }
      TreeSet<Integer> bitsOnCopy = new TreeSet<Integer>(bitsOn);

      for (Integer indexOfBitToTurnOn : bitsOn) {
        customBitSet.setBit(indexOfBitToTurnOn);
      }

      int startSearchIndex = random.nextInt(LENGTH);

      // Discard all bits before or at the search index
      Iterator<Integer> bitsOnIterator = bitsOn.iterator();
      while (bitsOnIterator.hasNext()) {
        Integer next = bitsOnIterator.next();
        if (next <= startSearchIndex) {
          bitsOnIterator.remove();
        }
      }

      // Discard all bits set on before the search ahead limit
      int nthBitToFind = random.nextInt(MAX_BITS_ON / 2) + 1;
      for (int j = 0; j < nthBitToFind - 1 && !bitsOn.isEmpty(); j++) {
        bitsOn.pollFirst();
      }

      // Check the result against the expected index
      int expectedIndex;
      if (bitsOn.isEmpty()) {
        expectedIndex = -1;
      } else {
        expectedIndex = bitsOn.pollFirst();
      }

      long nthBitSetAfter = customBitSet.findNthBitSetAfter(startSearchIndex, nthBitToFind);
      if (nthBitSetAfter != expectedIndex) {
        System.out.println("Bits set " + bitsOnCopy + ", searching for " + nthBitToFind + "th bit from " + startSearchIndex);
        nthBitSetAfter = customBitSet.findNthBitSetAfter(startSearchIndex, nthBitToFind);
      }
      Assert.assertEquals(nthBitSetAfter, expectedIndex, "Bits set " +
          bitsOnCopy + ", searching for " + nthBitToFind + "th bit from " + startSearchIndex);
    }
  }

  @Test
  public void testNthBit() {
    final int LENGTH = 256;
    for (int setBitIndex = 0; setBitIndex < LENGTH; ++setBitIndex) {
      for (int searchStartIndex = 0; searchStartIndex < LENGTH; ++searchStartIndex) {
        CustomBitSet customBitSet = CustomBitSet.withBitLength(LENGTH);
        customBitSet.setBit(setBitIndex);
        long foundSetBitIndex = customBitSet.nextSetBit(searchStartIndex);
        if (searchStartIndex <= setBitIndex) {
          Assert.assertEquals(foundSetBitIndex, setBitIndex, "Found bit at index " + foundSetBitIndex
              + " while it was set at " + setBitIndex + " searching from " + searchStartIndex);
        } else {
          Assert.assertEquals(foundSetBitIndex, -1, "Found bit at index " + foundSetBitIndex + " while it was set at "
              + setBitIndex + " searching from " + searchStartIndex);
        }
      }
    }
  }

  @Test
  public void testNthBitWithConfusingBit() {
    final int LENGTH = 256;
    final int CONFUSING_BIT_RANGE = 8;
    for (int setBitIndex = 0; setBitIndex < LENGTH - CONFUSING_BIT_RANGE; ++setBitIndex) {
      for (int confusingBitOffset = 1; confusingBitOffset < CONFUSING_BIT_RANGE; ++confusingBitOffset) {
        for (int searchStartIndex = 0; searchStartIndex < LENGTH; ++searchStartIndex) {
          int confusingBitIndex = setBitIndex + confusingBitOffset;
          CustomBitSet customBitSet = CustomBitSet.withBitLength(LENGTH);
          customBitSet.setBit(setBitIndex);
          customBitSet.setBit(confusingBitIndex);
          long foundSetBitIndex = customBitSet.nextSetBit(searchStartIndex);
          if (searchStartIndex <= setBitIndex) {
            Assert.assertEquals(foundSetBitIndex, setBitIndex, "Found bit at index " + foundSetBitIndex
                + " while it was set at " + setBitIndex + " searching from " + searchStartIndex);
          } else if (searchStartIndex <= confusingBitIndex) {
            Assert.assertEquals(foundSetBitIndex, confusingBitIndex, "Found bit at index " + foundSetBitIndex
                + " while it was set at " + confusingBitIndex + " searching from " + searchStartIndex);
          } else {
            Assert.assertEquals(foundSetBitIndex, -1, "Found bit at index " + foundSetBitIndex
                + " while it was set at " + setBitIndex + " searching from " + searchStartIndex);
          }
        }
      }
    }
  }

  @Test
  public void testNthBitFixed() {
    final int LENGTH = 256;
    int setBitIndex = 8;
    int searchStartIndex = 1;

    CustomBitSet customBitSet = CustomBitSet.withBitLength(LENGTH);
    customBitSet.setBit(setBitIndex);
    long foundSetBitIndex = customBitSet.nextSetBit(searchStartIndex);
    if (searchStartIndex <= setBitIndex) {
      Assert.assertEquals(foundSetBitIndex, setBitIndex, "Found bit at index " + foundSetBitIndex
          + " while it was set at " + setBitIndex + " searching from " + searchStartIndex);
    } else {
      Assert.assertEquals(foundSetBitIndex, -1, "Found bit at index " + foundSetBitIndex + " while it was set at "
          + setBitIndex + " searching from " + searchStartIndex);
    }
  }
}
