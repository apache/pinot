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
  public void testNthBit() {
    final int LENGTH = 256;
    for (int setBitIndex = 0; setBitIndex < LENGTH; ++setBitIndex) {
      for (int searchStartIndex = 0; searchStartIndex < LENGTH; ++searchStartIndex) {
        CustomBitSet customBitSet = CustomBitSet.withBitLength(LENGTH);
        customBitSet.setBit(setBitIndex);
        int foundSetBitIndex = customBitSet.nextSetBit(searchStartIndex);
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
          int foundSetBitIndex = customBitSet.nextSetBit(searchStartIndex);
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
    int foundSetBitIndex = customBitSet.nextSetBit(searchStartIndex);
    if (searchStartIndex <= setBitIndex) {
      Assert.assertEquals(foundSetBitIndex, setBitIndex, "Found bit at index " + foundSetBitIndex
          + " while it was set at " + setBitIndex + " searching from " + searchStartIndex);
    } else {
      Assert.assertEquals(foundSetBitIndex, -1, "Found bit at index " + foundSetBitIndex + " while it was set at "
          + setBitIndex + " searching from " + searchStartIndex);
    }
  }
}
