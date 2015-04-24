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
package com.linkedin.pinot.operator;

import java.util.Arrays;
import java.util.Random;

import org.apache.log4j.Logger;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockDocIdValueSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMetadata;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.operator.filter.BitmapBasedAndBlock;
import com.linkedin.pinot.core.operator.filter.BitmapBasedBlockDocIdSet;
import com.linkedin.pinot.core.operator.filter.BitmapBasedOrBlock;
import com.linkedin.pinot.core.operator.filter.ScanBasedAndBlock;
import com.linkedin.pinot.core.operator.filter.ScanBasedOrBlock;


public class AndOrFilterOperatorTest {

  private static final Logger LOGGER = Logger.getLogger(AndOrFilterOperatorTest.class);
  private static final int TOTAL_TEST_TIMES = 100;
  private static final int JUMP_VALUE_RANGE = 100;
  public static Random RANDOM = new Random(System.currentTimeMillis());
  public static long TOTAL_DOCS = 1000000L;

  @Test
  public void testScanOnlyAndOperator() {
    for (int testId = 0; testId < TOTAL_TEST_TIMES; ++testId) {
      LOGGER.info("Test id - " + testId);
      int numberOfFilters = RANDOM.nextInt(5) + 2;
      Block[] scanOnlyBlocks = new Block[numberOfFilters];
      long[] jumpValues = new long[numberOfFilters];
      for (int i = 0; i < numberOfFilters; ++i) {
        jumpValues[i] = RANDOM.nextInt(JUMP_VALUE_RANGE) + 1;
        scanOnlyBlocks[i] = getJumpValueBlock(TOTAL_DOCS, jumpValues[i]);
      }
      long lcm = lcm(jumpValues);
      LOGGER.info("All jump values = " + Arrays.toString(jumpValues));
      LOGGER.info("Lowest Common Multiplier = " + lcm);
      ScanBasedAndBlock scanBasedAndBlock = new ScanBasedAndBlock(scanOnlyBlocks);

      BlockDocIdIterator scanBasedAndBlockDocIdIterator = scanBasedAndBlock.getBlockDocIdSet().iterator();
      int docId;
      int expectedDocId = 0;
      while ((docId = scanBasedAndBlockDocIdIterator.next()) != Constants.EOF) {
        Assert.assertEquals(docId, expectedDocId);
        expectedDocId += lcm;
      }
    }
  }

  @Test
  public void testHybridAndOperator() {
    for (int testId = 0; testId < TOTAL_TEST_TIMES; ++testId) {
      LOGGER.info("Test id - " + testId);
      int numberOfScanFilters = RANDOM.nextInt(5) + 2;
      Block[] scanOnlyBlocks = new Block[numberOfScanFilters + 1];
      long[] scanOnlyJumpValues = new long[numberOfScanFilters];
      for (int i = 0; i < numberOfScanFilters; ++i) {
        scanOnlyJumpValues[i] = RANDOM.nextInt(JUMP_VALUE_RANGE) + 1;
        scanOnlyBlocks[i + 1] = getJumpValueBlock(TOTAL_DOCS, scanOnlyJumpValues[i]);
      }
      int numberOfBitmapFilters = RANDOM.nextInt(5) + 2;
      Block[] bitmapOnlyBlocks = new Block[numberOfBitmapFilters];
      long[] bitmapJumpValues = new long[numberOfBitmapFilters];
      for (int i = 0; i < numberOfBitmapFilters; ++i) {
        bitmapJumpValues[i] = RANDOM.nextInt(JUMP_VALUE_RANGE) + 1;
        bitmapOnlyBlocks[i] = getBitmapBlock(TOTAL_DOCS, bitmapJumpValues[i]);
      }
      long lcm1 = lcm(scanOnlyJumpValues);
      long lcm2 = lcm(bitmapJumpValues);
      long lcm = lcm(new long[] { lcm1, lcm2 });
      LOGGER.info("All jump values = " + Arrays.toString(scanOnlyJumpValues) + " , " + Arrays.toString(bitmapJumpValues));
      LOGGER.info("Lowest Common Multiplier = " + lcm);
      BitmapBasedAndBlock bitmapBasedAndBlock = new BitmapBasedAndBlock(bitmapOnlyBlocks);
      scanOnlyBlocks[0] = bitmapBasedAndBlock;
      ScanBasedAndBlock scanBasedAndBlock = new ScanBasedAndBlock(scanOnlyBlocks);
      BlockDocIdIterator scanBasedAndBlockDocIdIterator = scanBasedAndBlock.getBlockDocIdSet().iterator();
      int docId;
      int expectedDocId = 0;
      while ((docId = scanBasedAndBlockDocIdIterator.next()) != Constants.EOF) {
        Assert.assertEquals(docId, expectedDocId);
        expectedDocId += lcm;
      }
    }
  }

  @Test
  public void testScanOnlyOrOperator() {
    for (int testId = 0; testId < TOTAL_TEST_TIMES; ++testId) {
      LOGGER.info("Test id - " + testId);
      int numberOfFilters = RANDOM.nextInt(5) + 2;
      Block[] scanOnlyBlocks = new Block[numberOfFilters];
      long[] jumpValues = new long[numberOfFilters];
      for (int i = 0; i < numberOfFilters; ++i) {
        jumpValues[i] = RANDOM.nextInt(JUMP_VALUE_RANGE) + 1;
        scanOnlyBlocks[i] = getJumpValueBlock(TOTAL_DOCS, jumpValues[i]);
      }
      LOGGER.info("All jump values = " + Arrays.toString(jumpValues));
      ScanBasedOrBlock scanBasedOrBlock = new ScanBasedOrBlock(scanOnlyBlocks);

      BlockDocIdIterator scanBasedOrBlockDocIdIterator = scanBasedOrBlock.getBlockDocIdSet().iterator();
      int docId;
      int expectedDocId = 0;
      // List<Integer> scannedDocs = new ArrayList<Integer>();
      while ((docId = scanBasedOrBlockDocIdIterator.next()) != Constants.EOF) {
        // LOGGER.info("Trying to assert docId : " + docId + ", expectId : " + expectedDocId);
        Assert.assertEquals(docId, expectedDocId);
        boolean foundNextExpectedDoc = false;
        while (!foundNextExpectedDoc) {
          expectedDocId++;
          foundNextExpectedDoc = false;
          for (long jumpValue : jumpValues) {
            if (expectedDocId % jumpValue == 0) {
              foundNextExpectedDoc = true;
            }
          }
        }
        // scannedDocs.add(docId);
      }
      // LOGGER.info("Scanned docs : " + Arrays.toString(scannedDocs.toArray(new Integer[0])));
    }
  }

  @Test
  public void testHybridOrOperator() {
    for (int testId = 0; testId < TOTAL_TEST_TIMES; ++testId) {
      LOGGER.info("Test id - " + testId);
      int numberOfScanFilters = RANDOM.nextInt(5) + 2;
      Block[] scanOnlyBlocks = new Block[numberOfScanFilters + 1];
      long[] scanOnlyJumpValues = new long[numberOfScanFilters];
      for (int i = 0; i < numberOfScanFilters; ++i) {
        scanOnlyJumpValues[i] = RANDOM.nextInt(JUMP_VALUE_RANGE) + 1;
        scanOnlyBlocks[i + 1] = getJumpValueBlock(TOTAL_DOCS, scanOnlyJumpValues[i]);
      }
      int numberOfBitmapFilters = RANDOM.nextInt(5) + 2;
      Block[] bitmapOnlyBlocks = new Block[numberOfBitmapFilters];
      long[] bitmapJumpValues = new long[numberOfBitmapFilters];
      for (int i = 0; i < numberOfBitmapFilters; ++i) {
        bitmapJumpValues[i] = RANDOM.nextInt(JUMP_VALUE_RANGE) + 1;
        bitmapOnlyBlocks[i] = getBitmapBlock(TOTAL_DOCS, bitmapJumpValues[i]);
      }
      LOGGER.info("All jump values = " + Arrays.toString(scanOnlyJumpValues) + " , " + Arrays.toString(bitmapJumpValues));

      BitmapBasedOrBlock bitmapBasedOrBlock = new BitmapBasedOrBlock(bitmapOnlyBlocks);
      scanOnlyBlocks[0] = bitmapBasedOrBlock;
      ScanBasedOrBlock scanBasedOrBlock = new ScanBasedOrBlock(scanOnlyBlocks);
      BlockDocIdIterator scanBasedOrBlockDocIdIterator = scanBasedOrBlock.getBlockDocIdSet().iterator();
      int docId;
      int expectedDocId = 0;
      // List<Integer> scannedDocs = new ArrayList<Integer>();
      while ((docId = scanBasedOrBlockDocIdIterator.next()) != Constants.EOF) {
        // LOGGER.info("Trying to assert docId : " + docId + ", expectId : " + expectedDocId);
        Assert.assertEquals(docId, expectedDocId);
        boolean foundNextExpectedDoc = false;
        while (!foundNextExpectedDoc) {
          expectedDocId++;
          foundNextExpectedDoc = false;
          for (long jumpValue : scanOnlyJumpValues) {
            if (expectedDocId % jumpValue == 0) {
              foundNextExpectedDoc = true;
            }
          }
          for (long jumpValue : bitmapJumpValues) {
            if (expectedDocId % jumpValue == 0) {
              foundNextExpectedDoc = true;
            }
          }
        }
        // scannedDocs.add(docId);
      }
      // LOGGER.info("Scanned docs : " + Arrays.toString(scannedDocs.toArray(new Integer[0])));
    }
  }

  private Block getBitmapBlock(final long totalDocs, final long jumpValues) {
    return new Block() {

      @Override
      public BlockMetadata getMetadata() {
        throw new UnsupportedOperationException("Not supported getMetadata()");
      }

      @Override
      public BlockId getId() {
        throw new UnsupportedOperationException("Not supported getId()");
      }

      @Override
      public BlockValSet getBlockValueSet() {
        throw new UnsupportedOperationException("Not supported getBlockValueSet()");
      }

      @Override
      public BlockDocIdValueSet getBlockDocIdValueSet() {
        throw new UnsupportedOperationException("Not supported getBlockDocIdValueSet()");
      }

      @Override
      public BlockDocIdSet getBlockDocIdSet() {
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        int idx = 0;
        while (idx < totalDocs) {
          bitmap.add(idx);
          idx += jumpValues;
        }
        return new BitmapBasedBlockDocIdSet(bitmap);
      }

      @Override
      public boolean applyPredicate(Predicate predicate) {
        return false;
      }
    };
  }

  private Block getJumpValueBlock(final long totalDocs, final long jumpValues) {
    return new Block() {

      @Override
      public BlockMetadata getMetadata() {
        throw new UnsupportedOperationException("Not supported getMetadata()");
      }

      @Override
      public BlockId getId() {
        throw new UnsupportedOperationException("Not supported getId()");
      }

      @Override
      public BlockValSet getBlockValueSet() {
        throw new UnsupportedOperationException("Not supported getBlockValueSet()");
      }

      @Override
      public BlockDocIdValueSet getBlockDocIdValueSet() {
        throw new UnsupportedOperationException("Not supported getBlockDocIdValueSet()");
      }

      @Override
      public BlockDocIdSet getBlockDocIdSet() {
        return new BlockDocIdSet() {
          @Override
          public BlockDocIdIterator iterator() {
            return new BlockDocIdIterator() {
              int _currentDocId = 0;

              @Override
              public int skipTo(int targetDocId) {
                _currentDocId = (int) ((Math.floor((double) (targetDocId - 1.0) / (double) jumpValues)) * jumpValues);
                return next();
              }

              @Override
              public int next() {
                _currentDocId += jumpValues;
                if (_currentDocId < totalDocs) {
                  return _currentDocId;
                } else {
                  return Constants.EOF;
                }
              }

              @Override
              public int currentDocId() {
                return _currentDocId;
              }
            };
          }

          @Override
          public Object getRaw() {
            throw new UnsupportedOperationException("Not supported getRaw()");
          }
        };
      }

      @Override
      public boolean applyPredicate(Predicate predicate) {
        return false;
      }
    };
  }

  private static long lcm(long a, long b) {
    return a * (b / gcd(a, b));
  }

  private static long lcm(long[] input) {
    long result = input[0];
    for (int i = 1; i < input.length; i++) {
      result = lcm(result, input[i]);
    }
    return result;
  }

  private static long gcd(long a, long b) {
    while (b > 0) {
      long temp = b;
      b = a % b; // % is remainder
      a = temp;
    }
    return a;
  }
}
