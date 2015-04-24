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
import com.linkedin.pinot.core.operator.filter.ScanBasedAndBlock;


public class AndFilterOperatorTest {

  private static final Logger LOGGER = Logger.getLogger(AndFilterOperatorTest.class);
  private static final int TOTAL_TEST_TIMES = 1000;
  private static final int JUMP_VALUE_RANGE = 100;
  public static Random RANDOM = new Random(System.currentTimeMillis());
  public static long TOTAL_DOCS = 10000000L;

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
