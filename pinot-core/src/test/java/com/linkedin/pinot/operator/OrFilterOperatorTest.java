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
import com.linkedin.pinot.core.operator.filter.ScanBasedOrBlock;


public class OrFilterOperatorTest {

  private static final Logger LOGGER = Logger.getLogger(OrFilterOperatorTest.class);
  private static final int TOTAL_TEST_TIMES = 1000;
  private static final int JUMP_VALUE_RANGE = 100;
  public static Random RANDOM = new Random(System.currentTimeMillis());
  public static long TOTAL_DOCS = 100000L;

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

}
