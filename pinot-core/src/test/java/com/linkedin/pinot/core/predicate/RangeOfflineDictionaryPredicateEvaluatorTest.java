/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.predicate;

import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;
import com.linkedin.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class RangeOfflineDictionaryPredicateEvaluatorTest {
  private static final int DICT_LEN = 10;

  @Test
  public void testRanges() {
    int rangeStart, rangeEnd;
    {
      // (2,5)
      rangeStart = 2;
      rangeEnd = 5;
      ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, true);
      PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertTrue(evaluator.applySV(rangeStart));
      Assert.assertTrue(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeStart - 1));
      Assert.assertFalse(evaluator.applySV(rangeEnd + 1));

      int[] dictIds = new int[]{1, 3, 7};
      Assert.assertTrue(evaluator.applyMV(dictIds, dictIds.length));
      Assert.assertFalse(evaluator.applyMV(dictIds, 1));
      dictIds = evaluator.getMatchingDictIds();
      verifyDictId(dictIds, rangeStart, rangeEnd);
    }
    {
      // [2,5)
      rangeStart = 2;
      rangeEnd = 5;
      ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, false, rangeEnd, true);
      PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.applySV(rangeStart));
      Assert.assertTrue(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeStart - 1));
      Assert.assertFalse(evaluator.applySV(rangeEnd + 1));

      int[] dictIds = new int[]{1, 3, 7};
      Assert.assertTrue(evaluator.applyMV(dictIds, dictIds.length));
      Assert.assertFalse(evaluator.applyMV(dictIds, 1));
      dictIds = evaluator.getMatchingDictIds();
      verifyDictId(dictIds, rangeStart + 1, rangeEnd);
    }
    {
      // (2,5]
      rangeStart = 2;
      rangeEnd = 5;
      ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, false);
      PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertTrue(evaluator.applySV(rangeStart));
      Assert.assertFalse(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeStart - 1));
      Assert.assertFalse(evaluator.applySV(rangeEnd + 1));

      int[] dictIds = new int[]{1, 3, 7};
      Assert.assertTrue(evaluator.applyMV(dictIds, dictIds.length));
      Assert.assertFalse(evaluator.applyMV(dictIds, 1));
      dictIds = evaluator.getMatchingDictIds();
      verifyDictId(dictIds, rangeStart, rangeEnd - 1);
    }
    {
      // [2,5]
      rangeStart = 2;
      rangeEnd = 5;
      ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, false, rangeEnd, false);
      PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.applySV(rangeStart));
      Assert.assertFalse(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeStart - 1));
      Assert.assertFalse(evaluator.applySV(rangeEnd + 1));

      int[] dictIds = new int[]{1, 3, 7};
      Assert.assertTrue(evaluator.applyMV(dictIds, dictIds.length));
      Assert.assertFalse(evaluator.applyMV(dictIds, 1));
      dictIds = evaluator.getMatchingDictIds();
      verifyDictId(dictIds, rangeStart + 1, rangeEnd - 1);
    }
  }

  private void verifyDictId(int[] dictIds, int start, int end) {
    Assert.assertEquals(dictIds.length, end - start + 1);
    for (int dictId : dictIds) {
      Assert.assertEquals(dictId, start++);
    }
  }

  @Test
  public void testBoundaries() {
    int rangeStart, rangeEnd;
    {
      // (0,5]
      rangeStart = 0;
      rangeEnd = 5;
      ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, false);
      PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertTrue(evaluator.applySV(rangeStart));
      Assert.assertFalse(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeEnd + 1));

      int[] dictIds = new int[]{5, 7, 9};
      Assert.assertFalse(evaluator.applyMV(dictIds, dictIds.length));
      Assert.assertFalse(evaluator.applyMV(dictIds, 1));
      dictIds = evaluator.getMatchingDictIds();
      verifyDictId(dictIds, rangeStart, rangeEnd - 1);
    }
    {
      // (0,5)
      rangeStart = 0;
      rangeEnd = 5;
      ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, true);
      PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertTrue(evaluator.applySV(rangeStart));
      Assert.assertTrue(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeEnd + 1));
    }
    {
      // (6, DICT_LEN-1)
      rangeStart = 6;
      rangeEnd = DICT_LEN - 1;
      ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, true);
      PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertTrue(evaluator.applySV(rangeStart));
      Assert.assertTrue(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeStart - 1));

      int[] dictIds = new int[]{5, 7, 9};
      Assert.assertTrue(evaluator.applyMV(dictIds, dictIds.length));
      Assert.assertFalse(evaluator.applyMV(dictIds, 1));
      dictIds = evaluator.getMatchingDictIds();
      verifyDictId(dictIds, rangeStart, rangeEnd);
    }
    {
      // [6, DICT_LEN-1)
      rangeStart = 6;
      rangeEnd = DICT_LEN - 1;
      ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, false, rangeEnd, true);
      PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.applySV(rangeStart));
      Assert.assertTrue(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeStart - 1));
    }
  }

  @Test
  public void testZeroRange() {
    int rangeStart, rangeEnd;
    {
      rangeStart = 4;
      rangeEnd = 5;
      ImmutableDictionaryReader reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, false, rangeEnd, false);
      PredicateEvaluator evaluator = RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader);
      Assert.assertTrue(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.applySV(rangeStart));
      Assert.assertFalse(evaluator.applySV(rangeEnd));
      Assert.assertFalse(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeStart - 1));

      int[] dictIds = new int[]{5, 7, 9};
      Assert.assertFalse(evaluator.applyMV(dictIds, dictIds.length));
      Assert.assertFalse(evaluator.applyMV(dictIds, 1));
      dictIds = evaluator.getMatchingDictIds();
      verifyDictId(dictIds, rangeStart + 1, rangeEnd - 1);
    }
  }

  private ImmutableDictionaryReader createReader(int rangeStart, int rangeEnd) {
    ImmutableDictionaryReader reader = mock(ImmutableDictionaryReader.class);
    when(reader.insertionIndexOf("lower")).thenReturn(rangeStart);
    when(reader.insertionIndexOf("upper")).thenReturn(rangeEnd);
    when(reader.length()).thenReturn(DICT_LEN);
    return reader;
  }

  private RangePredicate createPredicate(int lower, boolean inclLower, int upper, boolean inclUpper) {
    RangePredicate predicate = mock(RangePredicate.class);
    when(predicate.includeLowerBoundary()).thenReturn(inclLower);
    when(predicate.includeUpperBoundary()).thenReturn(inclUpper);
    String lowerStr = "lower";
    if (lower == 0) {
      lowerStr = "*";
    }
    String upperStr = "upper";
    if (upper == DICT_LEN - 1) {
      upperStr = "*";
    }
    when(predicate.getLowerBoundary()).thenReturn(lowerStr);
    when(predicate.getUpperBoundary()).thenReturn(upperStr);
    return predicate;
  }
}
