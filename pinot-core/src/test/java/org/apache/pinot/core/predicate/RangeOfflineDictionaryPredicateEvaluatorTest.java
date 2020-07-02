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
package org.apache.pinot.core.predicate;

import org.apache.pinot.core.common.predicate.RangePredicate;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.RangePredicateEvaluatorFactory;
import org.apache.pinot.core.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class RangeOfflineDictionaryPredicateEvaluatorTest {
  private static final int DICT_LEN = 10;

  @Test
  public void testRanges() {
    int rangeStart, rangeEnd;
    {
      // [2, 5]
      rangeStart = 2;
      rangeEnd = 5;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, true);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.isAlwaysTrue());
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
      // (2, 5]
      rangeStart = 2;
      rangeEnd = 5;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, false, rangeEnd, true);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.isAlwaysTrue());
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
      // [2, 5)
      rangeStart = 2;
      rangeEnd = 5;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, false);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.isAlwaysTrue());
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
      // (2, 5)
      rangeStart = 2;
      rangeEnd = 5;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, false, rangeEnd, false);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.isAlwaysTrue());
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
      // [0, 5)
      rangeStart = 0;
      rangeEnd = 5;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, false);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.isAlwaysTrue());
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
      // [0, 5]
      rangeStart = 0;
      rangeEnd = 5;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, true);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.isAlwaysTrue());
      Assert.assertTrue(evaluator.applySV(rangeStart));
      Assert.assertTrue(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeEnd + 1));
    }
    {
      // [6, DICT_LEN-1]
      rangeStart = 6;
      rangeEnd = DICT_LEN - 1;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, true);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.isAlwaysTrue());
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
      // (6, DICT_LEN-1]
      rangeStart = 6;
      rangeEnd = DICT_LEN - 1;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, false, rangeEnd, true);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.isAlwaysTrue());
      Assert.assertFalse(evaluator.applySV(rangeStart));
      Assert.assertTrue(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeStart - 1));
    }
    {
      // [0, DICT_LEN-1]
      rangeStart = 0;
      rangeEnd = DICT_LEN - 1;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, true, rangeEnd, true);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertFalse(evaluator.isAlwaysFalse());
      Assert.assertTrue(evaluator.isAlwaysTrue());
      Assert.assertTrue(evaluator.applySV(rangeStart));
      Assert.assertTrue(evaluator.applySV(rangeEnd));
      Assert.assertTrue(evaluator.applySV(rangeStart + 1));
      Assert.assertFalse(evaluator.applySV(rangeStart - 1));
    }
  }

  @Test
  public void testZeroRange() {
    int rangeStart, rangeEnd;
    {
      // (4, 5)
      rangeStart = 4;
      rangeEnd = 5;
      BaseImmutableDictionary reader = createReader(rangeStart, rangeEnd);
      RangePredicate predicate = createPredicate(rangeStart, false, rangeEnd, false);
      PredicateEvaluator evaluator =
          RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator(predicate, reader, DataType.INT);
      Assert.assertTrue(evaluator.isAlwaysFalse());
      Assert.assertFalse(evaluator.isAlwaysTrue());
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

  private BaseImmutableDictionary createReader(int rangeStart, int rangeEnd) {
    BaseImmutableDictionary reader = mock(BaseImmutableDictionary.class);
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
    if (lower == 0 && inclLower) {
      lowerStr = "*";
    }
    String upperStr = "upper";
    if (upper == DICT_LEN - 1 && inclUpper) {
      upperStr = "*";
    }
    when(predicate.getLowerBoundary()).thenReturn(lowerStr);
    when(predicate.getUpperBoundary()).thenReturn(upperStr);
    return predicate;
  }
}
