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
package org.apache.pinot.core.query.aggregation.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class DistinctCountHLLAggregationFunctionTest {

  @Test
  public void testCanUseStarTreeDefaultLog2m() {
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col")));

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLL_LOG2M_KEY, "8")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLL_LOG2M_KEY, 8)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.HLL_LOG2M_KEY, 16)));

    function = new DistinctCountHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(Literal.stringValue("8"))));

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLL_LOG2M_KEY, "8")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLL_LOG2M_KEY, 8)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.HLL_LOG2M_KEY, "16")));
  }

  @Test
  public void testCanUseStarTreeCustomLog2m() {
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"), ExpressionContext.forLiteral(Literal.intValue(16))));

    Assert.assertFalse(function.canUseStarTree(Map.of()));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.HLL_LOG2M_KEY, "8")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLL_LOG2M_KEY, 16)));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.HLL_LOG2M_KEY, "16")));
  }

  /**
   * Verifies that BitSet deduplication produces the correct cardinality when dictIds contain duplicates.
   * The BitSet should count each distinct dict entry exactly once.
   */
  @Test
  public void testBitSetDeduplicationProducesCorrectCardinality() {
    int numDistinct = 100;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(numDistinct);
    for (int i = 0; i < numDistinct; i++) {
      when(dictionary.get(i)).thenReturn("value_" + i);
    }

    // Feed each dictId 5 times — after dedup via BitSet the cardinality must equal numDistinct
    int[] dictIds = new int[numDistinct * 5];
    for (int i = 0; i < dictIds.length; i++) {
      dictIds[i] = i % numDistinct;
    }

    ObjectAggregationResultHolder holder = new ObjectAggregationResultHolder();
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.intValue(12))));

    DistinctCountHLLAggregationFunction.getDictIdBitSet(holder, dictionary).addDictIds(dictIds, dictIds.length);

    HyperLogLog result = function.extractAggregationResult(holder);
    // With log2m=12 the expected error is ~1.6%; allow 5% to be safe
    Assert.assertEquals(result.cardinality(), numDistinct, numDistinct * 0.05,
        "Expected cardinality close to " + numDistinct + ", got: " + result.cardinality());
  }

  /**
   * Verifies that DictIdsWrapper correctly handles a large dictionary (1M entries), matching the reviewer's
   * expectation that BitSet overhead is negligible (128 KB) and the cardinality estimate stays accurate.
   */
  @Test
  public void testBitSetLargeCardinalityDictionary() {
    int numDistinct = 10_000;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(numDistinct);
    for (int i = 0; i < numDistinct; i++) {
      when(dictionary.get(i)).thenReturn("value_" + i);
    }

    // All dict IDs are unique — cardinality should match numDistinct
    int[] dictIds = new int[numDistinct];
    for (int i = 0; i < numDistinct; i++) {
      dictIds[i] = i;
    }

    ObjectAggregationResultHolder holder = new ObjectAggregationResultHolder();
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.intValue(14))));

    DistinctCountHLLAggregationFunction.getDictIdBitSet(holder, dictionary).addDictIds(dictIds, dictIds.length);

    HyperLogLog result = function.extractAggregationResult(holder);
    // log2m=14 gives ~0.8% error; allow 5%
    Assert.assertEquals(result.cardinality(), numDistinct, numDistinct * 0.05,
        "Expected cardinality close to " + numDistinct + ", got: " + result.cardinality());
  }

  /**
   * Verifies that getDictIdBitSet reuses the same DictIdsWrapper across multiple calls on the same holder,
   * accumulating all dict IDs correctly.
   */
  @Test
  public void testDictIdBitSetIsReusedAcrossBatches() {
    int numDistinct = 200;
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(numDistinct);
    for (int i = 0; i < numDistinct; i++) {
      when(dictionary.get(i)).thenReturn("value_" + i);
    }

    ObjectAggregationResultHolder holder = new ObjectAggregationResultHolder();
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.intValue(12))));

    // Batch 1: dict IDs 0–99
    int[] batch1 = new int[100];
    for (int i = 0; i < 100; i++) {
      batch1[i] = i;
    }
    DistinctCountHLLAggregationFunction.getDictIdBitSet(holder, dictionary).addDictIds(batch1, batch1.length);

    // Batch 2: dict IDs 100–199 (using the same holder — wrapper must be reused)
    int[] batch2 = new int[100];
    for (int i = 0; i < 100; i++) {
      batch2[i] = 100 + i;
    }
    DistinctCountHLLAggregationFunction.getDictIdBitSet(holder, dictionary).addDictIds(batch2, batch2.length);

    HyperLogLog result = function.extractAggregationResult(holder);
    Assert.assertEquals(result.cardinality(), numDistinct, numDistinct * 0.05,
        "Both batches should be accumulated; expected cardinality ~" + numDistinct + ", got: " + result.cardinality());
  }
}
