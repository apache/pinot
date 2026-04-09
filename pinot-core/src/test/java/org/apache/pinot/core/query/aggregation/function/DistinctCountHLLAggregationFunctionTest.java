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
  public void testDictSizeThresholdDefaults() {
    // Default: no threshold arg → DEFAULT_DICT_SIZE_THRESHOLD
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col")));
    int defaultThreshold = DistinctCountHLLAggregationFunction.DEFAULT_DICT_SIZE_THRESHOLD;
    Assert.assertEquals(function.getDictSizeThreshold(), defaultThreshold);

    // Explicit log2m, no threshold → still default
    function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"), ExpressionContext.forLiteral(Literal.intValue(12))));
    Assert.assertEquals(function.getDictSizeThreshold(), defaultThreshold);
  }

  @Test
  public void testDictSizeThresholdCustomValue() {
    // Explicit threshold
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.intValue(12)),
            ExpressionContext.forLiteral(Literal.intValue(50_000))));
    Assert.assertEquals(function.getLog2m(), 12);
    Assert.assertEquals(function.getDictSizeThreshold(), 50_000);
  }

  @Test
  public void testDictSizeThresholdNonPositiveDisablesOptimization() {
    // Non-positive threshold → Integer.MAX_VALUE (optimization disabled)
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.intValue(12)),
            ExpressionContext.forLiteral(Literal.intValue(0))));
    Assert.assertEquals(function.getDictSizeThreshold(), Integer.MAX_VALUE);

    function = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.intValue(12)),
            ExpressionContext.forLiteral(Literal.intValue(-1))));
    Assert.assertEquals(function.getDictSizeThreshold(), Integer.MAX_VALUE);
  }

  @Test
  public void testHighCardinalityDictBypassesBitmapAndProducesApproximateResult() {
    // Verify both the bitmap dedup path and the direct-HLL path produce approximately
    // the same cardinality estimate for dictionary-encoded columns.
    int numDistinct = 1000;

    // Build a Mockito dictionary stub: length() returns numDistinct, get(i) returns "value_i"
    Dictionary dictionary = mock(Dictionary.class);
    when(dictionary.length()).thenReturn(numDistinct);
    for (int i = 0; i < numDistinct; i++) {
      when(dictionary.get(i)).thenReturn("value_" + i);
    }

    // dictIds with 5x repetition — verifies duplicate handling works for both paths
    int[] dictIds = new int[numDistinct * 5];
    for (int i = 0; i < dictIds.length; i++) {
      dictIds[i] = i % numDistinct;
    }

    ObjectAggregationResultHolder bitmapHolder = new ObjectAggregationResultHolder();
    ObjectAggregationResultHolder directHolder = new ObjectAggregationResultHolder();

    // Bitmap path: use threshold above dict size so bitmap path is chosen
    DistinctCountHLLAggregationFunction bitmapFunction = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.intValue(12)),
            ExpressionContext.forLiteral(Literal.intValue(Integer.MAX_VALUE))));
    DistinctCountHLLAggregationFunction.getDictIdBitmap(bitmapHolder, dictionary).addN(dictIds, 0, dictIds.length);

    // Direct HLL path: use threshold=1 so dict size (1000) always exceeds it
    DistinctCountHLLAggregationFunction directHllFunction = new DistinctCountHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(Literal.intValue(12)),
            ExpressionContext.forLiteral(Literal.intValue(1))));
    HyperLogLog directHll = directHllFunction.getHyperLogLog(directHolder);
    for (int dictId : dictIds) {
      directHll.offer(dictionary.get(dictId));
    }

    long bitmapCardinality = bitmapFunction.extractAggregationResult(bitmapHolder).cardinality();
    long directCardinality = directHllFunction.extractAggregationResult(directHolder).cardinality();

    // Both paths should give approximately the same cardinality (within 5%)
    Assert.assertEquals(bitmapCardinality, numDistinct, numDistinct * 0.05,
        "Bitmap path cardinality should be close to " + numDistinct);
    Assert.assertEquals(directCardinality, numDistinct, numDistinct * 0.05,
        "Direct HLL path cardinality should be close to " + numDistinct);
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
}
