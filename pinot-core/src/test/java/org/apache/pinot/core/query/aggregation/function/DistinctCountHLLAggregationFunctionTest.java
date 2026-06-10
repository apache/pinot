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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.UuidUtils;
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

  /**
   * Regression: UUID columns have storedType=BYTES, but a UUID value is a logical scalar, not a serialized
   * HyperLogLog. The aggregator must route UUID columns through the same content-hash path as STRING (offering
   * canonical UUID strings) instead of trying to deserialize each 16-byte value as an HLL.
   */
  @Test
  public void testAggregateOnUuidColumnOffersCanonicalStringsAndProducesExactDistinctCount() {
    ExpressionContext expression = RequestContextUtils.getExpression("uuidCol");
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(List.of(expression));

    // Three distinct UUIDs across six rows; the same UUID repeats twice on rows 0/3, 1/4, 2/5.
    String[] uuidStrings = new String[]{
        "550e8400-e29b-41d4-a716-446655440000",
        "12345678-1234-1234-1234-1234567890ab",
        "9c5e1f24-0b8e-4c9d-87f1-0aa64a3b9d12",
        "550e8400-e29b-41d4-a716-446655440000",
        "12345678-1234-1234-1234-1234567890ab",
        "9c5e1f24-0b8e-4c9d-87f1-0aa64a3b9d12"
    };

    // Stub the BYTES fetch (raw 16-byte values) — the production path converts bytes to canonical strings
    // itself because ProjectionBlockValSet.getStringValuesSV() would render stored BYTES as bare hex.
    byte[][] uuidBytes = new byte[uuidStrings.length][];
    for (int i = 0; i < uuidStrings.length; i++) {
      uuidBytes[i] = UuidUtils.toBytes(uuidStrings[i]);
    }
    BlockValSet uuidBlockValSet = mock(BlockValSet.class);
    when(uuidBlockValSet.getValueType()).thenReturn(DataType.UUID);
    when(uuidBlockValSet.getBytesValuesSV()).thenReturn(uuidBytes);
    when(uuidBlockValSet.isSingleValue()).thenReturn(true);
    when(uuidBlockValSet.getDictionary()).thenReturn(null);

    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    blockValSetMap.put(expression, uuidBlockValSet);

    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(uuidStrings.length, resultHolder, blockValSetMap);

    Object intermediate = function.extractAggregationResult(resultHolder);
    Assert.assertTrue(intermediate instanceof HyperLogLog,
        "Intermediate result must be a HyperLogLog, not a dictionary bitmap");
    long cardinality = ((HyperLogLog) intermediate).cardinality();
    Assert.assertEquals(cardinality, 3L,
        "HLL cardinality must equal the 3 distinct UUIDs; got " + cardinality);
  }

  /**
   * Cross-type consistency: DISTINCTCOUNTHLL(uuidCol) must produce the same HLL cardinality as
   * DISTINCTCOUNTHLL(stringRepresentationOfSameUuids). Locks in the design contract that UUID columns are
   * hashed as canonical UUID strings.
   */
  @Test
  public void testUuidDistinctCountHllMatchesStringDistinctCountHll() {
    String[] uuidStrings = new String[]{
        "550e8400-e29b-41d4-a716-446655440000",
        "12345678-1234-1234-1234-1234567890ab",
        "9c5e1f24-0b8e-4c9d-87f1-0aa64a3b9d12"
    };

    long uuidHllCardinality = computeHllCardinality(uuidStrings, DataType.UUID);
    long stringHllCardinality = computeHllCardinality(uuidStrings, DataType.STRING);

    Assert.assertEquals(uuidHllCardinality, stringHllCardinality,
        "DISTINCTCOUNTHLL(uuidCol) must match DISTINCTCOUNTHLL(CAST(uuidCol AS STRING))");
  }

  private long computeHllCardinality(String[] values, DataType valueType) {
    ExpressionContext expression = RequestContextUtils.getExpression("col");
    DistinctCountHLLAggregationFunction function = new DistinctCountHLLAggregationFunction(List.of(expression));

    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(valueType);
    if (valueType == DataType.UUID) {
      // UUID path fetches raw bytes and converts to canonical form itself (projection string fetch returns hex)
      byte[][] uuidBytes = new byte[values.length][];
      for (int i = 0; i < values.length; i++) {
        uuidBytes[i] = UuidUtils.toBytes(values[i]);
      }
      when(blockValSet.getBytesValuesSV()).thenReturn(uuidBytes);
    } else {
      when(blockValSet.getStringValuesSV()).thenReturn(values);
    }
    when(blockValSet.isSingleValue()).thenReturn(true);
    when(blockValSet.getDictionary()).thenReturn(null);

    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    blockValSetMap.put(expression, blockValSet);

    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(values.length, resultHolder, blockValSetMap);
    Object intermediate = function.extractAggregationResult(resultHolder);
    return ((HyperLogLog) intermediate).cardinality();
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

    BitSet bitSet = DistinctCountHLLAggregationFunction.getDictIdBitSet(holder, dictionary);
    for (int dictId : dictIds) {
      bitSet.set(dictId);
    }

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

    BitSet bitSet = DistinctCountHLLAggregationFunction.getDictIdBitSet(holder, dictionary);
    for (int dictId : dictIds) {
      bitSet.set(dictId);
    }

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
    BitSet bitSet = DistinctCountHLLAggregationFunction.getDictIdBitSet(holder, dictionary);
    for (int dictId : batch1) {
      bitSet.set(dictId);
    }

    // Batch 2: dict IDs 100–199 (using the same holder — wrapper must be reused, same BitSet instance)
    int[] batch2 = new int[100];
    for (int i = 0; i < 100; i++) {
      batch2[i] = 100 + i;
    }
    BitSet bitSet2 = DistinctCountHLLAggregationFunction.getDictIdBitSet(holder, dictionary);
    Assert.assertSame(bitSet, bitSet2, "getDictIdBitSet must return the same BitSet on subsequent calls");
    for (int dictId : batch2) {
      bitSet2.set(dictId);
    }

    HyperLogLog result = function.extractAggregationResult(holder);
    Assert.assertEquals(result.cardinality(), numDistinct, numDistinct * 0.05,
        "Both batches should be accumulated; expected cardinality ~" + numDistinct + ", got: " + result.cardinality());
  }
}
