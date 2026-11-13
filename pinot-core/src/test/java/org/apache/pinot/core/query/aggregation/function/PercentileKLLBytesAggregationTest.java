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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Checks that KLL aggregation only deserializes included, non-null BYTES rows.
/// Test state is local to each invocation, so these tests can run concurrently.
public class PercentileKLLBytesAggregationTest {
  private static final ExpressionContext COLUMN = ExpressionContext.forIdentifier("column");

  @DataProvider
  public Object[][] bytesInputs() {
    KllDoublesSketch sketch = KllDoublesSketch.newHeapInstance();
    sketch.update(42.0);
    byte[] valid = sketch.toByteArray();
    byte[] invalid = new byte[0];
    return new Object[][]{
        // Null BYTES values use an empty default, which is not a serialized KLL sketch.
        {true, 2, RoaringBitmap.bitmapOf(1), new byte[][]{valid, invalid}, 1},
        // Entries after length must not be read, regardless of null handling.
        {true, 1, null, new byte[][]{valid, invalid}, 1},
        {false, 1, null, new byte[][]{valid, invalid}, 1},
        {true, 0, null, new byte[][]{invalid}, 0},
        {false, 0, null, new byte[][]{invalid}, 0},
        {true, 2, RoaringBitmap.bitmapOf(0, 1), new byte[][]{invalid, invalid}, 0}
    };
  }

  @Test(dataProvider = "bytesInputs")
  public void testAggregate(boolean nullHandlingEnabled, int length, @Nullable RoaringBitmap nullBitmap,
      byte[][] values, int expectedCount) {
    PercentileKLLAggregationFunction function = function(nullHandlingEnabled);
    AggregationResultHolder holder = function.createAggregationResultHolder();
    function.aggregate(length, holder, block(nullBitmap, values));
    assertResult(function.extractAggregationResult(holder), expectedCount);
  }

  @Test(dataProvider = "bytesInputs")
  public void testAggregateGroupBySV(boolean nullHandlingEnabled, int length, @Nullable RoaringBitmap nullBitmap,
      byte[][] values, int expectedCount) {
    PercentileKLLAggregationFunction function = function(nullHandlingEnabled);
    GroupByResultHolder holder = function.createGroupByResultHolder(1, 1);
    function.aggregateGroupBySV(length, new int[length], holder, block(nullBitmap, values));
    assertResult(function.extractGroupByResult(holder, 0), expectedCount);
  }

  @Test(dataProvider = "bytesInputs")
  public void testAggregateGroupByMV(boolean nullHandlingEnabled, int length, @Nullable RoaringBitmap nullBitmap,
      byte[][] values, int expectedCount) {
    PercentileKLLAggregationFunction function = function(nullHandlingEnabled);
    GroupByResultHolder holder = function.createGroupByResultHolder(2, 2);
    int[][] groupKeys = new int[length][];
    for (int i = 0; i < length; i++) {
      groupKeys[i] = new int[]{0, 1};
    }
    function.aggregateGroupByMV(length, groupKeys, holder, block(nullBitmap, values));
    assertResult(function.extractGroupByResult(holder, 0), expectedCount);
    assertResult(function.extractGroupByResult(holder, 1), expectedCount);
  }

  private static PercentileKLLAggregationFunction function(boolean nullHandlingEnabled) {
    return new PercentileKLLAggregationFunction(
        List.of(COLUMN, ExpressionContext.forLiteral(Literal.doubleValue(50.0))), nullHandlingEnabled);
  }

  private static Map<ExpressionContext, BlockValSet> block(@Nullable RoaringBitmap nullBitmap, byte[][] values) {
    return Map.of(COLUMN, SyntheticBlockValSets.Bytes.create(nullBitmap, values));
  }

  private static void assertResult(@Nullable KllDoublesSketch sketch, int expectedCount) {
    if (expectedCount == 0) {
      assertTrue(sketch == null || sketch.isEmpty());
    } else {
      assertNotNull(sketch);
      assertEquals(sketch.getN(), (long) expectedCount);
      assertEquals(sketch.getQuantile(0.5), 42.0);
    }
  }
}
