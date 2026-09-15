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
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class DistinctCountSmartHLLAggregationFunctionTest {

  @Test
  public void testParameterParsing() {
    // Test default values
    DistinctCountSmartHLLAggregationFunction function =
        new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);
    assertEquals(function.getThreshold(), 100_000);
    assertEquals(function.getDictIdCardinalityThreshold(), 100_000);

    // Test individual parameters
    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "threshold=50000")), false);
    assertEquals(function.getThreshold(), 50_000);
    assertEquals(function.getDictIdCardinalityThreshold(), 100_000);

    function = new DistinctCountSmartHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"), ExpressionContext.forLiteral(DataType.STRING, "log2m=8")),
        false);
    assertEquals(function.getThreshold(), 100_000);
    assertEquals(function.getDictIdCardinalityThreshold(), 100_000);

    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "dictThreshold=50000")), false);
    assertEquals(function.getThreshold(), 100_000);
    assertEquals(function.getDictIdCardinalityThreshold(), 50_000);

    // Test disabled dictThreshold (non-positive values)
    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "dictThreshold=-1")), false);
    assertEquals(function.getDictIdCardinalityThreshold(), Integer.MAX_VALUE);

    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "dictThreshold=0")), false);
    assertEquals(function.getDictIdCardinalityThreshold(), Integer.MAX_VALUE);

    // Test multiple parameters together
    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "threshold=200000;log2m=10;dictThreshold=150000")), false);
    assertEquals(function.getThreshold(), 200_000);
    assertEquals(function.getDictIdCardinalityThreshold(), 150_000);

    // Test parameter order independence
    DistinctCountSmartHLLAggregationFunction function1 = new DistinctCountSmartHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(DataType.STRING, "dictThreshold=50000;threshold=100000;log2m=8")), false);
    DistinctCountSmartHLLAggregationFunction function2 = new DistinctCountSmartHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(DataType.STRING, "log2m=8;dictThreshold=50000;threshold=100000")), false);
    assertEquals(function1.getThreshold(), function2.getThreshold());
    assertEquals(function1.getDictIdCardinalityThreshold(), function2.getDictIdCardinalityThreshold());

    // Test legacy parameter names
    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "hllConversionThreshold=50000;hllLog2m=10")), false);
    assertEquals(function.getThreshold(), 50_000);

    // Test case-insensitive parameters
    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "THRESHOLD=50000;LOG2M=8;DICTTHRESHOLD=100000")), false);
    assertEquals(function.getThreshold(), 50_000);
    assertEquals(function.getDictIdCardinalityThreshold(), 100_000);
  }

  @Test
  public void testFunctionMetadata() {
    DistinctCountSmartHLLAggregationFunction function =
        new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);

    // Test function type
    assertEquals(function.getType().getName(), "distinctCountSmartHLL");

    // Test result types
    assertEquals(function.getIntermediateResultColumnType(), DataSchema.ColumnDataType.OBJECT);
    assertEquals(function.getFinalResultColumnType(), DataSchema.ColumnDataType.INT);

    // Test result holder creation
    assertNotNull(function.createAggregationResultHolder());
    assertNotNull(function.createGroupByResultHolder(10, 100));
  }

  @Test
  public void testHLLOperations() {
    DistinctCountSmartHLLAggregationFunction function =
        new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);

    // Test merge final results (should sum)
    Integer finalResult = function.mergeFinalResult(100, 200);
    assertEquals(finalResult.intValue(), 300);

    // Test extract final result (HLL cardinality)
    HyperLogLog hll = new HyperLogLog(12);
    for (int i = 0; i < 1000; i++) {
      hll.offer(i);
    }
    Long cardinality = Long.valueOf(function.extractFinalResult(hll));
    assertNotNull(cardinality);
    assertTrue(cardinality >= 950 && cardinality <= 1050, "Cardinality: " + cardinality);

    // Test merge intermediate results (HLL union)
    HyperLogLog hll1 = new HyperLogLog(12);
    HyperLogLog hll2 = new HyperLogLog(12);
    for (int i = 0; i < 500; i++) {
      hll1.offer(i);
    }
    for (int i = 250; i < 750; i++) {
      hll2.offer(i);
    }
    HyperLogLog merged = (HyperLogLog) function.merge(hll1, hll2);
    assertNotNull(merged);
    long mergedCardinality = merged.cardinality();
    assertTrue(mergedCardinality >= 700 && mergedCardinality <= 800, "Merged cardinality: " + mergedCardinality);
  }

  @Test
  public void itExtractsHLLFromGroupByResultHolderWhenSketchConverted() {
    DistinctCountSmartHLLAggregationFunction function = new DistinctCountSmartHLLAggregationFunction(
        List.of(ExpressionContext.forIdentifier("col"),
            ExpressionContext.forLiteral(DataType.STRING, "threshold=5;dictThreshold=5")), false);

    ObjectGroupByResultHolder holder = new ObjectGroupByResultHolder(10, 10);

    HyperLogLog hll = new HyperLogLog(12);
    for (int i = 0; i < 100; i++) {
      hll.offer(i);
    }
    holder.setValueForKey(0, hll);

    Object extracted = function.extractGroupByResult(holder, 0);
    assertTrue(extracted instanceof HyperLogLog,
        "Expected HyperLogLog but got: " + (extracted == null ? "null" : extracted.getClass().getName()));
    assertFalse(extracted instanceof Set, "HyperLogLog must not be cast to Set");

    int cardinality = function.extractFinalResult(extracted);
    assertTrue(cardinality >= 90 && cardinality <= 110, "Cardinality out of range: " + cardinality);
  }

  @Test
  public void testAdaptiveConversion() {
    // Test adaptive conversion enabled by default (100K threshold)
    DistinctCountSmartHLLAggregationFunction function =
        new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col")), false);
    assertEquals(function.getDictIdCardinalityThreshold(), 100_000);

    // Test adaptive conversion with custom threshold
    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "dictThreshold=50000")), false);
    assertEquals(function.getDictIdCardinalityThreshold(), 50_000);

    // Test adaptive conversion disabled (Integer.MAX_VALUE)
    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "dictThreshold=" + Integer.MAX_VALUE)), false);
    assertEquals(function.getDictIdCardinalityThreshold(), Integer.MAX_VALUE);

    // Test non-positive threshold converted to Integer.MAX_VALUE (disabled)
    function = new DistinctCountSmartHLLAggregationFunction(List.of(ExpressionContext.forIdentifier("col"),
        ExpressionContext.forLiteral(DataType.STRING, "dictThreshold=-1")), false);
    assertEquals(function.getDictIdCardinalityThreshold(), Integer.MAX_VALUE);
  }
}
