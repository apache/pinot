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
package org.apache.pinot.segment.local.aggregator;

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import java.util.Collections;
import java.util.stream.IntStream;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class DistinctCountULLValueAggregatorTest {

  @Test
  public void initialShouldCreateSingleItemULL() {
    DistinctCountULLValueAggregator agg = new DistinctCountULLValueAggregator(Collections.emptyList());
    assertEquals(
        Math.round(agg.getInitialAggregatedValue("hello world").getDistinctCountEstimate()),
        1.0);
  }

  @Test
  public void initialShouldParseAULL() {
    UltraLogLog input = UltraLogLog.create(4);
    IntStream.range(0, 1000).forEach(i ->
        UltraLogLogUtils.hashObject(i)
            .ifPresent(input::add)
    );
    DistinctCountULLValueAggregator agg = new DistinctCountULLValueAggregator(Collections.singletonList(
        ExpressionContext.forLiteralContext(Literal.intValue(12))
    ));
    byte[] bytes = agg.serializeAggregatedValue(input);
    UltraLogLog aggregated = agg.getInitialAggregatedValue(bytes);

    assertEquals(agg.getInitialAggregatedValue(bytes).getDistinctCountEstimate(), input.getDistinctCountEstimate());

    // should preserve the smaller P
    assertEquals(aggregated.getP(), 4);
  }

  @Test
  public void initialShouldParseALargeULL() {
    UltraLogLog input = UltraLogLog.create(20);
    IntStream.range(0, 1000).forEach(i ->
        UltraLogLogUtils.hashObject(i)
            .ifPresent(input::add)
    );
    DistinctCountULLValueAggregator agg = new DistinctCountULLValueAggregator(Collections.singletonList(
        ExpressionContext.forLiteralContext(Literal.intValue(12))
    ));
    byte[] bytes = agg.serializeAggregatedValue(input);
    UltraLogLog aggregated = agg.getInitialAggregatedValue(bytes);

    assertEquals(
        agg.getInitialAggregatedValue(bytes).getDistinctCountEstimate(),
        input.downsize(12).getDistinctCountEstimate());

    // should downsize to the picked P
    assertEquals(aggregated.getP(), 12);
  }

  @Test
  public void applyAggregatedValueShouldUnion() {
    UltraLogLog input1 = UltraLogLog.create(12);
    IntStream.range(0, 1000).mapToObj(UltraLogLogUtils::hashObject).forEach(h -> h.ifPresent(input1::add));
    UltraLogLog input2 = UltraLogLog.create(12);
    IntStream.range(0, 1000).mapToObj(UltraLogLogUtils::hashObject).forEach(h -> h.ifPresent(input2::add));
    DistinctCountULLValueAggregator agg = new DistinctCountULLValueAggregator(Collections.emptyList());
    UltraLogLog result = agg.applyAggregatedValue(input1, input2);

    UltraLogLog union = UltraLogLog.create(12).add(input1).add(input2);

    assertEquals(result.getDistinctCountEstimate(), union.getDistinctCountEstimate());
  }

  @Test
  public void applyRawValueShouldUnion() {
    UltraLogLog input1 = UltraLogLog.create(12);
    IntStream.range(0, 1000).mapToObj(UltraLogLogUtils::hashObject).forEach(h -> h.ifPresent(input1::add));
    UltraLogLog input2 = UltraLogLog.create(12);
    IntStream.range(0, 1000).mapToObj(UltraLogLogUtils::hashObject).forEach(h -> h.ifPresent(input2::add));

    DistinctCountULLValueAggregator agg = new DistinctCountULLValueAggregator(Collections.emptyList());
    byte[] result2bytes = agg.serializeAggregatedValue(input2);
    UltraLogLog result = agg.applyRawValue(input1, result2bytes);

    UltraLogLog union = UltraLogLog.create(12).add(input1).add(input2);

    assertEquals(result.getDistinctCountEstimate(), union.getDistinctCountEstimate());
  }

  private long roundedEstimate(UltraLogLog ull) {
    return Math.round(ull.getDistinctCountEstimate());
  }

  @Test
  public void getInitialValueShouldSupportDifferentTypes() {
    DistinctCountULLValueAggregator agg = new DistinctCountULLValueAggregator(Collections.emptyList());
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(12345)), 1.0);
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(12345L)), 1.0);
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(12.345f)), 1.0);
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(12.345d)), 1.0);
    assertThrows(() -> agg.getInitialAggregatedValue(new Object()));
  }

  @Test
  public void getInitialValueShouldSupportMultiValueTypes() {
    DistinctCountULLValueAggregator agg = new DistinctCountULLValueAggregator(Collections.emptyList());
    Integer[] ints = {12345, 54321};
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(ints)), 2.0);
    Long[] longs = {12345L, 54321L};
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(longs)), 2.0);
    Float[] floats = {12.345f};
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(floats)), 1.0);
    Double[] doubles = {12.345d};
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(doubles)), 1.0);
    Object[] objects = {new Object()};
    assertThrows(() -> agg.getInitialAggregatedValue(objects));
    byte[][] zeroSketches = {};
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(zeroSketches)), 0.0);
    byte[][] twoSketches = {
        agg.serializeAggregatedValue(agg.getInitialAggregatedValue("hello")),
        agg.serializeAggregatedValue(agg.getInitialAggregatedValue("world"))
    };
    assertEquals(roundedEstimate(agg.getInitialAggregatedValue(twoSketches)), 2.0);
  }
}
