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

import java.util.List;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ValueAggregatorTest {

  @Test(dataProvider = "fixedSizeAggregatedValue")
  public void testFixedSizeAggregatedValue(AggregationFunctionType functionType, List<ExpressionContext> arguments,
      boolean expected) {
    assertEquals(ValueAggregatorFactory.getValueAggregator(functionType, arguments).isAggregatedValueFixedSize(),
        expected);
  }

  @DataProvider
  public static Object[][] fixedSizeAggregatedValue() {
    return new Object[][]{
        {AggregationFunctionType.COUNT, List.of(), true},
        {AggregationFunctionType.MIN, List.of(), true},
        {AggregationFunctionType.MAX, List.of(), true},
        {AggregationFunctionType.SUM, List.of(), true},
        {AggregationFunctionType.SUMPRECISION, List.of(), false},
        {AggregationFunctionType.SUMPRECISION, List.of(ExpressionContext.forLiteral(Literal.intValue(20))), true},
        {AggregationFunctionType.AVG, List.of(), true},
        {AggregationFunctionType.MINMAXRANGE, List.of(), true},
        {AggregationFunctionType.DISTINCTCOUNTBITMAP, List.of(), false},
        {AggregationFunctionType.DISTINCTCOUNTHLL, List.of(), true},
        {AggregationFunctionType.DISTINCTCOUNTHLLPLUS, List.of(), true},
        {AggregationFunctionType.DISTINCTCOUNTULL, List.of(), true},
        {AggregationFunctionType.DISTINCTCOUNTTHETASKETCH, List.of(), false},
        {AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH, List.of(), true},
        {AggregationFunctionType.DISTINCTCOUNTCPCSKETCH, List.of(), true},
        {AggregationFunctionType.PERCENTILEEST, List.of(), false},
        {AggregationFunctionType.PERCENTILETDIGEST, List.of(), false}
    };
  }
}
