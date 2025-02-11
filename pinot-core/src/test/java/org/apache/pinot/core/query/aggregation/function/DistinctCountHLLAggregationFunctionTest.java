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
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.Constants;
import org.testng.Assert;
import org.testng.annotations.Test;


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
}
