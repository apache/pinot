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

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.segment.spi.Constants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PercentileTDigestAggregationFunctionTest {

  @Test
  public void testCanUseStarTreeDefaultCompression() {
    PercentileTDigestAggregationFunction function = new PercentileTDigestAggregationFunction(
        ExpressionContext.forIdentifier("col"), 95, false);

    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "100")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, 100)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "200")));

    function = new PercentileTDigestAggregationFunction(ExpressionContext.forIdentifier("col"), 99, 100, true);
    Assert.assertTrue(function.canUseStarTree(Map.of()));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "100")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, 100)));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, 200)));
  }

  @Test
  public void testCanUseStarTreeCustomCompression() {
    PercentileTDigestAggregationFunction function = new PercentileTDigestAggregationFunction(
        ExpressionContext.forIdentifier("col"), 95, 200, false);

    Assert.assertFalse(function.canUseStarTree(Map.of()));
    Assert.assertFalse(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "100")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, "200")));
    Assert.assertTrue(function.canUseStarTree(Map.of(Constants.PERCENTILETDIGEST_COMPRESSION_FACTOR_KEY, 200)));
  }
}
