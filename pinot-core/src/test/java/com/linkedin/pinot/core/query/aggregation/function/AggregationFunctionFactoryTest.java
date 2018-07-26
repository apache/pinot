/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

import org.testng.Assert;
import org.testng.annotations.Test;


public class AggregationFunctionFactoryTest {

  @Test
  public void testGetAggregationFunction() {
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction("CoUnT") instanceof CountAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction("MiN") instanceof MinAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction("MaX") instanceof MaxAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction("SuM") instanceof SumAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction("AvG") instanceof AvgAggregationFunction);
    Assert.assertTrue(
        AggregationFunctionFactory.getAggregationFunction("MiNmAxRaNgE") instanceof MinMaxRangeAggregationFunction);
    Assert.assertTrue(
        AggregationFunctionFactory.getAggregationFunction("DiStInCtCoUnT") instanceof DistinctCountAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction(
        "DiStInCtCoUnThLl") instanceof DistinctCountHLLAggregationFunction);
    Assert.assertTrue(
        AggregationFunctionFactory.getAggregationFunction("FaStHlL") instanceof FastHLLAggregationFunction);
    Assert.assertTrue(
        AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLe5") instanceof PercentileAggregationFunction);
    Assert.assertEquals(
        AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLe5").getColumnName(new String[]{"column"}),
        "percentile5_column");
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction(
        "PeRcEnTiLeEsT50") instanceof PercentileEstAggregationFunction);
    Assert.assertEquals(
        AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLeEsT50").getColumnName(new String[]{"column"}),
        "percentileEst50_column");
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction(
        "PeRcEnTiLeTdIgEsT99") instanceof PercentileTDigestAggregationFunction);
    Assert.assertEquals(
        AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLeTdIgEsT99").getColumnName(new String[]{"column"}),
        "percentileTDigest99_column");
    Assert.assertTrue(
        AggregationFunctionFactory.getAggregationFunction("CoUnTMv") instanceof CountMVAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction("MiNmV") instanceof MinMVAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction("MaXmV") instanceof MaxMVAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction("SuMmV") instanceof SumMVAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction("AvGmV") instanceof AvgMVAggregationFunction);
    Assert.assertTrue(
        AggregationFunctionFactory.getAggregationFunction("MiNmAxRaNgEmV") instanceof MinMaxRangeMVAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction(
        "DiStInCtCoUnTmV") instanceof DistinctCountMVAggregationFunction);
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction(
        "DiStInCtCoUnThLlMv") instanceof DistinctCountHLLMVAggregationFunction);
    Assert.assertTrue(
        AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLe10Mv") instanceof PercentileMVAggregationFunction);
    Assert.assertEquals(
        AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLe10Mv").getColumnName(new String[]{"column"}),
        "percentile10MV_column");
    Assert.assertTrue(AggregationFunctionFactory.getAggregationFunction(
        "PeRcEnTiLeEsT95mV") instanceof PercentileEstMVAggregationFunction);
    Assert.assertEquals(
        AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLeEsT95mV").getColumnName(new String[]{"column"}),
        "percentileEst95MV_column");
  }
}
