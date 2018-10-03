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
  private static final String COLUMN = "column";

  @Test
  public void testGetAggregationFunction() {
    AggregationFunction aggregationFunction;

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("CoUnT");
    Assert.assertTrue(aggregationFunction instanceof CountAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNT);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "count_star");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("MiN");
    Assert.assertTrue(aggregationFunction instanceof MinAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MIN);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "min_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("MaX");
    Assert.assertTrue(aggregationFunction instanceof MaxAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAX);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "max_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("SuM");
    Assert.assertTrue(aggregationFunction instanceof SumAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUM);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "sum_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("AvG");
    Assert.assertTrue(aggregationFunction instanceof AvgAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVG);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "avg_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("MiNmAxRaNgE");
    Assert.assertTrue(aggregationFunction instanceof MinMaxRangeAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGE);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "minMaxRange_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("DiStInCtCoUnT");
    Assert.assertTrue(aggregationFunction instanceof DistinctCountAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNT);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCount_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("DiStInCtCoUnThLl");
    Assert.assertTrue(aggregationFunction instanceof DistinctCountHLLAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLL);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCountHLL_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("FaStHlL");
    Assert.assertTrue(aggregationFunction instanceof FastHLLAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.FASTHLL);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "fastHLL_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLe5");
    Assert.assertTrue(aggregationFunction instanceof PercentileAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILE);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentile5_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLeEsT50");
    Assert.assertTrue(aggregationFunction instanceof PercentileEstAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEEST);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentileEst50_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLeTdIgEsT99");
    Assert.assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentileTDigest99_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("CoUnTmV");
    Assert.assertTrue(aggregationFunction instanceof CountMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "countMV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("MiNmV");
    Assert.assertTrue(aggregationFunction instanceof MinMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "minMV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("MaXmV");
    Assert.assertTrue(aggregationFunction instanceof MaxMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAXMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "maxMV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("SuMmV");
    Assert.assertTrue(aggregationFunction instanceof SumMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUMMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "sumMV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("AvGmV");
    Assert.assertTrue(aggregationFunction instanceof AvgMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVGMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "avgMV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("MiNmAxRaNgEmV");
    Assert.assertTrue(aggregationFunction instanceof MinMaxRangeMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGEMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "minMaxRangeMV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("DiStInCtCoUnTmV");
    Assert.assertTrue(aggregationFunction instanceof DistinctCountMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCountMV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("DiStInCtCoUnThLlMv");
    Assert.assertTrue(aggregationFunction instanceof DistinctCountHLLMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLLMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCountHLLMV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLe10Mv");
    Assert.assertTrue(aggregationFunction instanceof PercentileMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentile10MV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLeEsT90mV");
    Assert.assertTrue(aggregationFunction instanceof PercentileEstMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEESTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentileEst90MV_column");

    aggregationFunction = AggregationFunctionFactory.getAggregationFunction("PeRcEnTiLeTdIgEsT95mV");
    Assert.assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentileTDigest95MV_column");
  }
}
