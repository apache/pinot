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

import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextConvertUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("rawtypes")
public class AggregationFunctionFactoryTest {
  private static final String ARGUMENT = "(column)";
  private static final QueryContext DUMMY_QUERY_CONTEXT =
      QueryContextConverterUtils.getQueryContextFromPQL("SELECT * FROM testTable");

  @Test
  public void testGetAggregationFunction() {
    FunctionContext function = getFunction("CoUnT");
    AggregationFunction aggregationFunction =
        AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof CountAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNT);
    assertEquals(aggregationFunction.getColumnName(), "count_star");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("MiN");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof MinAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.MIN);
    assertEquals(aggregationFunction.getColumnName(), "min_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("MaX");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof MaxAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAX);
    assertEquals(aggregationFunction.getColumnName(), "max_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("SuM");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof SumAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUM);
    assertEquals(aggregationFunction.getColumnName(), "sum_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("SuMPreCIsiON");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof SumPrecisionAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUMPRECISION);
    assertEquals(aggregationFunction.getColumnName(), "sumPrecision_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("AvG");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof AvgAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVG);
    assertEquals(aggregationFunction.getColumnName(), "avg_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("MiNmAxRaNgE");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof MinMaxRangeAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGE);
    assertEquals(aggregationFunction.getColumnName(), "minMaxRange_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("DiStInCtCoUnT");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof DistinctCountAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNT);
    assertEquals(aggregationFunction.getColumnName(), "distinctCount_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("DiStInCtCoUnThLl");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof DistinctCountHLLAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLL);
    assertEquals(aggregationFunction.getColumnName(), "distinctCountHLL_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("DiStInCtCoUnTrAwHlL");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof DistinctCountRawHLLAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTRAWHLL);
    assertEquals(aggregationFunction.getColumnName(), "distinctCountRawHLL_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("FaStHlL");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof FastHLLAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.FASTHLL);
    assertEquals(aggregationFunction.getColumnName(), "fastHLL_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("PeRcEnTiLe5");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILE);
    assertEquals(aggregationFunction.getColumnName(), "percentile5_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("PeRcEnTiLeEsT50");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileEstAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEEST);
    assertEquals(aggregationFunction.getColumnName(), "percentileEst50_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("PeRcEnTiLeTdIgEsT99");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
    assertEquals(aggregationFunction.getColumnName(), "percentileTDigest99_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("PeRcEnTiLe", "(column, 5)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILE);
    assertEquals(aggregationFunction.getColumnName(), "percentile5.0_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentile(column, 5.0)");

    function = getFunction("PeRcEnTiLe", "(column, 5.5)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILE);
    assertEquals(aggregationFunction.getColumnName(), "percentile5.5_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentile(column, 5.5)");

    function = getFunction("PeRcEnTiLeEsT", "(column, 50)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileEstAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEEST);
    assertEquals(aggregationFunction.getColumnName(), "percentileEst50.0_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentileest(column, 50.0)");

    function = getFunction("PeRcEnTiLeEsT", "(column, 55.555)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileEstAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEEST);
    assertEquals(aggregationFunction.getColumnName(), "percentileEst55.555_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentileest(column, 55.555)");

    function = getFunction("PeRcEnTiLeTdIgEsT", "(column, 99)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
    assertEquals(aggregationFunction.getColumnName(), "percentileTDigest99.0_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigest(column, 99.0)");

    function = getFunction("PeRcEnTiLeTdIgEsT", "(column, 99.9999)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
    assertEquals(aggregationFunction.getColumnName(), "percentileTDigest99.9999_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigest(column, 99.9999)");

    function = getFunction("CoUnTmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof CountMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNTMV);
    assertEquals(aggregationFunction.getColumnName(), "countMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("MiNmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof MinMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMV);
    assertEquals(aggregationFunction.getColumnName(), "minMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("MaXmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof MaxMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAXMV);
    assertEquals(aggregationFunction.getColumnName(), "maxMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("SuMmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof SumMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUMMV);
    assertEquals(aggregationFunction.getColumnName(), "sumMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("AvGmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof AvgMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVGMV);
    assertEquals(aggregationFunction.getColumnName(), "avgMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("AvG_mV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof AvgMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVGMV);
    assertEquals(aggregationFunction.getColumnName(), "avgMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), "avgmv(column)");

    function = getFunction("MiNmAxRaNgEmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof MinMaxRangeMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGEMV);
    assertEquals(aggregationFunction.getColumnName(), "minMaxRangeMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("DiStInCtCoUnTmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof DistinctCountMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTMV);
    assertEquals(aggregationFunction.getColumnName(), "distinctCountMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("DiStInCtCoUnThLlMv");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof DistinctCountHLLMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLLMV);
    assertEquals(aggregationFunction.getColumnName(), "distinctCountHLLMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("DiStInCt_CoUnT_hLl_Mv");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof DistinctCountHLLMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLLMV);
    assertEquals(aggregationFunction.getColumnName(), "distinctCountHLLMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), "distinctcounthllmv(column)");

    function = getFunction("DiStInCtCoUnTrAwHlLmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof DistinctCountRawHLLMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTRAWHLLMV);
    assertEquals(aggregationFunction.getColumnName(), "distinctCountRawHLLMV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("PeRcEnTiLe10Mv");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEMV);
    assertEquals(aggregationFunction.getColumnName(), "percentile10MV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("PeRcEnTiLeEsT90mV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileEstMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEESTMV);
    assertEquals(aggregationFunction.getColumnName(), "percentileEst90MV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("PeRcEnTiLeTdIgEsT95mV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
    assertEquals(aggregationFunction.getColumnName(), "percentileTDigest95MV_column");
    assertEquals(aggregationFunction.getResultColumnName(), function.toString());

    function = getFunction("PeRcEnTiLe_TdIgEsT_95_mV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
    assertEquals(aggregationFunction.getColumnName(), "percentileTDigest95MV_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigest95mv(column)");

    function = getFunction("PeRcEnTiLeMv", "(column, 10)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEMV);
    assertEquals(aggregationFunction.getColumnName(), "percentile10.0MV_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentilemv(column, 10.0)");

    function = getFunction("PeRcEnTiLeEsTmV", "(column, 90)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileEstMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEESTMV);
    assertEquals(aggregationFunction.getColumnName(), "percentileEst90.0MV_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentileestmv(column, 90.0)");

    function = getFunction("PeRcEnTiLeTdIgEsTmV", "(column, 95)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
    assertEquals(aggregationFunction.getColumnName(), "percentileTDigest95.0MV_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigestmv(column, 95.0)");

    function = getFunction("PeRcEnTiLe_TdIgEsT_mV", "(column, 95)");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, DUMMY_QUERY_CONTEXT);
    assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
    assertEquals(aggregationFunction.getColumnName(), "percentileTDigest95.0MV_column");
    assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigestmv(column, 95.0)");
  }

  private FunctionContext getFunction(String functionName) {
    return getFunction(functionName, ARGUMENT);
  }

  private FunctionContext getFunction(String functionName, String args) {
    return RequestContextConvertUtils.getExpression(functionName + args).getFunction();
  }

  @Test
  public void testAggregationFunctionWithMultipleArgs() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContextFromPQL("SELECT distinct(column1, column2, column3) FROM testTable");
    AggregationFunction aggregationFunction = AggregationFunctionFactory
        .getAggregationFunction(queryContext.getSelectExpressions().get(0).getFunction(), queryContext);
    assertTrue(aggregationFunction instanceof DistinctAggregationFunction);
    assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCT);
    assertEquals(aggregationFunction.getColumnName(), "distinct_column1:column2:column3");
    assertEquals(aggregationFunction.getResultColumnName(), "distinct(column1:column2:column3)");
  }
}
