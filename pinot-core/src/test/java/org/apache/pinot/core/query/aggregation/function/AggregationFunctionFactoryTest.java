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
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("rawtypes")
public class AggregationFunctionFactoryTest {
    private static final String ARGUMENT_COLUMN = "(column)";
    private static final String ARGUMENT_STAR = "(*)";

    @Test
    public void testGetAggregationFunction() {
        FunctionContext function = getFunction("CoUnT", ARGUMENT_STAR);
        AggregationFunction aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof CountAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNT);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("MiN");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof MinAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.MIN);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("MaX");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof MaxAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAX);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("SuM");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof SumAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUM);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("SuMPreCIsiON");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof SumPrecisionAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUMPRECISION);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("AvG");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof AvgAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVG);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("MoDe");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof ModeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.MODE);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("FiRsTwItHtImE", "(column,timeColumn,'BOOLEAN')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof FirstIntValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.FIRSTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("FiRsTwItHtImE", "(column,timeColumn,'INT')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof FirstIntValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.FIRSTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("FiRsTwItHtImE", "(column,timeColumn,'LONG')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof FirstLongValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.FIRSTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("FiRsTwItHtImE", "(column,timeColumn,'FLOAT')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof FirstFloatValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.FIRSTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("FiRsTwItHtImE", "(column,timeColumn,'DOUBLE')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof FirstDoubleValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.FIRSTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("FiRsTwItHtImE", "(column,timeColumn,'STRING')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof FirstStringValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.FIRSTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("LaStWiThTiMe", "(column,timeColumn,'BOOLEAN')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof LastIntValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.LASTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("LaStWiThTiMe", "(column,timeColumn,'INT')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof LastIntValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.LASTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("LaStWiThTiMe", "(column,timeColumn,'LONG')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof LastLongValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.LASTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("LaStWiThTiMe", "(column,timeColumn,'FLOAT')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof LastFloatValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.LASTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("LaStWiThTiMe", "(column,timeColumn,'DOUBLE')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof LastDoubleValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.LASTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("LaStWiThTiMe", "(column,timeColumn,'STRING')");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof LastStringValueWithTimeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.LASTWITHTIME);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("MiNmAxRaNgE");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof MinMaxRangeAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGE);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("DiStInCtCoUnT");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof DistinctCountAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNT);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("DiStInCtCoUnThLl");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof DistinctCountHLLAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLL);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("DiStInCtCoUnTrAwHlL");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof DistinctCountRawHLLAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTRAWHLL);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("FaStHlL");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof FastHLLAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.FASTHLL);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLe5");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILE);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLeEsT50");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileEstAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEEST);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLeRaWEsT50");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileRawEstAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILERAWEST);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLeTdIgEsT99");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLeRaWTdIgEsT99");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileRawTDigestAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILERAWTDIGEST);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLe", "(column, 5)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILE);
        assertEquals(aggregationFunction.getResultColumnName(), "percentile(column, 5.0)");

        function = getFunction("PeRcEnTiLe", "(column, 5.5)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILE);
        assertEquals(aggregationFunction.getResultColumnName(), "percentile(column, 5.5)");

        function = getFunction("PeRcEnTiLeEsT", "(column, 50)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileEstAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentileest(column, 50.0)");

        function = getFunction("PeRcEnTiLeRaWeSt", "(column, 50)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileRawEstAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILERAWEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentilerawest(column, 50.0)");

        function = getFunction("PeRcEnTiLeEsT", "(column, 55.555)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileEstAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentileest(column, 55.555)");

        function = getFunction("PeRcEnTiLeRaWeSt", "(column, 55.555)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileRawEstAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILERAWEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentilerawest(column, 55.555)");

        function = getFunction("PeRcEnTiLeTdIgEsT", "(column, 99)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigest(column, 99.0)");

        function = getFunction("PeRcEnTiLeTdIgEsT", "(column, 99.9999)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigest(column, 99.9999)");

        function = getFunction("PeRcEnTiLeTdIgEsT", "(column, 99.9999, 1000)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigest(column, 99.9999, 1000)");

        function = getFunction("PeRcEnTiLeRaWtDiGeSt", "(column, 99)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileRawTDigestAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILERAWTDIGEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentilerawtdigest(column, 99.0)");

        function = getFunction("PeRcEnTiLeRaWtDiGeSt", "(column, 99.9999)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileRawTDigestAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILERAWTDIGEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentilerawtdigest(column, 99.9999)");

        function = getFunction("PeRcEntiLEkll", "(column, 99.9999)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileKLLAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEKLL);
        assertEquals(aggregationFunction.getResultColumnName(), "percentilekll(column, 99.9999)");

        function = getFunction("PeRcEnTiLeRaWtDiGeSt", "(column, 99.9999, 500)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileRawTDigestAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILERAWTDIGEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentilerawtdigest(column, 99.9999, 500)");

        function = getFunction("PeRcEnTiLeRaWtDiGeSt", "(column, 99.9999, 100)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileRawTDigestAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILERAWTDIGEST);
        assertEquals(aggregationFunction.getResultColumnName(), "percentilerawtdigest(column, 99.9999)");

        function = getFunction("CoUnTmV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof CountMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNTMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("MiNmV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof MinMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("MaXmV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof MaxMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAXMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("SuMmV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof SumMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUMMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("AvGmV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof AvgMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVGMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("AvG_mV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof AvgMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVGMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("MiNmAxRaNgEmV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof MinMaxRangeMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGEMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("DiStInCtCoUnTmV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof DistinctCountMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("DiStInCtCoUnThLlMv");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof DistinctCountHLLMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLLMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("DiStInCt_CoUnT_hLl_Mv");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof DistinctCountHLLMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLLMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("DiStInCtCoUnTrAwHlLmV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof DistinctCountRawHLLMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTRAWHLLMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLe10Mv");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLeEsT90mV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileEstMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEESTMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLeTdIgEsT95mV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLe_TdIgEsT_95_mV");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("PeRcEnTiLeMv", "(column, 10)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEMV);
        assertEquals(aggregationFunction.getResultColumnName(), "percentilemv(column, 10.0)");

        function = getFunction("PeRcEnTiLeEsTmV", "(column, 90)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileEstMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEESTMV);
        assertEquals(aggregationFunction.getResultColumnName(), "percentileestmv(column, 90.0)");

        function = getFunction("PeRcEnTiLeTdIgEsTmV", "(column, 95)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
        assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigestmv(column, 95.0)");

        function = getFunction("PeRcEnTiLeTdIgEsTmV", "(column, 95, 1000)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
        assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigestmv(column, 95.0, 1000)");

        function = getFunction("PeRcEnTiLe_TdIgEsT_mV", "(column, 95)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
        assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigestmv(column, 95.0)");

        function = getFunction("PeRcEnTiLe_TdIgEsT_mV", "(column, 95, 200)");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
        assertEquals(aggregationFunction.getResultColumnName(), "percentiletdigestmv(column, 95.0, 200)");

        function = getFunction("bool_and");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof BooleanAndAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.BOOLAND);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("bool_or");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof BooleanOrAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.BOOLOR);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("skewness");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof FourthMomentAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.SKEWNESS);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());

        function = getFunction("kurtosis");
        aggregationFunction = AggregationFunctionFactory.getAggregationFunction(function, false);
        assertTrue(aggregationFunction instanceof FourthMomentAggregationFunction);
        assertEquals(aggregationFunction.getType(), AggregationFunctionType.KURTOSIS);
        assertEquals(aggregationFunction.getResultColumnName(), function.toString());
    }

    private FunctionContext getFunction(String functionName) {
        return getFunction(functionName, ARGUMENT_COLUMN);
    }

    private FunctionContext getFunction(String functionName, String args) {
        return RequestContextUtils.getExpression(functionName + args).getFunction();
    }
}
