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

import java.util.Collections;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.parsers.CompilerConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggregationFunctionFactoryTest {
  private static final String COLUMN = "column";

  @Test
  public void testGetAggregationFunction() {
    AggregationFunction aggregationFunction;

    BrokerRequest brokerRequest = new BrokerRequest();
    String column = "testColumn";

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("CoUnT");
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof CountAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNT);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "count_star");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MiN");
    column = "min_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MinAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MIN);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MaX");
    column = "max_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MaxAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAX);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("SuM");
    column = "sum_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof SumAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUM);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("AvG");
    column = "avg_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof AvgAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVG);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MiNmAxRaNgE");
    column = "minMaxRange_column";
    aggregationInfo.setAggregationParams(
        Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MinMaxRangeAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGE);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnT");
    column = "distinctCount_column";
    aggregationInfo.setAggregationParams(
        Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNT);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnThLl");
    column = "distinctCountHLL_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountHLLAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLL);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnTrAwHlL");
    column = "distinctCountRawHLL_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountRawHLLAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTRAWHLL);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);
    ;

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("FaStHlL");
    column = "fastHLL_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof FastHLLAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.FASTHLL);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLe5");
    column = "percentile5_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILE);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLeEsT50");
    column = "percentileEst50_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileEstAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEEST);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLeTdIgEsT99");
    column = "percentileTDigest99_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("CoUnTmV");
    column = "countMV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof CountMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MiNmV");
    column = "minMV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MinMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MaXmV");
    column = "maxMV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MaxMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAXMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("SuMmV");
    column = "sumMV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof SumMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUMMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("AvGmV");
    column = "avgMV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof AvgMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVGMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MiNmAxRaNgEmV");
    column = "minMaxRangeMV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MinMaxRangeMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGEMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnTmV");
    column = "distinctCountMV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnThLlMv");
    column = "distinctCountHLLMV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountHLLMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLLMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnTrAwHlLmV");
    column = "distinctCountRawHLLMV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountRawHLLMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTRAWHLLMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLe10Mv");
    column = "percentile10MV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLeEsT90mV");
    column = "percentileEst90MV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileEstMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEESTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLeTdIgEsT95mV");
    column = "percentileTDigest95MV_column";
    aggregationInfo
        .setAggregationParams(Collections.singletonMap(CompilerConstants.COLUMN_KEY_IN_AGGREGATION_INFO, column));
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), column);
  }
}
