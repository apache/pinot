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

import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AggregationFunctionFactoryTest {
  private static final String COLUMN = "column";

  @Test
  public void testGetAggregationFunction() {
    AggregationFunction aggregationFunction;

    BrokerRequest brokerRequest = new BrokerRequest();

    AggregationInfo aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("CoUnT");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof CountAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNT);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "count_star");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MiN");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MinAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MIN);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "min_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MaX");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MaxAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAX);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "max_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("SuM");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof SumAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUM);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "sum_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("AvG");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof AvgAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVG);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "avg_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MiNmAxRaNgE");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MinMaxRangeAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGE);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "minMaxRange_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnT");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNT);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCount_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnThLl");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountHLLAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLL);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCountHLL_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnTrAwHlL");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountRawHLLAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTRAWHLL);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCountRawHLL_column");
    ;

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("FaStHlL");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof FastHLLAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.FASTHLL);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "fastHLL_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLe5");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILE);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentile5_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLeEsT50");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileEstAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEEST);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentileEst50_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLeTdIgEsT99");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileTDigestAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGEST);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentileTDigest99_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("CoUnTmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof CountMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.COUNTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "countMV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MiNmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MinMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "minMV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MaXmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MaxMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MAXMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "maxMV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("SuMmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof SumMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.SUMMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "sumMV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("AvGmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof AvgMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.AVGMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "avgMV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("MiNmAxRaNgEmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof MinMaxRangeMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.MINMAXRANGEMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "minMaxRangeMV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnTmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCountMV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnThLlMv");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountHLLMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTHLLMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCountHLLMV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("DiStInCtCoUnTrAwHlLmV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof DistinctCountRawHLLMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.DISTINCTCOUNTRAWHLLMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "distinctCountRawHLLMV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLe10Mv");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentile10MV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLeEsT90mV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileEstMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILEESTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentileEst90MV_column");

    aggregationInfo = new AggregationInfo();
    aggregationInfo.setAggregationType("PeRcEnTiLeTdIgEsT95mV");
    aggregationFunction = AggregationFunctionFactory.getAggregationFunction(aggregationInfo, brokerRequest);
    Assert.assertTrue(aggregationFunction instanceof PercentileTDigestMVAggregationFunction);
    Assert.assertEquals(aggregationFunction.getType(), AggregationFunctionType.PERCENTILETDIGESTMV);
    Assert.assertEquals(aggregationFunction.getColumnName(COLUMN), "percentileTDigest95MV_column");
  }
}
