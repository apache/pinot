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
package org.apache.pinot.common.function;

import org.testng.Assert;
import org.testng.annotations.Test;


public class AggregationFunctionTypeTest {

  @Test
  public void testGetAggregationFunctionType() {
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("CoUnT"), AggregationFunctionType.COUNT);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("MiN"), AggregationFunctionType.MIN);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("MaX"), AggregationFunctionType.MAX);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("SuM"), AggregationFunctionType.SUM);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("AvG"), AggregationFunctionType.AVG);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("MiNmAxRaNgE"),
        AggregationFunctionType.MINMAXRANGE);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("DiStInCtCoUnT"),
        AggregationFunctionType.DISTINCTCOUNT);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("DiStInCtCoUnThLl"),
        AggregationFunctionType.DISTINCTCOUNTHLL);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("DiStInCtCoUnTrAwHlL"),
        AggregationFunctionType.DISTINCTCOUNTRAWHLL);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("FaStHlL"), AggregationFunctionType.FASTHLL);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("PeRcEnTiLe5"),
        AggregationFunctionType.PERCENTILE);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("PeRcEnTiLeEsT50"),
        AggregationFunctionType.PERCENTILEEST);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("PeRcEnTiLeTdIgEsT99"),
        AggregationFunctionType.PERCENTILETDIGEST);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("CoUnTMv"), AggregationFunctionType.COUNTMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("MiNmV"), AggregationFunctionType.MINMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("MaXmV"), AggregationFunctionType.MAXMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("SuMmV"), AggregationFunctionType.SUMMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("AvGmV"), AggregationFunctionType.AVGMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("MiNmAxRaNgEmV"),
        AggregationFunctionType.MINMAXRANGEMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("DiStInCtCoUnTmV"),
        AggregationFunctionType.DISTINCTCOUNTMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("DiStInCtCoUnThLlMv"),
        AggregationFunctionType.DISTINCTCOUNTHLLMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("DiStInCtCoUnTrAwHlLmV"),
        AggregationFunctionType.DISTINCTCOUNTRAWHLLMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("PeRcEnTiLe10Mv"),
        AggregationFunctionType.PERCENTILEMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("PeRcEnTiLeEsT90mV"),
        AggregationFunctionType.PERCENTILEESTMV);
    Assert.assertEquals(AggregationFunctionType.getAggregationFunctionType("PeRcEnTiLeTdIgEsT95mV"),
        AggregationFunctionType.PERCENTILETDIGESTMV);
  }
}
