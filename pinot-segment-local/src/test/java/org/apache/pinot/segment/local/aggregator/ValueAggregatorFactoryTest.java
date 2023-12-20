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

import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ValueAggregatorFactoryTest {

  @Test
  public void testGetValueAggregationType() {
    assertEquals(ValueAggregatorFactory.getAggregatedFunctionType(AggregationFunctionType.DISTINCTCOUNTRAWHLL),
        AggregationFunctionType.DISTINCTCOUNTHLL);
    assertEquals(ValueAggregatorFactory.getAggregatedFunctionType(AggregationFunctionType.PERCENTILERAWTDIGEST),
        AggregationFunctionType.PERCENTILETDIGEST);
    assertEquals(ValueAggregatorFactory.getAggregatedFunctionType(AggregationFunctionType.DISTINCTCOUNTRAWTHETASKETCH),
        AggregationFunctionType.DISTINCTCOUNTTHETASKETCH);
    assertEquals(
        ValueAggregatorFactory.getAggregatedFunctionType(AggregationFunctionType.DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH),
        AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH);
    assertEquals(
        ValueAggregatorFactory.getAggregatedFunctionType(AggregationFunctionType.SUMVALUESINTEGERSUMTUPLESKETCH),
        AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH);
    assertEquals(
        ValueAggregatorFactory.getAggregatedFunctionType(AggregationFunctionType.AVGVALUEINTEGERSUMTUPLESKETCH),
        AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH);
    assertEquals(ValueAggregatorFactory.getAggregatedFunctionType(AggregationFunctionType.DISTINCTCOUNTHLLPLUS),
        AggregationFunctionType.DISTINCTCOUNTHLLPLUS);
    // Default case
    assertEquals(ValueAggregatorFactory.getAggregatedFunctionType(AggregationFunctionType.COUNT),
        AggregationFunctionType.COUNT);
  }
}
