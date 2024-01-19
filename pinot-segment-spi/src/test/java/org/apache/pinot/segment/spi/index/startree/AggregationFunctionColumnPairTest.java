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
package org.apache.pinot.segment.spi.index.startree;

import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.index.startree.AggregationFunctionColumnPair.getStoredType;
import static org.testng.AssertJUnit.assertEquals;


public class AggregationFunctionColumnPairTest {

  @Test
  public void testResolveToAggregatedType() {
    assertEquals(AggregationFunctionColumnPair.fromColumnName("distinctCountThetaSketch__dimX"),
        AggregationFunctionColumnPair.resolveToStoredType(
            AggregationFunctionColumnPair.fromColumnName("distinctCountRawThetaSketch__dimX")));
    assertEquals(AggregationFunctionColumnPair.fromColumnName("count__*"),
        AggregationFunctionColumnPair.resolveToStoredType(
            AggregationFunctionColumnPair.fromColumnName("count__*")));
    assertEquals(AggregationFunctionColumnPair.fromColumnName("sum__dimY"),
        AggregationFunctionColumnPair.resolveToStoredType(
            AggregationFunctionColumnPair.fromColumnName("sum__dimY")));
  }

  @Test
  public void testGetValueAggregationType() {
    assertEquals(getStoredType(AggregationFunctionType.DISTINCTCOUNTRAWHLL), AggregationFunctionType.DISTINCTCOUNTHLL);
    assertEquals(getStoredType(AggregationFunctionType.PERCENTILERAWTDIGEST),
        AggregationFunctionType.PERCENTILETDIGEST);
    assertEquals(getStoredType(AggregationFunctionType.DISTINCTCOUNTRAWTHETASKETCH),
        AggregationFunctionType.DISTINCTCOUNTTHETASKETCH);
    assertEquals(getStoredType(AggregationFunctionType.DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH),
        AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH);
    assertEquals(getStoredType(AggregationFunctionType.SUMVALUESINTEGERSUMTUPLESKETCH),
        AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH);
    assertEquals(getStoredType(AggregationFunctionType.AVGVALUEINTEGERSUMTUPLESKETCH),
        AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH);
    assertEquals(getStoredType(AggregationFunctionType.DISTINCTCOUNTHLLPLUS),
        AggregationFunctionType.DISTINCTCOUNTHLLPLUS);
    // Default case
    assertEquals(getStoredType(AggregationFunctionType.COUNT), AggregationFunctionType.COUNT);
  }
}
