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
package org.apache.pinot.core.segment.processing.aggregator;


import java.util.HashMap;
import java.util.Map;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.Constants;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class DistinctCountCPCSketchAggregatorTest {

  private DistinctCountCPCSketchAggregator _cpcSketchAggregator;

  @BeforeMethod
  public void setUp() {
    _cpcSketchAggregator = new DistinctCountCPCSketchAggregator();
  }

  @Test
  public void testAggregateWithDefaultLgK() {
    CpcSketch firstSketch = new CpcSketch(10);
    CpcSketch secondSketch = new CpcSketch(20);
    byte[] value1 = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(firstSketch);
    byte[] value2 = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(secondSketch);

    Map<String, String> functionParameters = new HashMap<>();
    byte[] result = (byte[]) _cpcSketchAggregator.aggregate(value1, value2, functionParameters);

    CpcSketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    assertEquals(resultSketch.getLgK(), 12);
  }

  @Test
  public void testAggregateWithFunctionParameters() {
    CpcSketch firstSketch = new CpcSketch(10);
    CpcSketch secondSketch = new CpcSketch(20);
    byte[] value1 = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(firstSketch);
    byte[] value2 = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(secondSketch);

    Map<String, String> functionParameters = new HashMap<>();
    functionParameters.put(Constants.CPCSKETCH_LGK_KEY, "15");

    byte[] result = (byte[]) _cpcSketchAggregator.aggregate(value1, value2, functionParameters);

    CpcSketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    assertEquals(resultSketch.getLgK(), 15);
  }
}
