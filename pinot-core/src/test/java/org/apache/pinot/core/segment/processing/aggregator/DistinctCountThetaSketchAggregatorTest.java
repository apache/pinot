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
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.Constants;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class DistinctCountThetaSketchAggregatorTest {

  private DistinctCountThetaSketchAggregator _thetaSketchAggregator;

  @BeforeMethod
  public void setUp() {
    _thetaSketchAggregator = new DistinctCountThetaSketchAggregator();
  }

  @Test
  public void testAggregateWithDefaultBehaviour() {
    Sketch firstSketch = createThetaSketch(64);
    Sketch secondSketch = createThetaSketch(32);
    byte[] value1 = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(firstSketch);
    byte[] value2 = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(secondSketch);
    Map<String, String> functionParameters = new HashMap<>();

    byte[] result = (byte[]) _thetaSketchAggregator.aggregate(value1, value2, functionParameters);

    Sketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    assertEquals(resultSketch.getRetainedEntries(), 64);
  }

  @Test
  public void testAggregateWithNominalEntries() {
    Sketch firstSketch = createThetaSketch(64);
    Sketch secondSketch = createThetaSketch(32);
    byte[] value1 = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(firstSketch);
    byte[] value2 = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(secondSketch);

    Map<String, String> functionParameters = new HashMap<>();
    functionParameters.put(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "32");

    byte[] result = (byte[]) _thetaSketchAggregator.aggregate(value1, value2, functionParameters);

    Sketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    assertEquals(resultSketch.getRetainedEntries(), 32);
  }

  @Test
  public void testAggregateWithSamplingProbability() {
    Sketch firstSketch = createThetaSketch(64);
    Sketch secondSketch = createThetaSketch(32);
    byte[] value1 = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(firstSketch);
    byte[] value2 = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(secondSketch);

    Map<String, String> functionParameters = new HashMap<>();
    functionParameters.put(Constants.THETA_TUPLE_SKETCH_SAMPLING_PROBABILITY, "0.1");

    byte[] result = (byte[]) _thetaSketchAggregator.aggregate(value1, value2, functionParameters);

    Sketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    assertTrue(resultSketch.getRetainedEntries() < 64);
  }

  private Sketch createThetaSketch(int nominalEntries) {
    UpdateSketch updateSketch = UpdateSketch.builder().setNominalEntries(nominalEntries).build();
    for (int i = 0; i < nominalEntries; i++) {
      updateSketch.update(i);
    }
    return updateSketch.compact();
  }
}
