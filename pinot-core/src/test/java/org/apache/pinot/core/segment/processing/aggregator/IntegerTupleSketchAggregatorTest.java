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
import org.apache.datasketches.tuple.CompactSketch;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.aninteger.IntegerSketch;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.Constants;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class IntegerTupleSketchAggregatorTest {

  private IntegerTupleSketchAggregator _tupleSketchAggregator;

  @BeforeMethod
  public void setUp() {
    _tupleSketchAggregator = new IntegerTupleSketchAggregator(IntegerSummary.Mode.Max);
  }

  @Test
  public void testAggregateWithDefaultBehaviour() {
    Sketch<IntegerSummary> firstSketch = createTupleSketch(64);
    Sketch<IntegerSummary> secondSketch = createTupleSketch(32);
    byte[] value1 = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(firstSketch);
    byte[] value2 = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(secondSketch);
    Map<String, String> functionParameters = new HashMap<>();

    byte[] result = (byte[]) _tupleSketchAggregator.aggregate(value1, value2, functionParameters);

    Sketch<IntegerSummary> resultSketch = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    assertEquals(resultSketch.getRetainedEntries(), 64);
  }

  @Test
  public void testAggregateWithNominalEntries() {
    Sketch<IntegerSummary> firstSketch = createTupleSketch(64);
    Sketch<IntegerSummary> secondSketch = createTupleSketch(32);
    byte[] value1 = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(firstSketch);
    byte[] value2 = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.serialize(secondSketch);

    Map<String, String> functionParameters = new HashMap<>();
    functionParameters.put(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "32");

    byte[] result = (byte[]) _tupleSketchAggregator.aggregate(value1, value2, functionParameters);

    Sketch<IntegerSummary> resultSketch = ObjectSerDeUtils.DATA_SKETCH_INT_TUPLE_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    assertEquals(resultSketch.getRetainedEntries(), 32);
  }

  private CompactSketch<IntegerSummary> createTupleSketch(int nominalEntries) {
    int lgK = (int) (Math.log(nominalEntries) / Math.log(2));
    IntegerSketch integerSketch = new IntegerSketch(lgK, IntegerSummary.Mode.Max);
    for (int i = 0; i < nominalEntries; i++) {
      integerSketch.update(i, 1);
    }
    return integerSketch.compact();
  }
}
