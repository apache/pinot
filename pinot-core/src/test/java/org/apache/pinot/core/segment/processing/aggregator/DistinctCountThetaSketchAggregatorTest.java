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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

  @Test
  public void testSupportsBatchAggregation() {
    assertTrue(_thetaSketchAggregator.supportsBatchAggregation());
  }

  @Test
  public void testAggregateBatchWithNull() {
    Map<String, String> functionParameters = new HashMap<>();
    assertNull(_thetaSketchAggregator.aggregateBatch(null, functionParameters));
    assertNull(_thetaSketchAggregator.aggregateBatch(new ArrayList<>(), functionParameters));
  }

  @Test
  public void testAggregateBatchWithSingleValue() {
    Sketch sketch = createThetaSketch(64);
    byte[] value = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(sketch);
    List<Object> values = new ArrayList<>();
    values.add(value);
    Map<String, String> functionParameters = new HashMap<>();

    byte[] result = (byte[]) _thetaSketchAggregator.aggregateBatch(values, functionParameters);

    // Single value should be returned unchanged
    assertNotNull(result);
    Sketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(result);
    assertEquals(resultSketch.getRetainedEntries(), 64);
  }

  @Test
  public void testAggregateBatchWithMultipleValues() {
    // Create 5 sketches with distinct values
    List<Object> values = new ArrayList<>();
    int totalDistinct = 0;
    for (int i = 0; i < 5; i++) {
      Sketch sketch = createThetaSketchWithOffset(64, i * 100);
      values.add(ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(sketch));
      totalDistinct += 64;
    }
    Map<String, String> functionParameters = new HashMap<>();

    byte[] result = (byte[]) _thetaSketchAggregator.aggregateBatch(values, functionParameters);

    Sketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    // All values are distinct, so union should have all entries
    assertEquals(resultSketch.getRetainedEntries(), totalDistinct);
  }

  @Test
  public void testAggregateBatchMatchesPairwise() {
    // Verify batch aggregation produces same result as pairwise
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Sketch sketch = createThetaSketchWithOffset(32, i * 50);
      values.add(ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(sketch));
    }
    Map<String, String> functionParameters = new HashMap<>();

    // Batch aggregation
    byte[] batchResult = (byte[]) _thetaSketchAggregator.aggregateBatch(values, functionParameters);

    // Pairwise aggregation
    Object pairwiseResult = values.get(0);
    for (int i = 1; i < values.size(); i++) {
      pairwiseResult = _thetaSketchAggregator.aggregate(pairwiseResult, values.get(i), functionParameters);
    }

    Sketch batchSketch = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(batchResult);
    Sketch pairwiseSketch = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize((byte[]) pairwiseResult);

    // Results should be equivalent
    assertEquals(batchSketch.getRetainedEntries(), pairwiseSketch.getRetainedEntries());
    assertEquals(batchSketch.getEstimate(), pairwiseSketch.getEstimate(), 0.001);
  }

  @Test
  public void testAggregateBatchWithNominalEntries() {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      Sketch sketch = createThetaSketchWithOffset(64, i * 100);
      values.add(ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(sketch));
    }
    Map<String, String> functionParameters = new HashMap<>();
    functionParameters.put(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES, "128");

    byte[] result = (byte[]) _thetaSketchAggregator.aggregateBatch(values, functionParameters);

    Sketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    // With nominal entries of 128, result should be capped
    assertTrue(resultSketch.getRetainedEntries() <= 128);
  }

  private Sketch createThetaSketch(int nominalEntries) {
    return createThetaSketchWithOffset(nominalEntries, 0);
  }

  private Sketch createThetaSketchWithOffset(int nominalEntries, int offset) {
    UpdateSketch updateSketch = UpdateSketch.builder().setNominalEntries(nominalEntries).build();
    for (int i = 0; i < nominalEntries; i++) {
      updateSketch.update(i + offset);
    }
    return updateSketch.compact();
  }
}
