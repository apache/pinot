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

  @Test
  public void testSupportsBatchAggregation() {
    assertTrue(_cpcSketchAggregator.supportsBatchAggregation());
  }

  @Test
  public void testAggregateBatchWithNull() {
    Map<String, String> functionParameters = new HashMap<>();
    assertNull(_cpcSketchAggregator.aggregateBatch(null, functionParameters));
    assertNull(_cpcSketchAggregator.aggregateBatch(new ArrayList<>(), functionParameters));
  }

  @Test
  public void testAggregateBatchWithSingleValue() {
    CpcSketch sketch = createCpcSketch(12, 100);
    byte[] value = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(sketch);
    List<Object> values = new ArrayList<>();
    values.add(value);
    Map<String, String> functionParameters = new HashMap<>();

    byte[] result = (byte[]) _cpcSketchAggregator.aggregateBatch(values, functionParameters);

    assertNotNull(result);
    CpcSketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(result);
    assertEquals(resultSketch.getEstimate(), 100.0, 10.0);
  }

  @Test
  public void testAggregateBatchWithMultipleValues() {
    List<Object> values = new ArrayList<>();
    int totalDistinct = 0;
    for (int i = 0; i < 5; i++) {
      CpcSketch sketch = createCpcSketchWithOffset(12, 100, i * 200);
      values.add(ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(sketch));
      totalDistinct += 100;
    }
    Map<String, String> functionParameters = new HashMap<>();

    byte[] result = (byte[]) _cpcSketchAggregator.aggregateBatch(values, functionParameters);

    CpcSketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    // CPC provides estimate, allow some error margin
    assertEquals(resultSketch.getEstimate(), totalDistinct, totalDistinct * 0.1);
  }

  @Test
  public void testAggregateBatchMatchesPairwise() {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      CpcSketch sketch = createCpcSketchWithOffset(12, 50, i * 100);
      values.add(ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(sketch));
    }
    Map<String, String> functionParameters = new HashMap<>();

    // Batch aggregation
    byte[] batchResult = (byte[]) _cpcSketchAggregator.aggregateBatch(values, functionParameters);

    // Pairwise aggregation
    Object pairwiseResult = values.get(0);
    for (int i = 1; i < values.size(); i++) {
      pairwiseResult = _cpcSketchAggregator.aggregate(pairwiseResult, values.get(i), functionParameters);
    }

    CpcSketch batchSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(batchResult);
    CpcSketch pairwiseSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize((byte[]) pairwiseResult);

    // Results should be equivalent (CPC is probabilistic, allow small difference)
    assertEquals(batchSketch.getEstimate(), pairwiseSketch.getEstimate(), pairwiseSketch.getEstimate() * 0.01);
  }

  @Test
  public void testAggregateBatchWithLgK() {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      // Use lgK=14 for input sketches to match the expected output lgK
      CpcSketch sketch = createCpcSketchWithOffset(14, 100, i * 200);
      values.add(ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.serialize(sketch));
    }
    Map<String, String> functionParameters = new HashMap<>();
    functionParameters.put(Constants.CPCSKETCH_LGK_KEY, "14");

    byte[] result = (byte[]) _cpcSketchAggregator.aggregateBatch(values, functionParameters);

    CpcSketch resultSketch = ObjectSerDeUtils.DATA_SKETCH_CPC_SER_DE.deserialize(result);
    assertNotNull(resultSketch);
    assertEquals(resultSketch.getLgK(), 14);
  }

  private CpcSketch createCpcSketch(int lgK, int numDistinct) {
    return createCpcSketchWithOffset(lgK, numDistinct, 0);
  }

  private CpcSketch createCpcSketchWithOffset(int lgK, int numDistinct, int offset) {
    CpcSketch sketch = new CpcSketch(lgK);
    for (int i = 0; i < numDistinct; i++) {
      sketch.update(i + offset);
    }
    return sketch;
  }
}
