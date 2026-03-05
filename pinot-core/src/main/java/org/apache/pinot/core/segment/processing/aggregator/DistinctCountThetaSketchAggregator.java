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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.CommonConstants;


public class DistinctCountThetaSketchAggregator implements ValueAggregator {

  public DistinctCountThetaSketchAggregator() {
  }

  @Override
  public Object aggregate(Object value1, Object value2, Map<String, String> functionParameters) {
    SetOperationBuilder unionBuilder = Union.builder();

    String samplingProbabilityParam = functionParameters.get(Constants.THETA_TUPLE_SKETCH_SAMPLING_PROBABILITY);
    String nominalEntriesParam = functionParameters.get(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES);

    // Check if nominal entries is set
    if (nominalEntriesParam != null) {
      unionBuilder.setNominalEntries(Integer.parseInt(nominalEntriesParam));
    } else {
      // If the functionParameters don't have an explicit nominal entries value set,
      // use the default value for nominal entries
      unionBuilder.setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES);
    }

    // Check if sampling probability is set
    if (samplingProbabilityParam != null) {
      unionBuilder.setP(Float.parseFloat(samplingProbabilityParam));
    }

    Union union = unionBuilder.buildUnion();
    Sketch first = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize((byte[]) value1);
    Sketch second = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize((byte[]) value2);
    Sketch result = union.union(first, second);
    return ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(result);
  }

  @Override
  public boolean supportsBatchAggregation() {
    return true;
  }

  @Override
  public Object aggregateBatch(List<Object> values, Map<String, String> functionParameters) {
    if (values == null || values.isEmpty()) {
      return null;
    }
    if (values.size() == 1) {
      return values.get(0);
    }

    // Build union once for all values
    SetOperationBuilder unionBuilder = Union.builder();
    String nominalEntriesParam = functionParameters.get(Constants.THETA_TUPLE_SKETCH_NOMINAL_ENTRIES);
    if (nominalEntriesParam != null) {
      unionBuilder.setNominalEntries(Integer.parseInt(nominalEntriesParam));
    } else {
      unionBuilder.setNominalEntries(CommonConstants.Helix.DEFAULT_THETA_SKETCH_NOMINAL_ENTRIES);
    }
    String samplingProbabilityParam = functionParameters.get(Constants.THETA_TUPLE_SKETCH_SAMPLING_PROBABILITY);
    if (samplingProbabilityParam != null) {
      unionBuilder.setP(Float.parseFloat(samplingProbabilityParam));
    }
    Union union = unionBuilder.buildUnion();

    // Wrap all sketches using zero-copy when possible
    List<Sketch> sketches = new ArrayList<>(values.size());
    for (Object value : values) {
      Sketch sketch = Sketch.wrap(Memory.wrap((byte[]) value));
      sketches.add(sketch);
    }

    // Sort by theta (ascending) for early-stop optimization (Broder's algorithm)
    // Sketches with smaller theta values are more selective and should be processed first
    sketches.sort(Comparator.comparingDouble(Sketch::getTheta));

    // Union all sketches
    for (Sketch sketch : sketches) {
      union.union(sketch);
    }

    return ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.serialize(union.getResult());
  }
}
