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
package org.apache.pinot.segment.local.customobject;

import java.util.Comparator;
import org.apache.datasketches.theta.ThetaSetOperationBuilder;
import org.apache.datasketches.theta.ThetaSketch;
import org.apache.datasketches.theta.ThetaUnion;


/// Buffers theta sketches and unions pending inputs in ascending theta order to reduce intermediate retained entries.
/// Instances are mutable and not thread-safe. The supplied builder must not be modified after construction.
public class ThetaSketchAccumulator extends CustomObjectAccumulator<ThetaSketch> {
  private final ThetaSetOperationBuilder _setOperationBuilder;
  private ThetaUnion _union;

  public ThetaSketchAccumulator(ThetaSetOperationBuilder setOperationBuilder, int threshold) {
    super(threshold);
    _setOperationBuilder = setOperationBuilder;
  }

  /// Retains a deserialized sketch without unioning it, using the receiving query's configuration for future merges.
  public ThetaSketchAccumulator(ThetaSetOperationBuilder setOperationBuilder, int threshold,
      ThetaSketch initialSketch) {
    super(threshold, initialSketch);
    _setOperationBuilder = setOperationBuilder;
  }

  @Override
  protected void flush() {
    if (_accumulator == null || _accumulator.isEmpty()) {
      return;
    }
    if (_union == null) {
      _union = _setOperationBuilder.buildUnion();
    }

    // Start with the lowest theta so later inputs do not retain hashes that would be discarded by that input.
    _accumulator.sort(Comparator.comparingDouble(ThetaSketch::getTheta));
    for (ThetaSketch accumulatedSketch : _accumulator) {
      _union.union(accumulatedSketch);
    }
    _accumulator.clear();
  }

  @Override
  public ThetaSketch getResult() {
    // Return a single pending sketch unchanged. Its nominal entries are not adjusted until a union is needed.
    if (getNumInputs() == 1 && _accumulator.size() == 1) {
      return _accumulator.get(0);
    }
    if (_union == null) {
      _union = _setOperationBuilder.buildUnion();
    }
    flush();
    return _union.getResult(false, null);
  }
}
