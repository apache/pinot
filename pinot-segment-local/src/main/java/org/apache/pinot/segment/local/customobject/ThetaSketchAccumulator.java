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

import java.util.ArrayList;
import java.util.Comparator;
import javax.annotation.Nonnull;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;


/**
 * Intermediate state used by {@code DistinctCountThetaSketchAggregationFunction} which gives
 * the end user more control over how sketches are merged for performance.
 * The end user can set parameters that trade-off more memory usage for more pre-aggregation.
 * This permits use of the Union "early-stop" optimisation where ordered sketches require no further
 * processing beyond the minimum Theta value.
 * The union operation initialises an empty "gadget" bookkeeping sketch that is updated with hashed entries
 * that fall below the minimum Theta value for all input sketches ("Broder Rule").  When the initial
 * Theta value is set to the minimum immediately, further gains can be realised.
 */
public class ThetaSketchAccumulator {
  private ArrayList<Sketch> _accumulator;
  private SetOperationBuilder _setOperationBuilder = new SetOperationBuilder();
  private Union _union;
  private int _threshold;
  private int _numInputs = 0;

  public ThetaSketchAccumulator() {
  }

  // Note: The accumulator is serialized as a sketch.  This means that the majority of the processing
  // happens on serialization. Therefore, when deserialized, the values may be null and will
  // require re-initialisation. Since the primary use case is at query time for the Broker
  // and Server, these properties are already in memory and are re-set.
  public ThetaSketchAccumulator(SetOperationBuilder setOperationBuilder, int threshold) {
    _setOperationBuilder = setOperationBuilder;
    _threshold = threshold;
  }

  public void setSetOperationBuilder(SetOperationBuilder setOperationBuilder) {
    _setOperationBuilder = setOperationBuilder;
  }

  public void setThreshold(int threshold) {
    _threshold = threshold;
  }

  public boolean isEmpty() {
    return _numInputs == 0;
  }

  @Nonnull
  public Sketch getResult() {
    return unionAll();
  }

  public void apply(Sketch sketch) {
    internalAdd(sketch);
  }

  public void merge(ThetaSketchAccumulator thetaUnion) {
    if (thetaUnion.isEmpty()) {
      return;
    }
    Sketch sketch = thetaUnion.getResult();
    internalAdd(sketch);
  }

  private void internalAdd(Sketch sketch) {
    if (sketch.isEmpty()) {
      return;
    }
    if (_accumulator == null) {
      _accumulator = new ArrayList<>(_threshold);
    }
    _accumulator.add(sketch);
    _numInputs += 1;

    if (_accumulator.size() >= _threshold) {
      unionAll();
    }
  }

  private Sketch unionAll() {
    if (_union == null) {
      _union = _setOperationBuilder.buildUnion();
    }
    // Return the default update "gadget" sketch as a compact sketch
    if (isEmpty()) {
      return _union.getResult(false, null);
    }
    // Corner-case: the parameters are not strictly respected when there is a single sketch.
    // This single sketch might have been the result of a previously accumulated union and
    // would already have the parameters set.  The sketch is returned as-is without adjusting
    // nominal entries which requires an additional union operation.
    if (_numInputs == 1) {
      return _accumulator.get(0);
    }

    // Performance optimization: ensure that the minimum Theta is used for "early stop".
    // The "early stop" optimization is implemented in the Apache Datasketches Union operation for
    // ordered and compact Theta sketches. Internally, a compact and ordered Theta sketch can be
    // compared to a sorted array of K items.  When performing a union, only those items from
    // the input sketch less than Theta need to be processed.  The loop terminates as soon as a hash
    // is seen that is > Theta.
    // The following "sort" improves on this further by selecting the minimal Theta value up-front,
    // which results in fewer redundant entries being retained and subsequently discarded during the
    // union operation.
    _accumulator.sort(Comparator.comparingDouble(Sketch::getTheta));
    for (Sketch accumulatedSketch : _accumulator) {
      _union.union(accumulatedSketch);
    }
    _accumulator.clear();

    return _union.getResult(false, null);
  }
}
