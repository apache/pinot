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
import javax.annotation.Nonnull;
import org.apache.datasketches.tuple.Sketch;
import org.apache.datasketches.tuple.Union;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;


/**
 * Intermediate state used by {@code IntegerTupleSketchAggregationFunction} which gives
 * the end user more control over how sketches are merged for performance.
 * In particular, the Theta Sketch Union "early-stop" optimisation can be used - ordered sketches require no further
 * processing beyond the minimum Theta value.  This applies to Tuple sketches because they are an extension of the
 * Theta sketch.
 * The union operation initialises an empty "gadget" bookkeeping sketch that is updated with hashed entries
 * that fall below the minimum Theta value for all input sketches ("Broder Rule").  When the initial Theta value is
 * set to the minimum immediately, further gains can be realised.
 */
public class TupleIntSketchAccumulator extends CustomObjectAccumulator<Sketch<IntegerSummary>> {
  private IntegerSummarySetOperations _setOperations;
  private int _nominalEntries;
  private Union<IntegerSummary> _union;

  public TupleIntSketchAccumulator() {
  }

  // Note: The accumulator is serialized as a sketch.  This means that the majority of the processing
  // happens on serialization. Therefore, when deserialized, the values may be null and will
  // require re-initialisation. Since the primary use case is at query time for the Broker
  // and Server, these properties are already in memory and are re-set.
  public TupleIntSketchAccumulator(IntegerSummarySetOperations setOperations, int nominalEntries, int threshold) {
    super(threshold);
    _nominalEntries = nominalEntries;
    _setOperations = setOperations;
  }

  public void setSetOperations(IntegerSummarySetOperations setOperations) {
    _setOperations = setOperations;
  }

  public void setNominalEntries(int nominalEntries) {
    _nominalEntries = nominalEntries;
  }

  @Nonnull
  @Override
  public Sketch<IntegerSummary> getResult() {
    return unionAll();
  }

  private Sketch<IntegerSummary> unionAll() {
    if (_union == null) {
      _union = new Union<>(_nominalEntries, _setOperations);
    }
    // Return the default update "gadget" sketch as a compact sketch
    if (isEmpty()) {
      return _union.getResult();
    }
    // Corner-case: the parameters are not strictly respected when there is a single sketch.
    // This single sketch might have been the result of a previously accumulated union and
    // would already have the parameters set.  The sketch is returned as-is without adjusting
    // nominal entries which requires an additional union operation.
    if (getNumInputs() == 1) {
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
    for (Sketch<IntegerSummary> accumulatedSketch : _accumulator) {
      _union.union(accumulatedSketch);
    }
    _accumulator.clear();

    return _union.getResult();
  }
}
