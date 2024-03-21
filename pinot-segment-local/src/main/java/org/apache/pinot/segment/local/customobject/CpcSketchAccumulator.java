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

import javax.annotation.Nonnull;
import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;


/**
 * Intermediate state used by {@code DistinctCountCPCSketchAggregationFunction} which gives
 * the end user more control over how sketches are merged for performance.
 * The end user can set parameters that trade-off more memory usage for more pre-aggregation.
 */
public class CpcSketchAccumulator extends CustomObjectAccumulator<CpcSketch> {
  private int _lgNominalEntries = 4;
  private CpcUnion _union;

  public CpcSketchAccumulator() {
  }

  // Note: The accumulator is serialized as a sketch.  This means that the majority of the processing
  // happens on serialization. Therefore, when deserialized, the values may be null and will
  // require re-initialisation. Since the primary use case is at query time for the Broker
  // and Server, these properties are already in memory and are re-set.
  public CpcSketchAccumulator(int lgNominalEntries, int threshold) {
    super(threshold);
    _lgNominalEntries = lgNominalEntries;
  }

  public void setLgNominalEntries(int lgNominalEntries) {
    _lgNominalEntries = lgNominalEntries;
  }

  @Nonnull
  @Override
  public CpcSketch getResult() {
    return unionAll();
  }

  private CpcSketch unionAll() {
    if (_union == null) {
      _union = new CpcUnion(_lgNominalEntries);
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

    for (CpcSketch accumulatedSketch : _accumulator) {
      _union.update(accumulatedSketch);
    }

    return _union.getResult();
  }
}
