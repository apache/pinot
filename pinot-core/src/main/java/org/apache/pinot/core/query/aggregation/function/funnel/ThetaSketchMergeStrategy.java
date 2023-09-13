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
package org.apache.pinot.core.query.aggregation.function.funnel;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.List;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;


class ThetaSketchMergeStrategy implements MergeStrategy<List<Sketch>> {
  protected final int _numSteps;
  final SetOperationBuilder _setOperationBuilder;

  ThetaSketchMergeStrategy(int numSteps, int nominalEntries) {
    _numSteps = numSteps;
    _setOperationBuilder = new SetOperationBuilder().setNominalEntries(nominalEntries);
  }

  @Override
  public List<Sketch> merge(List<Sketch> sketches1, List<Sketch> sketches2) {
    final List<Sketch> mergedSketches = new ArrayList<>(_numSteps);
    for (int i = 0; i < _numSteps; i++) {
      // NOTE: Compact the sketch in unsorted, on-heap fashion for performance concern.
      //       See https://datasketches.apache.org/docs/Theta/ThetaSize.html for more details.
      mergedSketches.add(_setOperationBuilder.buildUnion().union(sketches1.get(i), sketches2.get(i), false, null));
    }
    return mergedSketches;
  }

  @Override
  public LongArrayList extractFinalResult(List<Sketch> sketches) {
    long[] result = new long[_numSteps];

    Sketch sketch = sketches.get(0);
    result[0] = Math.round(sketch.getEstimate());
    for (int i = 1; i < _numSteps; i++) {
      Intersection intersection = _setOperationBuilder.buildIntersection();
      sketch = intersection.intersect(sketch, sketches.get(i));
      result[i] = Math.round(sketch.getEstimate());
    }
    return LongArrayList.wrap(result);
  }
}
