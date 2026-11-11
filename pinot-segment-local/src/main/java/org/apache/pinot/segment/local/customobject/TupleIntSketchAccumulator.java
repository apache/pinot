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
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.TupleUnion;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.datasketches.tuple.aninteger.IntegerSummarySetOperations;


/// Buffers integer tuple sketches and reuses a union with the configured summary operations and nominal entries.
/// Instances are mutable and not thread-safe.
public class TupleIntSketchAccumulator extends CustomObjectAccumulator<TupleSketch<IntegerSummary>> {
  private final IntegerSummarySetOperations _setOperations;
  private final int _nominalEntries;
  private TupleUnion<IntegerSummary> _union;

  public TupleIntSketchAccumulator(IntegerSummarySetOperations setOperations, int nominalEntries, int threshold) {
    super(threshold);
    _nominalEntries = nominalEntries;
    _setOperations = setOperations;
  }

  /// Retains a deserialized sketch without unioning it, using the receiving query's configuration for future merges.
  public TupleIntSketchAccumulator(IntegerSummarySetOperations setOperations, int nominalEntries, int threshold,
      TupleSketch<IntegerSummary> initialSketch) {
    super(threshold, initialSketch);
    _nominalEntries = nominalEntries;
    _setOperations = setOperations;
  }

  @Override
  protected void flush() {
    if (_accumulator == null || _accumulator.isEmpty()) {
      return;
    }
    if (_union == null) {
      _union = new TupleUnion<>(_nominalEntries, _setOperations);
    }

    _accumulator.sort(Comparator.comparingDouble(TupleSketch::getTheta));
    for (TupleSketch<IntegerSummary> accumulatedSketch : _accumulator) {
      _union.union(accumulatedSketch);
    }
    _accumulator.clear();
  }

  @Override
  public TupleSketch<IntegerSummary> getResult() {
    // Return a single pending sketch unchanged. Its nominal entries are not adjusted until a union is needed.
    if (getNumInputs() == 1 && _accumulator.size() == 1) {
      return _accumulator.get(0);
    }
    if (_union == null) {
      _union = new TupleUnion<>(_nominalEntries, _setOperations);
    }
    flush();
    return _union.getResult();
  }
}
