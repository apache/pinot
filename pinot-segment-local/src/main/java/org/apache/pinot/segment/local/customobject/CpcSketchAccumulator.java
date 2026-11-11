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

import org.apache.datasketches.cpc.CpcSketch;
import org.apache.datasketches.cpc.CpcUnion;


/// Buffers CPC sketches for deferred merges with the configured precision.
/// Instances are mutable and not thread-safe.
public class CpcSketchAccumulator extends CustomObjectAccumulator<CpcSketch> {
  private final int _lgNominalEntries;
  private CpcUnion _union;

  public CpcSketchAccumulator(int lgNominalEntries, int threshold) {
    super(threshold);
    _lgNominalEntries = lgNominalEntries;
  }

  /// Retains a deserialized sketch without unioning it, using the receiving query's configuration for future merges.
  public CpcSketchAccumulator(int lgNominalEntries, int threshold, CpcSketch initialSketch) {
    super(threshold, initialSketch);
    _lgNominalEntries = lgNominalEntries;
  }

  @Override
  protected void flush() {
    if (_accumulator == null || _accumulator.isEmpty()) {
      return;
    }
    if (_union == null) {
      _union = new CpcUnion(_lgNominalEntries);
    }
    for (CpcSketch accumulatedSketch : _accumulator) {
      _union.update(accumulatedSketch);
    }
    _accumulator.clear();
  }

  @Override
  public CpcSketch getResult() {
    // Return a single pending sketch unchanged. Its precision is not adjusted until a union is needed.
    if (getNumInputs() == 1 && _accumulator.size() == 1) {
      return _accumulator.get(0);
    }
    if (_union == null) {
      _union = new CpcUnion(_lgNominalEntries);
    }
    flush();
    return _union.getResult();
  }
}
