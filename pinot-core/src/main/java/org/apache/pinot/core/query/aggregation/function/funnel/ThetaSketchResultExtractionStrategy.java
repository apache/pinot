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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;


class ThetaSketchResultExtractionStrategy implements ResultExtractionStrategy<UpdateSketch[], List<Sketch>> {
  private static final Sketch EMPTY_SKETCH = new UpdateSketchBuilder().build().compact();

  protected final int _numSteps;

  ThetaSketchResultExtractionStrategy(int numSteps) {
    _numSteps = numSteps;
  }

  @Override
  public List<Sketch> extractIntermediateResult(UpdateSketch[] stepsSketches) {
    if (stepsSketches == null) {
      return Collections.nCopies(_numSteps, EMPTY_SKETCH);
    }
    return Arrays.asList(stepsSketches);
  }
}
