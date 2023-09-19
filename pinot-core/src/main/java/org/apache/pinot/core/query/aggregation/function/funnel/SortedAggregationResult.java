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


/**
 * Aggregation result data structure leveraged by sorted aggregation strategy.
 */
class SortedAggregationResult {
  final int _numSteps;
  final long[] _stepCounters;
  final boolean[] _correlatedSteps;
  int _lastCorrelationId = Integer.MIN_VALUE;

  SortedAggregationResult(int numSteps) {
    _numSteps = numSteps;
    _stepCounters = new long[_numSteps];
    _correlatedSteps = new boolean[_numSteps];
  }

  public void add(int step, int correlationId) {
    if (correlationId != _lastCorrelationId) {
      // End of correlation group, calculate funnel conversion counts
      incrStepCounters();

      // initialize next correlation group
      for (int n = 0; n < _numSteps; n++) {
        _correlatedSteps[n] = false;
      }
      _lastCorrelationId = correlationId;
    }
    _correlatedSteps[step] = true;
  }

  void incrStepCounters() {
    for (int n = 0; n < _numSteps; n++) {
      if (!_correlatedSteps[n]) {
        break;
      }
      _stepCounters[n]++;
    }
  }

  public LongArrayList extractResult() {
    // count last correlation id left open
    incrStepCounters();
    return LongArrayList.wrap(_stepCounters);
  }
}
