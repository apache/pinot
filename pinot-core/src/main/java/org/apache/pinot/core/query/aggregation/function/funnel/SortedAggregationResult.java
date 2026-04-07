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

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Aggregation result data structure leveraged by sorted aggregation strategy.
 *
 * <p>For single-key, uses simple last-ID tracking since data is sorted by the
 * correlation column. For multi-key, data is sorted by the primary (first)
 * correlation column only; secondary keys are tracked via a HashMap within
 * each primary-key group.
 */
class SortedAggregationResult {
  final int _numSteps;
  final long[] _stepCounters;
  private final int _numKeys;

  // Single-key tracking
  final boolean[] _correlatedSteps;
  int _lastCorrelationId = Integer.MIN_VALUE;

  // Multi-key tracking
  private int _lastPrimaryId = Integer.MIN_VALUE;
  private Map<IntArrayList, boolean[]> _secondaryKeySteps;
  private final IntArrayList _lookupKey;

  SortedAggregationResult(int numSteps) {
    this(numSteps, 1);
  }

  SortedAggregationResult(int numSteps, int numKeys) {
    _numSteps = numSteps;
    _numKeys = numKeys;
    _stepCounters = new long[_numSteps];
    if (numKeys == 1) {
      _correlatedSteps = new boolean[_numSteps];
      _lookupKey = null;
    } else {
      _correlatedSteps = null;
      _secondaryKeySteps = new HashMap<>();
      _lookupKey = new IntArrayList(numKeys - 1);
    }
  }

  public void add(int step, int correlationId) {
    if (correlationId != _lastCorrelationId) {
      incrStepCounters();
      for (int n = 0; n < _numSteps; n++) {
        _correlatedSteps[n] = false;
      }
      _lastCorrelationId = correlationId;
    }
    _correlatedSteps[step] = true;
  }

  /**
   * Multi-key add. Data must be sorted by correlationIds[0] (primary key).
   * Secondary keys are tracked via HashMap within each primary-key group.
   *
   * <p>Within a primary-key group, rows for the same (primary, secondary) combination may appear
   * non-contiguously (e.g. interleaved with other secondary keys). This is handled correctly
   * because the HashMap accumulates all step observations per secondary key regardless of row order.
   */
  public void addMultiKey(int step, int[] correlationIds) {
    int primaryId = correlationIds[0];
    if (primaryId != _lastPrimaryId) {
      flushMultiKeyGroup();
      _lastPrimaryId = primaryId;
      _secondaryKeySteps.clear();
    }

    _lookupKey.clear();
    for (int k = 1; k < correlationIds.length; k++) {
      _lookupKey.add(correlationIds[k]);
    }

    boolean[] steps = _secondaryKeySteps.get(_lookupKey);
    if (steps == null) {
      steps = new boolean[_numSteps];
      _secondaryKeySteps.put(new IntArrayList(_lookupKey), steps);
    }
    steps[step] = true;
  }

  private void flushMultiKeyGroup() {
    if (_secondaryKeySteps == null) {
      return;
    }
    for (boolean[] steps : _secondaryKeySteps.values()) {
      for (int n = 0; n < _numSteps; n++) {
        if (!steps[n]) {
          break;
        }
        _stepCounters[n]++;
      }
    }
  }

  void incrStepCounters() {
    if (_correlatedSteps == null) {
      return;
    }
    for (int n = 0; n < _numSteps; n++) {
      if (!_correlatedSteps[n]) {
        break;
      }
      _stepCounters[n]++;
    }
  }

  public LongArrayList extractResult() {
    if (_numKeys > 1) {
      flushMultiKeyGroup();
    } else {
      incrStepCounters();
    }
    return LongArrayList.wrap(_stepCounters);
  }
}
