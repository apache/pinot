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
import java.util.Arrays;


/**
 * Aggregation result data structure leveraged by sorted aggregation strategy.
 *
 * <p>For single-key, uses simple last-ID tracking since data is sorted by the correlation column.
 * For multi-key, data is sorted by the primary (first) correlation column only; secondary keys
 * are tracked via pre-allocated flat arrays within each primary-key group.
 */
class SortedAggregationResult {
  private static final int INITIAL_CAPACITY = 8;

  final int _numSteps;
  final long[] _stepCounters;
  private final int _numKeys;

  // Single-key tracking
  final boolean[] _correlatedSteps;
  int _lastCorrelationId = Integer.MIN_VALUE;

  // Multi-key tracking — flat arrays, pre-allocated once and reused across groups
  private int _lastPrimaryId = Integer.MIN_VALUE;
  private int[][] _entryKeys;
  private boolean[][] _entrySteps;
  private int _entryCount;

  SortedAggregationResult(int numSteps) {
    this(numSteps, 1);
  }

  SortedAggregationResult(int numSteps, int numKeys) {
    _numSteps = numSteps;
    _numKeys = numKeys;
    _stepCounters = new long[numSteps];
    _correlatedSteps = numKeys == 1 ? new boolean[numSteps] : null;
    _entryKeys = numKeys > 1 ? new int[INITIAL_CAPACITY][numKeys] : null;
    _entrySteps = numKeys > 1 ? new boolean[INITIAL_CAPACITY][numSteps] : null;
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
   * Secondary keys are tracked via linear scan over pre-allocated flat arrays.
   *
   * <p>The full correlationIds array (including the primary key at index 0) is used as the
   * lookup key. The primary key is the same for every entry within a group, so including it
   * is redundant but harmless — it avoids the cost of copying a sub-array.
   */
  public void addMultiKey(int step, int[] correlationIds) {
    int primaryId = correlationIds[0];
    if (primaryId != _lastPrimaryId) {
      flushMultiKeyGroup();
      _lastPrimaryId = primaryId;
      _entryCount = 0;
    }

    for (int i = 0; i < _entryCount; i++) {
      if (keysMatch(_entryKeys[i], correlationIds)) {
        _entrySteps[i][step] = true;
        return;
      }
    }

    ensureCapacity();
    System.arraycopy(correlationIds, 0, _entryKeys[_entryCount], 0, _numKeys);
    Arrays.fill(_entrySteps[_entryCount], false);
    _entrySteps[_entryCount][step] = true;
    _entryCount++;
  }

  private boolean keysMatch(int[] stored, int[] incoming) {
    for (int i = 0; i < _numKeys; i++) {
      if (stored[i] != incoming[i]) {
        return false;
      }
    }
    return true;
  }

  private void ensureCapacity() {
    if (_entryCount < _entryKeys.length) {
      return;
    }
    int oldCap = _entryKeys.length;
    int newCap = oldCap * 2;
    _entryKeys = Arrays.copyOf(_entryKeys, newCap);
    _entrySteps = Arrays.copyOf(_entrySteps, newCap);
    for (int i = oldCap; i < newCap; i++) {
      _entryKeys[i] = new int[_numKeys];
      _entrySteps[i] = new boolean[_numSteps];
    }
  }

  private void flushMultiKeyGroup() {
    for (int i = 0; i < _entryCount; i++) {
      for (int n = 0; n < _numSteps; n++) {
        if (!_entrySteps[i][n]) {
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

  /**
   * Extracts the final funnel result. Must be called exactly once.
   */
  public LongArrayList extractResult() {
    if (_numKeys > 1) {
      flushMultiKeyGroup();
      _entryCount = 0;
    } else {
      incrStepCounters();
    }
    return LongArrayList.wrap(_stepCounters);
  }
}
