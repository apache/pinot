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

import com.google.common.base.Preconditions;
import java.util.ArrayList;


/// Buffers intermediate values until the configured threshold is reached or a result is requested.
/// Subclasses merge pending values in [#flush()]. Instances are mutable and not thread-safe.
public abstract class CustomObjectAccumulator<T> {
  protected ArrayList<T> _accumulator;
  private final int _threshold;
  private int _numInputs = 0;

  protected CustomObjectAccumulator(int threshold) {
    Preconditions.checkArgument(threshold > 0, "Invalid threshold: %s, must be positive", threshold);
    _threshold = threshold;
  }

  /// Retains an initial result without flushing, even when the threshold is one. A deserialized sketch can then
  /// be returned unchanged until another input is added, rather than being unioned again during deserialization.
  protected CustomObjectAccumulator(int threshold, T initialResult) {
    this(threshold);
    _accumulator = new ArrayList<>(1);
    _accumulator.add(Preconditions.checkNotNull(initialResult));
    _numInputs = 1;
  }

  /// Returns the configured threshold for this accumulator.
  public int getThreshold() {
    return _threshold;
  }

  /// Returns the number of inputs that have been added to the accumulator state.
  public int getNumInputs() {
    return _numInputs;
  }

  /// Returns true if no inputs have been added to the accumulator state.
  public boolean isEmpty() {
    return _numInputs == 0;
  }

  /// Returns the accumulated result, merging pending inputs as needed.
  public abstract T getResult();

  /// Merges the pending items into the internal state and clears them, without building a result.
  protected abstract void flush();

  /// Merges another accumulator with this one, by extracting the result from "other".
  /// @param other the custom object accumulator to merge.
  public void merge(CustomObjectAccumulator<T> other) {
    if (other.isEmpty()) {
      return;
    }
    T result = other.getResult();
    applyInternal(result);
  }

  /// Adds an input and flushes pending inputs when their count reaches the threshold.
  /// @param item the item to add to the accumulator state, cannot be null.
  public void apply(T item) {
    Preconditions.checkNotNull(item);
    applyInternal(item);
  }

  private void applyInternal(T item) {
    if (_accumulator == null) {
      _accumulator = new ArrayList<>(_threshold);
    }
    _accumulator.add(item);
    _numInputs += 1;

    if (_accumulator.size() >= _threshold) {
      flush();
    }
  }
}
