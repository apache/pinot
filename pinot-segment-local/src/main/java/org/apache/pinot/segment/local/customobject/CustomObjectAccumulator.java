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
import javax.annotation.Nonnull;


/**
 * Intermediate state used by some aggregation functions which gives the end user more control over how custom objects
 * are merged for performance reasons.  Some custom objects such as DataSketches have better merge performance when
 * more than two items are merged at once through the elimination of intermediate bookkeeping overheads.
 *
 * The end user can set a value for the "threshold" parameter that defers the merge operation until at least as many
 * items are ready to be merged, or the callee forces the merge directly via "getResult" - e.g. on serialization.
 * This data structure trades-off more memory usage for a greater degree of pre-aggregation in the accumulator state.
 */
public abstract class CustomObjectAccumulator<T> {
  protected ArrayList<T> _accumulator;
  private int _threshold;
  private int _numInputs = 0;

  public CustomObjectAccumulator() {
    this(2);
  }

  public CustomObjectAccumulator(int threshold) {
    setThreshold(threshold);
  }

  /**
   * Sets the threshold that determines how much memory to use for the internal accumulator state before
   * the intermediate state is merged.
   * @param threshold the threshold [> 0].
   */
  public void setThreshold(int threshold) {
    Preconditions.checkArgument(threshold > 0, "Invalid threshold: %s, must be positive", threshold);
    _threshold = threshold;
  }

  /**
   * Returns the configured threshold for this accumulator.
   */
  public int getThreshold() {
    return _threshold;
  }

  /**
   * Returns the number of inputs that have been added to the accumulator state.
   */
  public int getNumInputs() {
    return _numInputs;
  }

  /**
   * Returns true if no inputs have been added to the accumulator state.
   */
  public boolean isEmpty() {
    return _numInputs == 0;
  }

  @Nonnull
  /**
   * Forces the item T in internal state to be merged with all pending items in the accumulator state
   * and returns the result.  This should not result in the accumulator state being updated or cleared.
   * @return T result of the merge.
   */
  public abstract T getResult();

  /**
   * Merges another accumulator with this one, by extracting the result from "other".
   * @param other the custom object accumulator to merge.
   */
  public void merge(CustomObjectAccumulator<T> other) {
    if (other.isEmpty()) {
      return;
    }
    T result = other.getResult();
    applyInternal(result);
  }

  /**
   * Adds a new item to the accumulator state.  If the accumulator state is equal to the threshold value,
   * the internal state is updated and the accumulator state is cleared.
   * @param item the item to add to the accumulator state, cannot be null.
   */
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
      getResult();
      _accumulator.clear();
    }
  }
}
