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
package org.apache.pinot.core.operator.filter.predicate;

public abstract class BaseRawValueBasedPredicateEvaluator extends BasePredicateEvaluator {

  @Override
  public final boolean isDictionaryBased() {
    return false;
  }

  @Override
  public final boolean isAlwaysTrue() {
    return false;
  }

  @Override
  public final boolean isAlwaysFalse() {
    return false;
  }

  @Override
  public final int[] getMatchingDictIds() {
    throw new UnsupportedOperationException();
  }

  @Override
  public final int[] getNonMatchingDictIds() {
    throw new UnsupportedOperationException();
  }

  /**
   * Apply a single-value entry to the predicate.
   *
   * @param value Raw value
   * @return Whether the entry matches the predicate
   */
  @Override
  public boolean applySV(int value) {
    throw new UnsupportedOperationException();
  }

  /**
   * Apply a multi-value entry to the predicate.
   *
   * @param values Array of raw values
   * @param length Number of values in the entry
   * @return Whether the entry matches the predicate
   */
  @SuppressWarnings("Duplicates")
  @Override
  public boolean applyMV(int[] values, int length) {
    if (isExclusive()) {
      for (int i = 0; i < length; i++) {
        if (!applySV(values[i])) {
          return false;
        }
      }
      return true;
    } else {
      for (int i = 0; i < length; i++) {
        if (applySV(values[i])) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean applySV(long value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("Duplicates")
  @Override
  public boolean applyMV(long[] values, int length) {
    if (isExclusive()) {
      for (int i = 0; i < length; i++) {
        if (!applySV(values[i])) {
          return false;
        }
      }
      return true;
    } else {
      for (int i = 0; i < length; i++) {
        if (applySV(values[i])) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean applySV(float value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("Duplicates")
  @Override
  public boolean applyMV(float[] values, int length) {
    if (isExclusive()) {
      for (int i = 0; i < length; i++) {
        if (!applySV(values[i])) {
          return false;
        }
      }
      return true;
    } else {
      for (int i = 0; i < length; i++) {
        if (applySV(values[i])) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean applySV(double value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("Duplicates")
  @Override
  public boolean applyMV(double[] values, int length) {
    if (isExclusive()) {
      for (int i = 0; i < length; i++) {
        if (!applySV(values[i])) {
          return false;
        }
      }
      return true;
    } else {
      for (int i = 0; i < length; i++) {
        if (applySV(values[i])) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean applySV(String value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("Duplicates")
  @Override
  public boolean applyMV(String[] values, int length) {
    if (isExclusive()) {
      for (int i = 0; i < length; i++) {
        if (!applySV(values[i])) {
          return false;
        }
      }
      return true;
    } else {
      for (int i = 0; i < length; i++) {
        if (applySV(values[i])) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public boolean applySV(byte[] value) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("Duplicates")
  @Override
  public boolean applyMV(byte[][] values, int length) {
    if (isExclusive()) {
      for (int i = 0; i < length; i++) {
        if (!applySV(values[i])) {
          return false;
        }
      }
      return true;
    } else {
      for (int i = 0; i < length; i++) {
        if (applySV(values[i])) {
          return true;
        }
      }
      return false;
    }
  }
}
