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

import java.math.BigDecimal;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.spi.data.FieldSpec.DataType;


public interface PredicateEvaluator {

  /**
   * APIs for both dictionary based and raw value based predicate evaluator
   */

  /**
   * Get the predicate type.
   */
  Predicate.Type getPredicateType();

  /**
   * Return whether the predicate evaluator is dictionary based or raw value based.
   */
  boolean isDictionaryBased();

  /**
   * Returns the data type expected by the PredicateEvaluator. Returns INT for dictionary-based PredicateEvaluator.
   */
  DataType getDataType();

  /**
   * Return whether the predicate is exclusive (e.g. NEQ, NOT_IN).
   */
  boolean isExclusive();

  /**
   * Return whether the predicate will always be evaluated as true.
   */
  boolean isAlwaysTrue();

  /**
   * Return whether the predicate will always be evaluated as false.
   */
  boolean isAlwaysFalse();

  /**
   * Apply a single-value entry to the predicate.
   *
   * @param value Dictionary id or raw value
   * @return Whether the entry matches the predicate
   */
  boolean applySV(int value);

  /**
   * Apply a multi-value entry to the predicate.
   *
   * @param values Array of dictionary ids or raw values
   * @param length Number of values in the entry
   * @return Whether the entry matches the predicate
   */
  boolean applyMV(int[] values, int length);

  /**
   * APIs for dictionary based predicate evaluator
   */

  /**
   * Get the number of matching dictionary ids.
   */
  int getNumMatchingDictIds();

  /**
   * Get the matching dictionary ids.
   */
  int[] getMatchingDictIds();

  /**
   * Get the number of non-matching dictionary ids.
   */
  int getNumNonMatchingDictIds();

  /**
   * Get the non-matching dictionary ids.
   */
  int[] getNonMatchingDictIds();

  /**
   * APIs for raw value based predicate evaluator.
   */

  /**
   * Apply a single-value entry to the predicate.
   *
   * @param value Raw value
   * @return Whether the entry matches the predicate
   */
  boolean applySV(long value);

  /**
   * Apply a multi-value entry to the predicate.
   *
   * @param values Array of raw values
   * @param length Number of values in the entry
   * @return Whether the entry matches the predicate
   */
  boolean applyMV(long[] values, int length);

  /**
   * Apply a single-value entry to the predicate.
   *
   * @param value Raw value
   * @return Whether the entry matches the predicate
   */
  boolean applySV(float value);

  /**
   * Apply a multi-value entry to the predicate.
   *
   * @param values Array of raw values
   * @param length Number of values in the entry
   * @return Whether the entry matches the predicate
   */
  boolean applyMV(float[] values, int length);

  /**
   * Apply a single-value entry to the predicate.
   *
   * @param value Raw value
   * @return Whether the entry matches the predicate
   */
  boolean applySV(double value);

  /**
   * Apply a multi-value entry to the predicate.
   *
   * @param values Array of raw values
   * @param length Number of values in the entry
   * @return Whether the entry matches the predicate
   */
  boolean applyMV(double[] values, int length);

  /**
   * Apply a single-value entry to the predicate.
   *
   * @param value Raw value
   * @return Whether the entry matches the predicate
   */
  boolean applySV(String value);

  /**
   * Apply a multi-value entry to the predicate.
   *
   * @param values Array of raw values
   * @param length Number of values in the entry
   * @return Whether the entry matches the predicate
   */
  boolean applyMV(String[] values, int length);

  /**
   * Apply a single-value entry to the predicate.
   *
   * @param value Raw value
   * @return Whether the entry matches the predicate
   */
  boolean applySV(byte[] value);

  /**
   * Apply a multi-value entry to the predicate.
   *
   * @param values Array of raw values
   * @param length Number of values in the entry
   * @return Whether the entry matches the predicate
   */
  boolean applyMV(byte[][] values, int length);

  /**
   * Apply a single-value entry to the predicate.
   *
   * @param value Raw value
   * @return Whether the entry matches the predicate
   */
  boolean applySV(BigDecimal value);

  /**
   * Apply a multi-value entry to the predicate.
   *
   * @param values Array of raw values
   * @param length Number of values in the entry
   * @return Whether the entry matches the predicate
   */
  boolean applyMV(BigDecimal[] values, int length);
}
