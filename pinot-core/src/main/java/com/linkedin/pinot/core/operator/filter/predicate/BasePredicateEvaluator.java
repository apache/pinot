/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.filter.predicate;


public abstract class BasePredicateEvaluator implements PredicateEvaluator {

  private static final String EXCEPTION_MESSAGE = "Incorrect method called on base class.";

  @Override
  public boolean apply(int dictionaryId) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(int[] dictionaryIds) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(int[] dictionaryIds, int length) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public int[] getMatchingDictionaryIds() {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public int[] getNonMatchingDictionaryIds() {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean alwaysFalse() {
    return false;
  }

  /**
   * STRING
   */
  @Override
  public boolean apply(String value) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(String[] values) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(String[] values, int length) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  /**
   * LONG
   */
  @Override
  public boolean apply(long value) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(long[] values) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(long[] values, int length) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  /**
   * FLOAT
   */
  @Override
  public boolean apply(float value) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(float[] values) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(float[] values, int length) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  /**
   * DOUBLE
   */
  @Override
  public boolean apply(double value) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(double[] values) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }

  @Override
  public boolean apply(double[] values, int length) {
    throw new UnsupportedOperationException(EXCEPTION_MESSAGE);
  }
}
