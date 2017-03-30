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

public interface PredicateEvaluator {

  /**
   * @param dictionaryId
   * @return
   */
  public boolean apply(int dictionaryId);

  /**
   * @param dictionaryIds
   * @return
   */
  public boolean apply(int[] dictionaryIds);

  /**
   * @param dictionaryIds
   * @param length how many elements in the array should the predicate be evaluated against
   * @return
   */
  public boolean apply(int[] dictionaryIds, int length);

  /**
   * @return matching dictionary Ids
   */
  public int[] getMatchingDictionaryIds();

  /**
   * @return not matching dictionary Ids, useful for NOT IN, IN etc
   */
  public int[] getNonMatchingDictionaryIds();

  /**
   * Will return true if the predicate is evaluated as false all the time. Useful to skip the
   * segment. e.g if country=zm and segment contains no record for "zm" country we can skip the
   * segment
   *
   * @return
   */
  public boolean alwaysFalse();

  /**
   *
   * @param value
   * @return
   */
  public boolean apply(String value);

  /**
   *
   * @param values
   * @return
   */
  public boolean apply(String[] values);

  /**
   * @param values String[] type
   * @param length how many elements in the array should the predicate be evaluated against
   * @return
   */

  public boolean apply(String[] values, int length);

  /**
   * LONG
   */
  public boolean apply(long value);

  public boolean apply(long[] values);

  public boolean apply(long[] values, int length);

  /**
   * FLOAT
   */
  public boolean apply(float value);

  public boolean apply(float[] values);

  public boolean apply(float[] values, int length);

  /**
   * DOUBLE
   */
  public boolean apply(double value);

  public boolean apply(double[] values);

  public boolean apply(double[] values, int length);
}
