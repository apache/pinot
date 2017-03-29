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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

public class EqualsPredicateEvaluator extends BasePredicateEvaluator {
  private EqualsPredicateEvaluator() {

  }

  public static EqualsPredicateEvaluator newDictionaryBasedEvaluator(EqPredicate predicate, Dictionary dictionary) {
    return new DictionaryBasedEqualsPredicateEvaluator(predicate, dictionary);
  }

  public static EqualsPredicateEvaluator newNoDictionaryBasedEvaluator(EqPredicate predicate, DataType dataType) {
    switch (dataType) {
    case BOOLEAN:
      break;
    case BYTE:
      break;
    case CHAR:
      break;
    case DOUBLE:
      break;
    case FLOAT:
      break;
    case INT:
      break;
    case LONG:
      break;
    case SHORT:
      break;
    case STRING:
      return new StringNoDictionaryBasedEqualsEvaluator(predicate);
    default:
      break;
    }
    throw new UnsupportedOperationException("No dictionary based Equals predicate evaluator not supported for datatype:" + dataType);
  }

  /**
   * 
   * String data type
   */
  public static final class StringNoDictionaryBasedEqualsEvaluator extends EqualsPredicateEvaluator {

    String expectedValue;

    private StringNoDictionaryBasedEqualsEvaluator(EqPredicate predicate) {
      expectedValue = predicate.getEqualsValue();
    }

    @Override
    public boolean apply(String inputValue) {
      return (expectedValue.equals(inputValue));
    }

    @Override
    public boolean apply(String[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(String[] inputValues, int length) {

      // we cannot do binary search since the multivalue columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        String inputValue = inputValues[i];
        if (expectedValue.equals(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * 
   * INT data type
   */
  public static final class IntNoDictionaryBasedEqualsEvaluator extends EqualsPredicateEvaluator {

    int expectedValue;

    private IntNoDictionaryBasedEqualsEvaluator(EqPredicate predicate) {
      expectedValue = Integer.parseInt(predicate.getEqualsValue());
    }

    @Override
    public boolean apply(int inputValue) {
      return (expectedValue == inputValue);
    }

    @Override
    public boolean apply(int[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(int[] inputValues, int length) {

      // we cannot do binary search since the multivalue columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        int inputValue = inputValues[i];
        if (expectedValue == inputValue) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * 
   * Long data type
   */
  public static final class LongNoDictionaryBasedEqualsEvaluator extends EqualsPredicateEvaluator {

    long expectedValue;

    private LongNoDictionaryBasedEqualsEvaluator(EqPredicate predicate) {
      expectedValue = Long.parseLong(predicate.getEqualsValue());
    }

    @Override
    public boolean apply(long inputValue) {
      return (expectedValue == inputValue);
    }

    @Override
    public boolean apply(long[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(long[] inputValues, int length) {

      // we cannot do binary search since the multivalue columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        long inputValue = inputValues[i];
        if (expectedValue == inputValue) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * 
   * Dictionary Based Equals predicate Evaluator
   *
   */
  public static final class DictionaryBasedEqualsPredicateEvaluator extends EqualsPredicateEvaluator {

    private int[] matchingIds;
    private int equalsMatchDictId;
    private EqPredicate predicate;

    private DictionaryBasedEqualsPredicateEvaluator(EqPredicate predicate, Dictionary dictionary) {
      this.predicate = predicate;
      equalsMatchDictId = dictionary.indexOf(predicate.getEqualsValue());
      if (equalsMatchDictId >= 0) {
        matchingIds = new int[1];
        matchingIds[0] = equalsMatchDictId;
      } else {
        matchingIds = new int[0];
      }
    }

    @Override
    public boolean apply(int dictionaryId) {
      return (dictionaryId == equalsMatchDictId);
    }

    @Override
    public boolean apply(int[] dictionaryIds) {
      if (equalsMatchDictId < 0) {
        return false;
      }
      // we cannot do binary search since the multivalue columns are not sorted in the raw segment
      for (int dictId : dictionaryIds) {
        if (dictId == equalsMatchDictId) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int[] getMatchingDictionaryIds() {
      return matchingIds;
    }

    @Override
    public int[] getNonMatchingDictionaryIds() {
      throw new UnsupportedOperationException("Returning non matching values is expensive for predicateType:" + predicate.getType());
    }

    @Override
    public boolean apply(int[] dictionaryIds, int length) {
      for (int i = 0; i < length; i++) {
        int dictId = dictionaryIds[i];
        if (dictId == equalsMatchDictId) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean alwaysFalse() {
      return equalsMatchDictId < 0;
    }

  }

}