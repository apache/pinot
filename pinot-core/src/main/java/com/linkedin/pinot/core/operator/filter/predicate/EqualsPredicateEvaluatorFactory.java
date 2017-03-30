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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


/**
 * Factory for equality predicate evaluators.
 */
public class EqualsPredicateEvaluatorFactory {

  // Private constructor
  private EqualsPredicateEvaluatorFactory() {

  }

  /**
   * Returns a new instance of dictionary based equality predicate evaluator.
   * @param predicate Predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based equality predicate evaluator
   */
  public static PredicateEvaluator newDictionaryBasedEvaluator(EqPredicate predicate, Dictionary dictionary) {
    return new DictionaryBasedEqualsPredicateEvaluator(predicate, dictionary);
  }

  /**
   * Returns a new instance of no-dictionary based equality predicate evaluator.
   * @param predicate Predicate to evaluate
   * @param dataType Data type for the column
   * @return No Dictionary based equality predicate evaluator
   */
  public static PredicateEvaluator newNoDictionaryBasedEvaluator(EqPredicate predicate, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntNoDictionaryBasedEqualsEvaluator(predicate);

      case LONG:
        return new LongNoDictionaryBasedEqualsEvaluator(predicate);

      case FLOAT:
        return new FloatNoDictionaryBasedEqualsEvaluator(predicate);

      case DOUBLE:
        return new DoubleNoDictionaryBasedEqualsEvaluator(predicate);

      case STRING:
        return new StringNoDictionaryBasedEqualsEvaluator(predicate);

      default:
        throw new UnsupportedOperationException(
            "No dictionary based Equals predicate evaluator not supported for datatype:" + dataType);
    }
  }

  /**
   *
   * No dictionary equality evaluator for int data type.
   */
  public static final class IntNoDictionaryBasedEqualsEvaluator extends BasePredicateEvaluator {
    int _expectedValue;

    public IntNoDictionaryBasedEqualsEvaluator(EqPredicate predicate) {
      _expectedValue = Integer.parseInt(predicate.getEqualsValue());
    }

    @Override
    public boolean apply(int inputValue) {
      return (_expectedValue == inputValue);
    }

    @Override
    public boolean apply(int[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(int[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        int inputValue = inputValues[i];
        if (_expectedValue == inputValue) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   *
   * No dictionary equality evaluator for long data type.
   */
  public static final class LongNoDictionaryBasedEqualsEvaluator extends BasePredicateEvaluator {
    long _expectedValue;

    public LongNoDictionaryBasedEqualsEvaluator(EqPredicate predicate) {
      _expectedValue = Long.parseLong(predicate.getEqualsValue());
    }

    @Override
    public boolean apply(long inputValue) {
      return (_expectedValue == inputValue);
    }

    @Override
    public boolean apply(long[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(long[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        long inputValue = inputValues[i];
        if (_expectedValue == inputValue) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   *
   * No dictionary equality evaluator for float data type.
   */
  public static final class FloatNoDictionaryBasedEqualsEvaluator extends BasePredicateEvaluator {
    float _expectedValue;

    public FloatNoDictionaryBasedEqualsEvaluator(EqPredicate predicate) {
      _expectedValue = Float.parseFloat(predicate.getEqualsValue());
    }

    @Override
    public boolean apply(float inputValue) {
      return (_expectedValue == inputValue);
    }

    @Override
    public boolean apply(float[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(float[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        float inputValue = inputValues[i];
        if (_expectedValue == inputValue) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   *
   * No dictionary equality evaluator for double data type.
   */
  public static final class DoubleNoDictionaryBasedEqualsEvaluator extends BasePredicateEvaluator {
    double _expectedValue;

    public DoubleNoDictionaryBasedEqualsEvaluator(EqPredicate predicate) {
      _expectedValue = Double.parseDouble(predicate.getEqualsValue());
    }

    @Override
    public boolean apply(double inputValue) {
      return (_expectedValue == inputValue);
    }

    @Override
    public boolean apply(double[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(double[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        double inputValue = inputValues[i];
        if (_expectedValue == inputValue) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   *
   * No dictionary equality evaluator for string data type.
   */
  public static final class StringNoDictionaryBasedEqualsEvaluator extends BasePredicateEvaluator {
    String _expectedValue;

    public StringNoDictionaryBasedEqualsEvaluator(EqPredicate predicate) {
      _expectedValue = predicate.getEqualsValue();
    }

    @Override
    public boolean apply(String inputValue) {
      return (_expectedValue.equals(inputValue));
    }

    @Override
    public boolean apply(String[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(String[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        String inputValue = inputValues[i];
        if (_expectedValue.equals(inputValue)) {
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
  public static final class DictionaryBasedEqualsPredicateEvaluator extends BasePredicateEvaluator {
    private int[] _matchingIds;
    private int _equalsMatchDictId;
    private EqPredicate _predicate;

    public DictionaryBasedEqualsPredicateEvaluator(EqPredicate predicate, Dictionary dictionary) {
      _predicate = predicate;
      _equalsMatchDictId = dictionary.indexOf(predicate.getEqualsValue());
      if (_equalsMatchDictId >= 0) {
        _matchingIds = new int[1];
        _matchingIds[0] = _equalsMatchDictId;
      } else {
        _matchingIds = new int[0];
      }
    }

    @Override
    public boolean apply(int dictionaryId) {
      return (dictionaryId == _equalsMatchDictId);
    }

    @Override
    public boolean apply(int[] dictionaryIds) {
      if (_equalsMatchDictId < 0) {
        return false;
      }
      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int dictId : dictionaryIds) {
        if (dictId == _equalsMatchDictId) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int[] getMatchingDictionaryIds() {
      return _matchingIds;
    }

    @Override
    public int[] getNonMatchingDictionaryIds() {
      throw new UnsupportedOperationException(
          "Returning non matching values is expensive for predicateType:" + _predicate.getType());
    }

    @Override
    public boolean apply(int[] dictionaryIds, int length) {
      for (int i = 0; i < length; i++) {
        int dictId = dictionaryIds[i];
        if (dictId == _equalsMatchDictId) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean alwaysFalse() {
      return _equalsMatchDictId < 0;
    }
  }
}
