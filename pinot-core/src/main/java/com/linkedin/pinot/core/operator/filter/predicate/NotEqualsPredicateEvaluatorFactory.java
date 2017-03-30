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
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


/**
 * Factory for NotEquals predicate evaluators.
 */
public class NotEqualsPredicateEvaluatorFactory {

  // Private constructor
  private NotEqualsPredicateEvaluatorFactory() {

  }

  /**
   * Returns a new instance of dictionary based NEQ evaluator.
   *
   * @param predicate NEQ predicate
   * @param dictionary Dictionary
   * @return Dictionary based NEQ predicate evaluator
   */
  public static PredicateEvaluator newDictionaryBasedEvaluator(NEqPredicate predicate,
      Dictionary dictionary) {
    return new DictionaryBasedNotEqualsPredicateEvaluator(predicate, dictionary);
  }

  /**
   * Returns a new instance of no-dictionary based NEQ evaluator.
   *
   * @param predicate NEQ predicate
   * @param dataType Data type of input value
   * @return No-Dictionary based NEQ predicate evaluator
   */
  public static PredicateEvaluator newNoDictionaryBasedEvaluator(NEqPredicate predicate,
      FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntNoDictionaryNeqEvaluator(predicate);

      case LONG:
        return new LongNoDictionaryNeqEvaluator(predicate);

      case FLOAT:
        return new FloatNoDictionaryNeqEvaluator(predicate);

      case DOUBLE:
        return new DoubleNoDictionaryNeqEvaluator(predicate);

      case STRING:
        return new StringNoDictionaryNeqEvaluator(predicate);

      default:
        throw new UnsupportedOperationException("Data type not supported for index without dictionary: " + dataType);
    }
  }

  /**
   *
   * No dictionary NEQ evaluator for int data type.
   */
  public static final class IntNoDictionaryNeqEvaluator extends BasePredicateEvaluator {
    private int _neqValue;

    private IntNoDictionaryNeqEvaluator(NEqPredicate predicate) {
      _neqValue = Integer.parseInt(predicate.getNotEqualsValue());
    }

    @Override
    public boolean apply(int inputValue) {
      return (_neqValue != inputValue);
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
        if (_neqValue == inputValue) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   *
   * No dictionary NEQ evaluator for long data type.
   */
  public static final class LongNoDictionaryNeqEvaluator extends BasePredicateEvaluator {
    private long _neqValue;

    private LongNoDictionaryNeqEvaluator(NEqPredicate predicate) {
      _neqValue = Long.parseLong(predicate.getNotEqualsValue());
    }

    @Override
    public boolean apply(long inputValue) {
      return (_neqValue != inputValue);
    }

    @Override
    public boolean apply(long[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(long[] inputValues, int length) {
      // We cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        long inputValue = inputValues[i];
        if (_neqValue == inputValue) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   *
   * No dictionary NEQ evaluator for float data type.
   */
  public static final class FloatNoDictionaryNeqEvaluator extends BasePredicateEvaluator {
    private float _neqValue;

    private FloatNoDictionaryNeqEvaluator(NEqPredicate predicate) {
      _neqValue = Float.parseFloat(predicate.getNotEqualsValue());
    }

    @Override
    public boolean apply(float inputValue) {
      return (_neqValue != inputValue);
    }

    @Override
    public boolean apply(float[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(float[] inputValues, int length) {
      // We cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        float inputValue = inputValues[i];
        if (_neqValue == inputValue) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   *
   * No dictionary NEQ evaluator for double data type.
   */
  public static final class DoubleNoDictionaryNeqEvaluator extends BasePredicateEvaluator {
    private double _neqValue;

    private DoubleNoDictionaryNeqEvaluator(NEqPredicate predicate) {
      _neqValue = Double.parseDouble(predicate.getNotEqualsValue());
    }

    @Override
    public boolean apply(double inputValue) {
      return (_neqValue != inputValue);
    }

    @Override
    public boolean apply(double[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(double[] inputValues, int length) {
      // We cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        double inputValue = inputValues[i];
        if (_neqValue == inputValue) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   *
   * No dictionary NEQ evaluator for string data type.
   */
  public static final class StringNoDictionaryNeqEvaluator extends BasePredicateEvaluator {
    private String _neqValue;

    private StringNoDictionaryNeqEvaluator(NEqPredicate predicate) {
      _neqValue = predicate.getNotEqualsValue();
    }

    @Override
    public boolean apply(String inputValue) {
      return (!inputValue.equals(_neqValue));
    }

    @Override
    public boolean apply(String[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(String[] inputValues, int length) {
      // We cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        String inputValue = inputValues[i];
        if (_neqValue.equals(inputValue)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * Dictionary based implementation for {@link NotEqualsPredicateEvaluatorFactory}
   */
  public static final class DictionaryBasedNotEqualsPredicateEvaluator extends BasePredicateEvaluator {
    private int _neqDictValue;
    private int[] _matchingDictIds;
    private Dictionary _dictionary;
    private int[] _nonMatchingDictIds;

    private DictionaryBasedNotEqualsPredicateEvaluator(NEqPredicate predicate, Dictionary dictionary) {
      _dictionary = dictionary;
      _neqDictValue = dictionary.indexOf(predicate.getNotEqualsValue());
      if (_neqDictValue > -1) {
        _nonMatchingDictIds = new int[]{_neqDictValue};
      } else {
        _nonMatchingDictIds = new int[0];
      }
    }

    @Override
    public boolean apply(int dictionaryId) {
      return _neqDictValue != dictionaryId;
    }

    @Override
    public boolean apply(int[] dictionaryIds) {
      if (_neqDictValue < 0) {
        return true;
      }
      //we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int dictId : dictionaryIds) {
        if (dictId == _neqDictValue) {
          return false;
        }
      }

      return true;
    }

    @Override
    public int[] getMatchingDictionaryIds() {
      // This is expensive for NOT EQ predicate, some operators need this for now. Eventually we should remove the need for exposing matching dict ids
      if (_matchingDictIds == null) {
        int size;
        if (_neqDictValue >= 0) {
          size = _dictionary.length() - 1;
        } else {
          size = _dictionary.length();
        }
        _matchingDictIds = new int[size];
        int count = 0;
        for (int i = 0; i < _dictionary.length(); i++) {
          if (i != _neqDictValue) {
            _matchingDictIds[count] = i;
            count = count + 1;
          }
        }
      }
      return _matchingDictIds;
    }

    @Override
    public int[] getNonMatchingDictionaryIds() {
      return _nonMatchingDictIds;
    }

    @Override
    public boolean apply(int[] dictionaryIds, int length) {
      if (_neqDictValue < 0) {
        return true;
      }
      for (int i = 0; i < length; i++) {
        int dictId = dictionaryIds[i];
        if (dictId == _neqDictValue) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean alwaysFalse() {
      return _nonMatchingDictIds.length == _dictionary.length();
    }
  }
}
