/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


/**
 * Factory for NEQ predicate evaluators.
 */
public class NotEqualsPredicateEvaluatorFactory {
  private NotEqualsPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based NEQ predicate evaluator.
   *
   * @param nEqPredicate NEQ predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based NEQ predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(NEqPredicate nEqPredicate,
      Dictionary dictionary) {
    return new DictionaryBasedNeqPredicateEvaluator(nEqPredicate, dictionary);
  }

  /**
   * Create a new instance of raw value based NEQ predicate evaluator.
   *
   * @param nEqPredicate NEQ predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based NEQ predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(NEqPredicate nEqPredicate,
      FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntRawValueBasedNeqPredicateEvaluator(nEqPredicate);
      case LONG:
        return new LongRawValueBasedNeqPredicateEvaluator(nEqPredicate);
      case FLOAT:
        return new FloatRawValueBasedNeqPredicateEvaluator(nEqPredicate);
      case DOUBLE:
        return new DoubleRawValueBasedNeqPredicateEvaluator(nEqPredicate);
      case STRING:
        return new StringRawValueBasedNeqPredicateEvaluator(nEqPredicate);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedNeqPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _nonMatchingDictId;
    final int[] _nonMatchingDictIds;
    final Dictionary _dictionary;
    int[] _matchingDictIds;

    DictionaryBasedNeqPredicateEvaluator(NEqPredicate nEqPredicate, Dictionary dictionary) {
      _nonMatchingDictId = dictionary.indexOf(nEqPredicate.getNotEqualsValue());
      if (_nonMatchingDictId >= 0) {
        _nonMatchingDictIds = new int[]{_nonMatchingDictId};
      } else {
        _nonMatchingDictIds = new int[0];
      }
      _dictionary = dictionary;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NEQ;
    }

    @Override
    public boolean isAlwaysFalse() {
      return _nonMatchingDictIds.length == _dictionary.length();
    }

    @Override
    public boolean applySV(int dictId) {
      return _nonMatchingDictId != dictId;
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        int dictionarySize = _dictionary.length();
        if (_nonMatchingDictId >= 0) {
          _matchingDictIds = new int[dictionarySize - 1];
          int index = 0;
          for (int dictId = 0; dictId < dictionarySize; dictId++) {
            if (dictId != _nonMatchingDictId) {
              _matchingDictIds[index++] = dictId;
            }
          }
        } else {
          _matchingDictIds = new int[dictionarySize];
          for (int dictId = 0; dictId < dictionarySize; dictId++) {
            _matchingDictIds[dictId] = dictId;
          }
        }
      }
      return _matchingDictIds;
    }

    @Override
    public int[] getNonMatchingDictIds() {
      return _nonMatchingDictIds;
    }
  }

  private static final class IntRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final int _nonMatchingValue;

    IntRawValueBasedNeqPredicateEvaluator(NEqPredicate nEqPredicate) {
      _nonMatchingValue = Integer.parseInt(nEqPredicate.getNotEqualsValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NEQ;
    }

    @Override
    public boolean applySV(int value) {
      return _nonMatchingValue != value;
    }
  }

  private static final class LongRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final long _nonMatchingValue;

    LongRawValueBasedNeqPredicateEvaluator(NEqPredicate nEqPredicate) {
      _nonMatchingValue = Long.parseLong(nEqPredicate.getNotEqualsValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NEQ;
    }

    @Override
    public boolean applySV(long value) {
      return _nonMatchingValue != value;
    }
  }

  private static final class FloatRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final float _nonMatchingValue;

    FloatRawValueBasedNeqPredicateEvaluator(NEqPredicate nEqPredicate) {
      _nonMatchingValue = Float.parseFloat(nEqPredicate.getNotEqualsValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NEQ;
    }

    @Override
    public boolean applySV(float value) {
      return _nonMatchingValue != value;
    }
  }

  private static final class DoubleRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final double _nonMatchingValue;

    DoubleRawValueBasedNeqPredicateEvaluator(NEqPredicate nEqPredicate) {
      _nonMatchingValue = Double.parseDouble(nEqPredicate.getNotEqualsValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NEQ;
    }

    @Override
    public boolean applySV(double value) {
      return _nonMatchingValue != value;
    }
  }

  private static final class StringRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final String _nonMatchingValue;

    StringRawValueBasedNeqPredicateEvaluator(NEqPredicate nEqPredicate) {
      _nonMatchingValue = nEqPredicate.getNotEqualsValue();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NEQ;
    }

    @Override
    public boolean applySV(String value) {
      return !_nonMatchingValue.equals(value);
    }
  }
}
