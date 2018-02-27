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
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


/**
 * Factory for EQ predicate evaluators.
 */
public class EqualsPredicateEvaluatorFactory {
  private EqualsPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based EQ predicate evaluator.
   *
   * @param eqPredicate EQ predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based EQ predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(EqPredicate eqPredicate,
      Dictionary dictionary) {
    return new DictionaryBasedEqPredicateEvaluator(eqPredicate, dictionary);
  }

  /**
   * Create a new instance of raw value based EQ predicate evaluator.
   *
   * @param eqPredicate EQ predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based EQ predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(EqPredicate eqPredicate,
      FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntRawValueBasedEqPredicateEvaluator(eqPredicate);
      case LONG:
        return new LongRawValueBasedEqPredicateEvaluator(eqPredicate);
      case FLOAT:
        return new FloatRawValueBasedEqPredicateEvaluator(eqPredicate);
      case DOUBLE:
        return new DoubleRawValueBasedEqPredicateEvaluator(eqPredicate);
      case STRING:
        return new StringRawValueBasedEqPredicateEvaluator(eqPredicate);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedEqPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _matchingDictId;
    final int[] _matchingDictIds;

    DictionaryBasedEqPredicateEvaluator(EqPredicate eqPredicate, Dictionary dictionary) {
      _matchingDictId = dictionary.indexOf(eqPredicate.getEqualsValue());
      if (_matchingDictId >= 0) {
        _matchingDictIds = new int[]{_matchingDictId};
      } else {
        _matchingDictIds = new int[0];
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public boolean isAlwaysFalse() {
      return _matchingDictId < 0;
    }

    @Override
    public boolean applySV(int dictId) {
      return _matchingDictId == dictId;
    }

    @Override
    public int[] getMatchingDictIds() {
      return _matchingDictIds;
    }
  }

  private static final class IntRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final int _matchingValue;

    IntRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _matchingValue = Integer.parseInt(eqPredicate.getEqualsValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public boolean applySV(int value) {
      return _matchingValue == value;
    }
  }

  private static final class LongRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final long _matchingValue;

    LongRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _matchingValue = Long.parseLong(eqPredicate.getEqualsValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public boolean applySV(long value) {
      return (_matchingValue == value);
    }
  }

  private static final class FloatRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final float _matchingValue;

    FloatRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _matchingValue = Float.parseFloat(eqPredicate.getEqualsValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public boolean applySV(float value) {
      return _matchingValue == value;
    }
  }

  private static final class DoubleRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final double _matchingValue;

    DoubleRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _matchingValue = Double.parseDouble(eqPredicate.getEqualsValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public boolean applySV(double value) {
      return _matchingValue == value;
    }
  }

  private static final class StringRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final String _matchingValue;

    StringRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _matchingValue = eqPredicate.getEqualsValue();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public boolean applySV(String value) {
      return _matchingValue.equals(value);
    }
  }
}
