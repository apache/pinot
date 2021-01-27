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

import org.apache.pinot.core.query.request.context.predicate.EqPredicate;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;


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
      DataType dataType) {
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
      case BYTES:
        return new BytesRawValueBasedEqPredicateEvaluator(eqPredicate);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedEqPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _matchingDictId;
    final int[] _matchingDictIds;

    DictionaryBasedEqPredicateEvaluator(EqPredicate eqPredicate, Dictionary dictionary) {
      _precomputed = eqPredicate.getPrecomputedResult();
      if (_precomputed != null && !_precomputed) {
        // This predicate will always evaluate to false, so there are no matching dictionary ids.
        _matchingDictId = -1;
        _matchingDictIds = new int[0];
      } else {
        _matchingDictId = dictionary.indexOf(eqPredicate.getValue());
        if (_matchingDictId >= 0) {
          _matchingDictIds = new int[]{_matchingDictId};
          if (dictionary.length() == 1) {
            _alwaysTrue = true;
          }
        } else {
          _matchingDictIds = new int[0];
          _alwaysFalse = true;
        }
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
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
      _precomputed = eqPredicate.getPrecomputedResult();
      _matchingValue = Integer.parseInt(eqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return _precomputed == null ? _matchingValue == value : _precomputed;
    }
  }

  private static final class LongRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final long _matchingValue;

    LongRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _precomputed = eqPredicate.getPrecomputedResult();
      _matchingValue = Long.parseLong(eqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      return _precomputed == null ? _matchingValue == value : _precomputed;
    }
  }

  private static final class FloatRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final float _matchingValue;

    FloatRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _precomputed = eqPredicate.getPrecomputedResult();
      _matchingValue = Float.parseFloat(eqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      return _precomputed == null ? _matchingValue == value : _precomputed;
    }
  }

  private static final class DoubleRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final double _matchingValue;

    DoubleRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _precomputed = eqPredicate.getPrecomputedResult();
      _matchingValue = Double.parseDouble(eqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      return _precomputed == null ? _matchingValue == value : _precomputed;
    }
  }

  private static final class StringRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final String _matchingValue;

    StringRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _precomputed = eqPredicate.getPrecomputedResult();
      _matchingValue = eqPredicate.getValue();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return _precomputed == null ? _matchingValue.equals(value) : _precomputed;
    }
  }

  private static final class BytesRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final byte[] _matchingValue;

    BytesRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate) {
      _precomputed = eqPredicate.getPrecomputedResult();
      _matchingValue = BytesUtils.toBytes(eqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.BYTES;
    }

    @Override
    public boolean applySV(byte[] value) {
      return _precomputed == null ? ByteArray.compare(_matchingValue, value) == 0 : _precomputed;
    }
  }
}
