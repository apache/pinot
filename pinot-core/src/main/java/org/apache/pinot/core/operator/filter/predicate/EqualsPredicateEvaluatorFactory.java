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
import java.util.Arrays;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


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
   * @param dataType Data type for the column
   * @return Dictionary based EQ predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(EqPredicate eqPredicate,
      Dictionary dictionary, DataType dataType) {
    return new DictionaryBasedEqPredicateEvaluator(eqPredicate, dictionary, dataType);
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
    String value = eqPredicate.getValue();
    switch (dataType) {
      case INT:
        return new IntRawValueBasedEqPredicateEvaluator(Integer.parseInt(value));
      case LONG:
        return new LongRawValueBasedEqPredicateEvaluator(Long.parseLong(value));
      case FLOAT:
        return new FloatRawValueBasedEqPredicateEvaluator(Float.parseFloat(value));
      case DOUBLE:
        return new DoubleRawValueBasedEqPredicateEvaluator(Double.parseDouble(value));
      case BOOLEAN:
        return new IntRawValueBasedEqPredicateEvaluator(BooleanUtils.toInt(value));
      case TIMESTAMP:
        return new LongRawValueBasedEqPredicateEvaluator(TimestampUtils.toMillisSinceEpoch(value));
      case STRING:
        return new StringRawValueBasedEqPredicateEvaluator(value);
      case BYTES:
        return new BytesRawValueBasedEqPredicateEvaluator(BytesUtils.toBytes(value));
      case BIGDECIMAL:
        return new BigDecimalRawValueBasedEqPredicateEvaluator(BigDecimalUtils.toBigDecimal(value));
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedEqPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _matchingDictId;
    final int[] _matchingDictIds;

    DictionaryBasedEqPredicateEvaluator(EqPredicate eqPredicate, Dictionary dictionary, DataType dataType) {
      String predicateValue = PredicateUtils.getStoredValue(eqPredicate.getValue(), dataType);
      _matchingDictId = dictionary.indexOf(predicateValue);
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

    IntRawValueBasedEqPredicateEvaluator(int matchingValue) {
      _matchingValue = matchingValue;
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
      return _matchingValue == value;
    }
  }

  private static final class LongRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final long _matchingValue;

    LongRawValueBasedEqPredicateEvaluator(long matchingValue) {
      _matchingValue = matchingValue;
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
      return (_matchingValue == value);
    }
  }

  private static final class FloatRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final float _matchingValue;

    FloatRawValueBasedEqPredicateEvaluator(float matchingValue) {
      _matchingValue = matchingValue;
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
      return _matchingValue == value;
    }
  }

  private static final class DoubleRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final double _matchingValue;

    DoubleRawValueBasedEqPredicateEvaluator(double matchingValue) {
      _matchingValue = matchingValue;
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
      return _matchingValue == value;
    }
  }

  private static final class StringRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final String _matchingValue;

    StringRawValueBasedEqPredicateEvaluator(String matchingValue) {
      _matchingValue = matchingValue;
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
      return _matchingValue.equals(value);
    }
  }

  private static final class BytesRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final byte[] _matchingValue;

    BytesRawValueBasedEqPredicateEvaluator(byte[] matchingValue) {
      _matchingValue = matchingValue;
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
      return Arrays.equals(_matchingValue, value);
    }
  }

  private static final class BigDecimalRawValueBasedEqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final BigDecimal _matchingValue;

    BigDecimalRawValueBasedEqPredicateEvaluator(BigDecimal matchingValue) {
      _matchingValue = matchingValue;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.BIGDECIMAL;
    }

    @Override
    public boolean applySV(BigDecimal value) {
      return _matchingValue.equals(value);
    }
  }
}
