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
import org.apache.pinot.core.operator.filter.predicate.traits.DoubleValue;
import org.apache.pinot.core.operator.filter.predicate.traits.FloatValue;
import org.apache.pinot.core.operator.filter.predicate.traits.IntValue;
import org.apache.pinot.core.operator.filter.predicate.traits.LongValue;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MultiValueVisitor;
import org.apache.pinot.spi.data.SingleValueVisitor;
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
  public static EqRawPredicateEvaluator newRawValueBasedEvaluator(EqPredicate eqPredicate,
      DataType dataType) {
    String value = eqPredicate.getValue();
    switch (dataType) {
      case INT:
        return new IntRawValueBasedEqPredicateEvaluator(eqPredicate, Integer.parseInt(value));
      case LONG:
        return new LongRawValueBasedEqPredicateEvaluator(eqPredicate, Long.parseLong(value));
      case FLOAT:
        return new FloatRawValueBasedEqPredicateEvaluator(eqPredicate, Float.parseFloat(value));
      case DOUBLE:
        return new DoubleRawValueBasedEqPredicateEvaluator(eqPredicate, Double.parseDouble(value));
      case BIG_DECIMAL:
        return new BigDecimalRawValueBasedEqPredicateEvaluator(eqPredicate, new BigDecimal(value));
      case BOOLEAN:
        return new IntRawValueBasedEqPredicateEvaluator(eqPredicate, BooleanUtils.toInt(value));
      case TIMESTAMP:
        return new LongRawValueBasedEqPredicateEvaluator(eqPredicate, TimestampUtils.toMillisSinceEpoch(value));
      case STRING:
        return new StringRawValueBasedEqPredicateEvaluator(eqPredicate, value);
      case BYTES:
        return new BytesRawValueBasedEqPredicateEvaluator(eqPredicate, BytesUtils.toBytes(value));
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedEqPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator
      implements IntValue {
    final int _matchingDictId;
    final int[] _matchingDictIds;

    DictionaryBasedEqPredicateEvaluator(EqPredicate eqPredicate, Dictionary dictionary, DataType dataType) {
      super(eqPredicate);
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
    public int getNumMatchingItems() {
      return 1;
    }

    @Override
    public boolean applySV(int dictId) {
      return _matchingDictId == dictId;
    }

    @Override
    public int applySV(int limit, int[] docIds, int[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        int value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public int[] getMatchingDictIds() {
      return _matchingDictIds;
    }

    @Override
    public int getInt() {
      return _matchingDictId;
    }
  }

  public static abstract class EqRawPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    public EqRawPredicateEvaluator(Predicate predicate) {
      super(predicate);
    }

    /**
     * Visits the matching value of this predicate.
     */
    public abstract <R> R accept(SingleValueVisitor<R> visitor);

    /**
     * Visits the matching value of this predicate, which will be transformed into an array with a single value.
     */
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return accept(visitor.asSingleValueVisitor());
    }
  }

  private static final class IntRawValueBasedEqPredicateEvaluator extends EqRawPredicateEvaluator implements IntValue {
    final int _matchingValue;

    IntRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate, int matchingValue) {
      super(eqPredicate);
      _matchingValue = matchingValue;
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitInt(_matchingValue);
    }

    @Override
    public int getNumMatchingItems() {
      return 1;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return _matchingValue == value;
    }

    @Override
    public int applySV(int limit, int[] docIds, int[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        int value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public int getInt() {
      return _matchingValue;
    }
  }

  private static final class LongRawValueBasedEqPredicateEvaluator extends EqRawPredicateEvaluator
      implements LongValue {
    final long _matchingValue;

    LongRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate, long matchingValue) {
      super(eqPredicate);
      _matchingValue = matchingValue;
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitLong(_matchingValue);
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return visitor.asSingleValueVisitor().visitLong(_matchingValue);
    }

    @Override
    public int getNumMatchingItems() {
      return 1;
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      return (_matchingValue == value);
    }

    @Override
    public int applySV(int limit, int[] docIds, long[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        long value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public long getLong() {
      return _matchingValue;
    }
  }

  private static final class FloatRawValueBasedEqPredicateEvaluator extends EqRawPredicateEvaluator
      implements FloatValue {
    final float _matchingValue;

    FloatRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate, float matchingValue) {
      super(eqPredicate);
      _matchingValue = matchingValue;
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitFloat(_matchingValue);
    }

    @Override
    public int getNumMatchingItems() {
      return 1;
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      return _matchingValue == value;
    }

    @Override
    public int applySV(int limit, int[] docIds, float[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        float value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public float getFloat() {
      return _matchingValue;
    }
  }

  private static final class DoubleRawValueBasedEqPredicateEvaluator extends EqRawPredicateEvaluator
      implements DoubleValue {
    final double _matchingValue;

    DoubleRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate, double matchingValue) {
      super(eqPredicate);
      _matchingValue = matchingValue;
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitDouble(_matchingValue);
    }

    @Override
    public int getNumMatchingItems() {
      return 1;
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      return _matchingValue == value;
    }

    @Override
    public int applySV(int limit, int[] docIds, double[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        double value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public double getDouble() {
      return _matchingValue;
    }
  }

  private static final class BigDecimalRawValueBasedEqPredicateEvaluator extends EqRawPredicateEvaluator {
    final BigDecimal _matchingValue;

    BigDecimalRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate, BigDecimal matchingValue) {
      super(eqPredicate);
      _matchingValue = matchingValue;
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitBigDecimal(_matchingValue);
    }

    @Override
    public int getNumMatchingItems() {
      return 1;
    }

    @Override
    public DataType getDataType() {
      return DataType.BIG_DECIMAL;
    }

    @Override
    public boolean applySV(BigDecimal value) {
      return _matchingValue.compareTo(value) == 0;
    }
  }

  private static final class StringRawValueBasedEqPredicateEvaluator extends EqRawPredicateEvaluator {
    final String _matchingValue;

    StringRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate, String matchingValue) {
      super(eqPredicate);
      _matchingValue = matchingValue;
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitString(_matchingValue);
    }

    @Override
    public int getNumMatchingItems() {
      return 1;
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

  private static final class BytesRawValueBasedEqPredicateEvaluator extends EqRawPredicateEvaluator {
    final byte[] _matchingValue;

    BytesRawValueBasedEqPredicateEvaluator(EqPredicate eqPredicate, byte[] matchingValue) {
      super(eqPredicate);
      _matchingValue = matchingValue;
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitBytes(_matchingValue);
    }

    @Override
    public int getNumMatchingItems() {
      return 1;
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
}
