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
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MultiValueVisitor;
import org.apache.pinot.spi.data.SingleValueVisitor;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


/**
 * Factory for NEQ predicate evaluators.
 */
public class NotEqualsPredicateEvaluatorFactory {
  private NotEqualsPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based NEQ predicate evaluator.
   *
   * @param notEqPredicate NOT_EQ predicate to evaluate
   * @param dictionary Dictionary for the column
   * @param dataType Data type for the column
   * @return Dictionary based NOT_EQ predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(NotEqPredicate notEqPredicate,
      Dictionary dictionary, DataType dataType) {
    return new DictionaryBasedNeqPredicateEvaluator(notEqPredicate, dictionary, dataType);
  }

  /**
   * Create a new instance of raw value based NEQ predicate evaluator.
   *
   * @param notEqPredicate NOT_EQ predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based NOT_EQ predicate evaluator
   */
  public static NeqRawPredicateEvaluator newRawValueBasedEvaluator(NotEqPredicate notEqPredicate,
      DataType dataType) {
    String value = notEqPredicate.getValue();
    switch (dataType) {
      case INT:
        return new IntRawValueBasedNeqPredicateEvaluator(notEqPredicate, Integer.parseInt(value));
      case LONG:
        return new LongRawValueBasedNeqPredicateEvaluator(notEqPredicate, Long.parseLong(value));
      case FLOAT:
        return new FloatRawValueBasedNeqPredicateEvaluator(notEqPredicate, Float.parseFloat(value));
      case DOUBLE:
        return new DoubleRawValueBasedNeqPredicateEvaluator(notEqPredicate, Double.parseDouble(value));
      case BIG_DECIMAL:
        return new BigDecimalRawValueBasedNeqPredicateEvaluator(notEqPredicate, new BigDecimal(value));
      case BOOLEAN:
        return new IntRawValueBasedNeqPredicateEvaluator(notEqPredicate, BooleanUtils.toInt(value));
      case TIMESTAMP:
        return new LongRawValueBasedNeqPredicateEvaluator(notEqPredicate, TimestampUtils.toMillisSinceEpoch(value));
      case STRING:
        return new StringRawValueBasedNeqPredicateEvaluator(notEqPredicate, value);
      case BYTES:
        return new BytesRawValueBasedNeqPredicateEvaluator(notEqPredicate, BytesUtils.toBytes(value));
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedNeqPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _nonMatchingDictId;
    final int[] _nonMatchingDictIds;
    final Dictionary _dictionary;
    int[] _matchingDictIds;

    DictionaryBasedNeqPredicateEvaluator(NotEqPredicate notEqPredicate, Dictionary dictionary, DataType dataType) {
      super(notEqPredicate);
      String predicateValue = PredicateUtils.getStoredValue(notEqPredicate.getValue(), dataType);
      _nonMatchingDictId = dictionary.indexOf(predicateValue);
      if (_nonMatchingDictId >= 0) {
        _nonMatchingDictIds = new int[]{_nonMatchingDictId};
        if (dictionary.length() == 1) {
          _alwaysFalse = true;
        }
      } else {
        _nonMatchingDictIds = new int[0];
        _alwaysTrue = true;
      }
      _dictionary = dictionary;
    }

    @Override
    public int getNumMatchingItems() {
      return -1;
    }

    @Override
    public boolean applySV(int dictId) {
      return _nonMatchingDictId != dictId;
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

  public static abstract class NeqRawPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    public NeqRawPredicateEvaluator(Predicate predicate) {
      super(predicate);
    }

    /**
     * Visits the not matching value of this predicate.
     */
    public abstract <R> R accept(SingleValueVisitor<R> visitor);

    /**
     * Visits the not matching value of this predicate, which will be transformed into an array with a single value.
     */
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return accept(visitor.asSingleValueVisitor());
    }
  }

  private static final class IntRawValueBasedNeqPredicateEvaluator extends NeqRawPredicateEvaluator {
    final int _nonMatchingValue;

    IntRawValueBasedNeqPredicateEvaluator(NotEqPredicate notEqPredicate, int nonMatchingValue) {
      super(notEqPredicate);
      _nonMatchingValue = nonMatchingValue;
    }

    @Override
    public int getNumMatchingItems() {
      return -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return _nonMatchingValue != value;
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
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitInt(_nonMatchingValue);
    }
  }

  private static final class LongRawValueBasedNeqPredicateEvaluator extends NeqRawPredicateEvaluator {
    final long _nonMatchingValue;

    LongRawValueBasedNeqPredicateEvaluator(NotEqPredicate notEqPredicate, long nonMatchingValue) {
      super(notEqPredicate);
      _nonMatchingValue = nonMatchingValue;
    }

    @Override
    public int getNumMatchingItems() {
      return -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      return _nonMatchingValue != value;
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
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitLong(_nonMatchingValue);
    }
  }

  private static final class FloatRawValueBasedNeqPredicateEvaluator extends NeqRawPredicateEvaluator {
    final float _nonMatchingValue;

    FloatRawValueBasedNeqPredicateEvaluator(NotEqPredicate notEqPredicate, float nonMatchingValue) {
      super(notEqPredicate);
      _nonMatchingValue = nonMatchingValue;
    }

    @Override
    public int getNumMatchingItems() {
      return -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      return _nonMatchingValue != value;
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
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitFloat(_nonMatchingValue);
    }
  }

  private static final class DoubleRawValueBasedNeqPredicateEvaluator extends NeqRawPredicateEvaluator {
    final double _nonMatchingValue;

    DoubleRawValueBasedNeqPredicateEvaluator(NotEqPredicate notEqPredicate, double nonMatchingValue) {
      super(notEqPredicate);
      _nonMatchingValue = nonMatchingValue;
    }

    @Override
    public int getNumMatchingItems() {
      return -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      return _nonMatchingValue != value;
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
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitDouble(_nonMatchingValue);
    }
  }

  private static final class BigDecimalRawValueBasedNeqPredicateEvaluator extends NeqRawPredicateEvaluator {
    final BigDecimal _nonMatchingValue;

    BigDecimalRawValueBasedNeqPredicateEvaluator(NotEqPredicate notEqPredicate, BigDecimal nonMatchingValue) {
      super(notEqPredicate);
      _nonMatchingValue = nonMatchingValue;
    }

    @Override
    public int getNumMatchingItems() {
      return -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.BIG_DECIMAL;
    }

    @Override
    public boolean applySV(BigDecimal value) {
      return _nonMatchingValue.compareTo(value) != 0;
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitBigDecimal(_nonMatchingValue);
    }
  }

  private static final class StringRawValueBasedNeqPredicateEvaluator extends NeqRawPredicateEvaluator {
    final String _nonMatchingValue;

    StringRawValueBasedNeqPredicateEvaluator(NotEqPredicate notEqPredicate, String nonMatchingValue) {
      super(notEqPredicate);
      _nonMatchingValue = nonMatchingValue;
    }

    @Override
    public int getNumMatchingItems() {
      return -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return !_nonMatchingValue.equals(value);
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitString(_nonMatchingValue);
    }
  }

  private static final class BytesRawValueBasedNeqPredicateEvaluator extends NeqRawPredicateEvaluator {
    final byte[] _nonMatchingValue;

    BytesRawValueBasedNeqPredicateEvaluator(NotEqPredicate notEqPredicate, byte[] nonMatchingValue) {
      super(notEqPredicate);
      _nonMatchingValue = nonMatchingValue;
    }

    @Override
    public int getNumMatchingItems() {
      return -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.BYTES;
    }

    @Override
    public boolean applySV(byte[] value) {
      return !Arrays.equals(_nonMatchingValue, value);
    }

    @Override
    public <R> R accept(SingleValueVisitor<R> visitor) {
      return visitor.visitBytes(_nonMatchingValue);
    }
  }
}
