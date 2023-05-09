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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.math.BigDecimal;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.operator.filter.predicate.traits.DoubleRange;
import org.apache.pinot.core.operator.filter.predicate.traits.FloatRange;
import org.apache.pinot.core.operator.filter.predicate.traits.IntRange;
import org.apache.pinot.core.operator.filter.predicate.traits.LongRange;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


/**
 * Factory for RANGE predicate evaluators.
 */
public class RangePredicateEvaluatorFactory {
  private RangePredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based RANGE predicate evaluator.
   *
   * @param rangePredicate RANGE predicate to evaluate
   * @param dictionary Dictionary for the column
   * @param dataType Data type for the column
   * @return Dictionary based RANGE predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(RangePredicate rangePredicate,
      Dictionary dictionary, DataType dataType) {
    if (dictionary.isSorted()) {
      return new SortedDictionaryBasedRangePredicateEvaluator(rangePredicate, dictionary, dataType);
    } else {
      return new UnsortedDictionaryBasedRangePredicateEvaluator(rangePredicate, dictionary, dataType);
    }
  }

  /**
   * Create a new instance of raw value based RANGE predicate evaluator.
   *
   * @param rangePredicate RANGE predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based RANGE predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(RangePredicate rangePredicate,
      DataType dataType) {
    String lowerBound = rangePredicate.getLowerBound();
    String upperBound = rangePredicate.getUpperBound();
    // NOTE: Handle unbounded as inclusive min/max value of the data type
    boolean lowerUnbounded = lowerBound.equals(RangePredicate.UNBOUNDED);
    boolean upperUnbounded = upperBound.equals(RangePredicate.UNBOUNDED);
    boolean lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
    boolean upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();
    switch (dataType) {
      case INT:
        return new IntRawValueBasedRangePredicateEvaluator(rangePredicate,
            lowerUnbounded ? Integer.MIN_VALUE : Integer.parseInt(lowerBound),
            upperUnbounded ? Integer.MAX_VALUE : Integer.parseInt(upperBound), lowerInclusive, upperInclusive);
      case LONG:
        return new LongRawValueBasedRangePredicateEvaluator(rangePredicate,
            lowerUnbounded ? Long.MIN_VALUE : Long.parseLong(lowerBound),
            upperUnbounded ? Long.MAX_VALUE : Long.parseLong(upperBound), lowerInclusive, upperInclusive);
      case FLOAT:
        return new FloatRawValueBasedRangePredicateEvaluator(rangePredicate,
            lowerUnbounded ? Float.NEGATIVE_INFINITY : Float.parseFloat(lowerBound),
            upperUnbounded ? Float.POSITIVE_INFINITY : Float.parseFloat(upperBound), lowerInclusive, upperInclusive);
      case DOUBLE:
        return new DoubleRawValueBasedRangePredicateEvaluator(rangePredicate,
            lowerUnbounded ? Double.NEGATIVE_INFINITY : Double.parseDouble(lowerBound),
            upperUnbounded ? Double.POSITIVE_INFINITY : Double.parseDouble(upperBound), lowerInclusive, upperInclusive);
      case BIG_DECIMAL:
        return new BigDecimalRawValueBasedRangePredicateEvaluator(rangePredicate,
            lowerUnbounded ? null : new BigDecimal(lowerBound), upperUnbounded ? null : new BigDecimal(upperBound),
            lowerInclusive, upperInclusive);
      case BOOLEAN:
        return new IntRawValueBasedRangePredicateEvaluator(rangePredicate,
            lowerUnbounded ? Integer.MIN_VALUE : BooleanUtils.toInt(lowerBound),
            upperUnbounded ? Integer.MAX_VALUE : BooleanUtils.toInt(upperBound), lowerInclusive, upperInclusive);
      case TIMESTAMP:
        return new LongRawValueBasedRangePredicateEvaluator(rangePredicate,
            lowerUnbounded ? Long.MIN_VALUE : TimestampUtils.toMillisSinceEpoch(lowerBound),
            upperUnbounded ? Long.MAX_VALUE : TimestampUtils.toMillisSinceEpoch(upperBound), lowerInclusive,
            upperInclusive);
      case STRING:
        return new StringRawValueBasedRangePredicateEvaluator(rangePredicate, lowerUnbounded ? null : lowerBound,
            upperUnbounded ? null : upperBound, lowerInclusive, upperInclusive);
      case BYTES:
        return new BytesRawValueBasedRangePredicateEvaluator(rangePredicate,
            lowerUnbounded ? null : BytesUtils.toBytes(lowerBound),
            upperUnbounded ? null : BytesUtils.toBytes(upperBound), lowerInclusive, upperInclusive);
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  public static final class SortedDictionaryBasedRangePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator
      implements IntRange {
    final int _startDictId;
    // Exclusive
    final int _endDictId;
    final int _numMatchingDictIds;
    int[] _matchingDictIds;

    SortedDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, Dictionary dictionary,
        DataType dataType) {
      super(rangePredicate);
      String lowerBound = rangePredicate.getLowerBound();
      String upperBound = rangePredicate.getUpperBound();
      boolean lowerInclusive = rangePredicate.isLowerInclusive();
      boolean upperInclusive = rangePredicate.isUpperInclusive();

      if (lowerBound.equals(RangePredicate.UNBOUNDED)) {
        _startDictId = 0;
      } else {
        int insertionIndex = dictionary.insertionIndexOf(PredicateUtils.getStoredValue(lowerBound, dataType));
        if (insertionIndex < 0) {
          _startDictId = -(insertionIndex + 1);
        } else {
          if (lowerInclusive) {
            _startDictId = insertionIndex;
          } else {
            _startDictId = insertionIndex + 1;
          }
        }
      }
      if (upperBound.equals(RangePredicate.UNBOUNDED)) {
        _endDictId = dictionary.length();
      } else {
        int insertionIndex = dictionary.insertionIndexOf(PredicateUtils.getStoredValue(upperBound, dataType));
        if (insertionIndex < 0) {
          _endDictId = -(insertionIndex + 1);
        } else {
          if (upperInclusive) {
            _endDictId = insertionIndex + 1;
          } else {
            _endDictId = insertionIndex;
          }
        }
      }

      _numMatchingDictIds = _endDictId - _startDictId;
      if (_numMatchingDictIds <= 0) {
        _alwaysFalse = true;
      } else if (dictionary.length() == _numMatchingDictIds) {
        _alwaysTrue = true;
      }
    }

    public int getStartDictId() {
      return _startDictId;
    }

    public int getEndDictId() {
      return _endDictId;
    }

    @Override
    public boolean applySV(int dictId) {
      return _startDictId <= dictId && _endDictId > dictId;
    }

    @Override
    public int applySV(int limit, int[] docIds, int[] dictIds) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        int dictId = dictIds[i];
        if (applySV(dictId)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public int getNumMatchingDictIds() {
      return _numMatchingDictIds;
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        if (_numMatchingDictIds <= 0) {
          _matchingDictIds = new int[0];
        } else {
          _matchingDictIds = new int[_numMatchingDictIds];
          for (int i = 0; i < _numMatchingDictIds; i++) {
            _matchingDictIds[i] = _startDictId + i;
          }
        }
      }
      return _matchingDictIds;
    }

    @Override
    public int getNumMatchingItems() {
      return Math.max(_numMatchingDictIds, 0);
    }

    @Override
    public int getInclusiveLowerBound() {
      return getStartDictId();
    }

    @Override
    public int getInclusiveUpperBound() {
      return getEndDictId() - 1;
    }
  }

  private static final class UnsortedDictionaryBasedRangePredicateEvaluator
      extends BaseDictionaryBasedPredicateEvaluator {
    // When the cardinality of the column is lower than this threshold, pre-calculate the matching dictionary ids;
    // otherwise, fetch the value when evaluating each dictionary id.
    // TODO: Tune this threshold
    private static final int DICT_ID_SET_BASED_CARDINALITY_THRESHOLD = 1000;

    final Dictionary _dictionary;
    final boolean _dictIdSetBased;
    final IntSet _matchingDictIdSet;
    final BaseRawValueBasedPredicateEvaluator _rawValueBasedEvaluator;

    UnsortedDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, Dictionary dictionary,
        DataType dataType) {
      super(rangePredicate);
      _dictionary = dictionary;
      int cardinality = dictionary.length();
      if (cardinality < DICT_ID_SET_BASED_CARDINALITY_THRESHOLD) {
        _dictIdSetBased = true;
        _rawValueBasedEvaluator = null;
        String lowerBound = rangePredicate.getLowerBound();
        if (!lowerBound.equals(RangePredicate.UNBOUNDED)) {
          lowerBound = PredicateUtils.getStoredValue(lowerBound, dataType);
        }
        String upperBound = rangePredicate.getUpperBound();
        if (!upperBound.equals(RangePredicate.UNBOUNDED)) {
          upperBound = PredicateUtils.getStoredValue(upperBound, dataType);
        }
        _matchingDictIdSet = dictionary.getDictIdsInRange(lowerBound, upperBound, rangePredicate.isLowerInclusive(),
            rangePredicate.isUpperInclusive());
        int numMatchingDictIds = _matchingDictIdSet.size();
        if (numMatchingDictIds == 0) {
          _alwaysFalse = true;
        } else if (numMatchingDictIds == cardinality) {
          _alwaysTrue = true;
        }
      } else {
        _dictIdSetBased = false;
        _matchingDictIdSet = null;
        _rawValueBasedEvaluator = newRawValueBasedEvaluator(rangePredicate, dataType);
      }
    }

    @Override
    public boolean applySV(int dictId) {
      if (_dictIdSetBased) {
        return _matchingDictIdSet.contains(dictId);
      } else {
        switch (_dictionary.getValueType()) {
          case INT:
            return _rawValueBasedEvaluator.applySV(_dictionary.getIntValue(dictId));
          case LONG:
            return _rawValueBasedEvaluator.applySV(_dictionary.getLongValue(dictId));
          case FLOAT:
            return _rawValueBasedEvaluator.applySV(_dictionary.getFloatValue(dictId));
          case DOUBLE:
            return _rawValueBasedEvaluator.applySV(_dictionary.getDoubleValue(dictId));
          case BIG_DECIMAL:
            return _rawValueBasedEvaluator.applySV(_dictionary.getBigDecimalValue(dictId));
          case STRING:
            return _rawValueBasedEvaluator.applySV(_dictionary.getStringValue(dictId));
          case BYTES:
            return _rawValueBasedEvaluator.applySV(_dictionary.getBytesValue(dictId));
          default:
            throw new IllegalStateException("Unsupported value type: " + _dictionary.getValueType());
        }
      }
    }

    @Override
    public int getNumMatchingItems() {
      return _matchingDictIdSet == null ? super.getNumMatchingItems() : _matchingDictIdSet.size();
    }

    @Override
    public int[] getMatchingDictIds() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class IntRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator
      implements IntRange {
    final int _inclusiveLowerBound;
    final int _inclusiveUpperBound;

    IntRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate, int lowerBound, int upperBound,
        boolean lowerInclusive, boolean upperInclusive) {
      super(rangePredicate);
      if (lowerInclusive) {
        _inclusiveLowerBound = lowerBound;
      } else {
        _inclusiveLowerBound = lowerBound + 1;
        Preconditions.checkArgument(_inclusiveLowerBound > lowerBound, "Invalid range: %s", rangePredicate);
      }
      if (upperInclusive) {
        _inclusiveUpperBound = upperBound;
      } else {
        _inclusiveUpperBound = upperBound - 1;
        Preconditions.checkArgument(_inclusiveUpperBound < upperBound, "Invalid range: %s", rangePredicate);
      }
    }

    @Override
    public int getInclusiveLowerBound() {
      return _inclusiveLowerBound;
    }

    @Override
    public int getInclusiveUpperBound() {
      return _inclusiveUpperBound;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return value >= _inclusiveLowerBound && value <= _inclusiveUpperBound;
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
  }

  private static final class LongRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator
      implements LongRange {
    final long _inclusiveLowerBound;
    final long _inclusiveUpperBound;

    LongRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate, long lowerBound, long upperBound,
        boolean lowerInclusive, boolean upperInclusive) {
      super(rangePredicate);
      if (lowerInclusive) {
        _inclusiveLowerBound = lowerBound;
      } else {
        _inclusiveLowerBound = lowerBound + 1;
        Preconditions.checkArgument(_inclusiveLowerBound > lowerBound, "Invalid range: %s", rangePredicate);
      }
      if (upperInclusive) {
        _inclusiveUpperBound = upperBound;
      } else {
        _inclusiveUpperBound = upperBound - 1;
        Preconditions.checkArgument(_inclusiveUpperBound < upperBound, "Invalid range: %s", rangePredicate);
      }
    }

    @Override
    public long getInclusiveLowerBound() {
      return _inclusiveLowerBound;
    }

    @Override
    public long getInclusiveUpperBound() {
      return _inclusiveUpperBound;
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      return value >= _inclusiveLowerBound && value <= _inclusiveUpperBound;
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
  }

  private static final class FloatRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator
      implements FloatRange {
    final float _inclusiveLowerBound;
    final float _inclusiveUpperBound;

    FloatRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate, float lowerBound, float upperBound,
        boolean lowerInclusive, boolean upperInclusive) {
      super(rangePredicate);
      if (lowerInclusive) {
        _inclusiveLowerBound = lowerBound;
      } else {
        _inclusiveLowerBound = Math.nextUp(lowerBound);
        Preconditions.checkArgument(_inclusiveLowerBound > lowerBound, "Invalid range: %s", rangePredicate);
      }
      if (upperInclusive) {
        _inclusiveUpperBound = upperBound;
      } else {
        _inclusiveUpperBound = Math.nextDown(upperBound);
        Preconditions.checkArgument(_inclusiveUpperBound < upperBound, "Invalid range: %s", rangePredicate);
      }
    }

    @Override
    public float getInclusiveLowerBound() {
      return _inclusiveLowerBound;
    }

    @Override
    public float getInclusiveUpperBound() {
      return _inclusiveUpperBound;
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      return value >= _inclusiveLowerBound && value <= _inclusiveUpperBound;
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
  }

  private static final class DoubleRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator
      implements DoubleRange {
    final double _inclusiveLowerBound;
    final double _inclusiveUpperBound;

    DoubleRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate, double lowerBound, double upperBound,
        boolean lowerInclusive, boolean upperInclusive) {
      super(rangePredicate);
      if (lowerInclusive) {
        _inclusiveLowerBound = lowerBound;
      } else {
        _inclusiveLowerBound = Math.nextUp(lowerBound);
        Preconditions.checkArgument(_inclusiveLowerBound > lowerBound, "Invalid range: %s", rangePredicate);
      }
      if (upperInclusive) {
        _inclusiveUpperBound = upperBound;
      } else {
        _inclusiveUpperBound = Math.nextDown(upperBound);
        Preconditions.checkArgument(_inclusiveUpperBound < upperBound, "Invalid range: %s", rangePredicate);
      }
    }

    @Override
    public double getInclusiveLowerBound() {
      return _inclusiveLowerBound;
    }

    @Override
    public double getInclusiveUpperBound() {
      return _inclusiveUpperBound;
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      return value >= _inclusiveLowerBound && value <= _inclusiveUpperBound;
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
  }

  public static final class BigDecimalRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final BigDecimal _lowerBound;
    final BigDecimal _upperBound;
    final int _lowerComparisonValue;
    final int _upperComparisonValue;

    BigDecimalRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate, BigDecimal lowerBound,
        BigDecimal upperBound, boolean lowerInclusive, boolean upperInclusive) {
      super(rangePredicate);
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _lowerComparisonValue = lowerInclusive ? 0 : 1;
      _upperComparisonValue = upperInclusive ? 0 : -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.BIG_DECIMAL;
    }

    @Override
    public boolean applySV(BigDecimal value) {
      return (_lowerBound == null || value.compareTo(_lowerBound) >= _lowerComparisonValue) && (_upperBound == null
          || value.compareTo(_upperBound) <= _upperComparisonValue);
    }
  }

  private static final class StringRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final String _lowerBound;
    final String _upperBound;
    final int _lowerComparisonValue;
    final int _upperComparisonValue;

    StringRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate, String lowerBound, String upperBound,
        boolean lowerInclusive, boolean upperInclusive) {
      super(rangePredicate);
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _lowerComparisonValue = lowerInclusive ? 0 : 1;
      _upperComparisonValue = upperInclusive ? 0 : -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return (_lowerBound == null || value.compareTo(_lowerBound) >= _lowerComparisonValue) && (_upperBound == null
          || value.compareTo(_upperBound) <= _upperComparisonValue);
    }
  }

  private static final class BytesRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final byte[] _lowerBound;
    final byte[] _upperBound;
    final int _lowerComparisonValue;
    final int _upperComparisonValue;

    BytesRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate, byte[] lowerBound, byte[] upperBound,
        boolean lowerInclusive, boolean upperInclusive) {
      super(rangePredicate);
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _lowerComparisonValue = lowerInclusive ? 0 : 1;
      _upperComparisonValue = upperInclusive ? 0 : -1;
    }

    @Override
    public DataType getDataType() {
      return DataType.BYTES;
    }

    @Override
    public boolean applySV(byte[] value) {
      return (_lowerBound == null || ByteArray.compare(value, _lowerBound) >= _lowerComparisonValue) && (
          _upperBound == null || ByteArray.compare(value, _upperBound) <= _upperComparisonValue);
    }
  }
}
