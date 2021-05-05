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

import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
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
        return new IntRawValueBasedRangePredicateEvaluator(
            lowerUnbounded ? Integer.MIN_VALUE : Integer.parseInt(lowerBound),
            upperUnbounded ? Integer.MAX_VALUE : Integer.parseInt(upperBound), lowerInclusive, upperInclusive);
      case LONG:
        return new LongRawValueBasedRangePredicateEvaluator(
            lowerUnbounded ? Long.MIN_VALUE : Long.parseLong(lowerBound),
            upperUnbounded ? Long.MAX_VALUE : Long.parseLong(upperBound), lowerInclusive, upperInclusive);
      case FLOAT:
        return new FloatRawValueBasedRangePredicateEvaluator(
            lowerUnbounded ? Float.NEGATIVE_INFINITY : Float.parseFloat(lowerBound),
            upperUnbounded ? Float.POSITIVE_INFINITY : Float.parseFloat(upperBound), lowerInclusive, upperInclusive);
      case DOUBLE:
        return new DoubleRawValueBasedRangePredicateEvaluator(
            lowerUnbounded ? Double.NEGATIVE_INFINITY : Double.parseDouble(lowerBound),
            upperUnbounded ? Double.POSITIVE_INFINITY : Double.parseDouble(upperBound), lowerInclusive, upperInclusive);
      case BOOLEAN:
        return new IntRawValueBasedRangePredicateEvaluator(
            lowerUnbounded ? Integer.MIN_VALUE : BooleanUtils.toInt(lowerBound),
            upperUnbounded ? Integer.MAX_VALUE : BooleanUtils.toInt(upperBound), lowerInclusive, upperInclusive);
      case TIMESTAMP:
        return new LongRawValueBasedRangePredicateEvaluator(
            lowerUnbounded ? Long.MIN_VALUE : TimestampUtils.toMillisSinceEpoch(lowerBound),
            upperUnbounded ? Long.MAX_VALUE : TimestampUtils.toMillisSinceEpoch(upperBound), lowerInclusive,
            upperInclusive);
      case STRING:
      case JSON:
        return new StringRawValueBasedRangePredicateEvaluator(lowerUnbounded ? null : lowerBound,
            upperUnbounded ? null : upperBound, lowerInclusive, upperInclusive);
      case BYTES:
        return new BytesRawValueBasedRangePredicateEvaluator(lowerUnbounded ? null : BytesUtils.toBytes(lowerBound),
            upperUnbounded ? null : BytesUtils.toBytes(upperBound), lowerInclusive, upperInclusive);
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  public static final class SortedDictionaryBasedRangePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _startDictId;
    // Exclusive
    final int _endDictId;
    final int _numMatchingDictIds;
    int[] _matchingDictIds;

    SortedDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, Dictionary dictionary,
        DataType dataType) {
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
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public boolean applySV(int dictId) {
      return _startDictId <= dictId && _endDictId > dictId;
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
  }

  private static final class UnsortedDictionaryBasedRangePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
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
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
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
          case STRING:
            return _rawValueBasedEvaluator.applySV(_dictionary.getStringValue(dictId));
          case BYTES:
            return _rawValueBasedEvaluator.applySV(_dictionary.getBytesValue(dictId));
          default:
            throw new IllegalStateException();
        }
      }
    }

    @Override
    public int[] getMatchingDictIds() {
      throw new UnsupportedOperationException();
    }
  }

  public static final class IntRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final int _lowerBound;
    final int _upperBound;
    final boolean _lowerInclusive;
    final boolean _upperInclusive;

    IntRawValueBasedRangePredicateEvaluator(int lowerBound, int upperBound, boolean lowerInclusive,
        boolean upperInclusive) {
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _lowerInclusive = lowerInclusive;
      _upperInclusive = upperInclusive;
    }

    public int geLowerBound() {
      return _lowerBound;
    }

    public int getUpperBound() {
      return _upperBound;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      boolean result;
      if (_lowerInclusive) {
        result = _lowerBound <= value;
      } else {
        result = _lowerBound < value;
      }
      if (_upperInclusive) {
        result &= _upperBound >= value;
      } else {
        result &= _upperBound > value;
      }
      return result;
    }
  }

  public static final class LongRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final long _lowerBound;
    final long _upperBound;
    final boolean _lowerInclusive;
    final boolean _upperInclusive;

    LongRawValueBasedRangePredicateEvaluator(long lowerBound, long upperBound, boolean lowerInclusive,
        boolean upperInclusive) {
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _lowerInclusive = lowerInclusive;
      _upperInclusive = upperInclusive;
    }

    public long geLowerBound() {
      return _lowerBound;
    }

    public long getUpperBound() {
      return _upperBound;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      boolean result;
      if (_lowerInclusive) {
        result = _lowerBound <= value;
      } else {
        result = _lowerBound < value;
      }
      if (_upperInclusive) {
        result &= _upperBound >= value;
      } else {
        result &= _upperBound > value;
      }
      return result;
    }
  }

  public static final class FloatRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final float _lowerBound;
    final float _upperBound;
    final boolean _lowerInclusive;
    final boolean _upperInclusive;

    FloatRawValueBasedRangePredicateEvaluator(float lowerBound, float upperBound, boolean lowerInclusive,
        boolean upperInclusive) {
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _lowerInclusive = lowerInclusive;
      _upperInclusive = upperInclusive;
    }

    public float geLowerBound() {
      return _lowerBound;
    }

    public float getUpperBound() {
      return _upperBound;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      boolean result;
      if (_lowerInclusive) {
        result = _lowerBound <= value;
      } else {
        result = _lowerBound < value;
      }
      if (_upperInclusive) {
        result &= _upperBound >= value;
      } else {
        result &= _upperBound > value;
      }
      return result;
    }
  }

  public static final class DoubleRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final double _lowerBound;
    final double _upperBound;
    final boolean _lowerInclusive;
    final boolean _upperInclusive;

    DoubleRawValueBasedRangePredicateEvaluator(double lowerBound, double upperBound, boolean lowerInclusive,
        boolean upperInclusive) {
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _lowerInclusive = lowerInclusive;
      _upperInclusive = upperInclusive;
    }

    public double geLowerBound() {
      return _lowerBound;
    }

    public double getUpperBound() {
      return _upperBound;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      boolean result;
      if (_lowerInclusive) {
        result = _lowerBound <= value;
      } else {
        result = _lowerBound < value;
      }
      if (_upperInclusive) {
        result &= _upperBound >= value;
      } else {
        result &= _upperBound > value;
      }
      return result;
    }
  }

  private static final class StringRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final String _lowerBound;
    final String _upperBound;
    final boolean _lowerInclusive;
    final boolean _upperInclusive;

    StringRawValueBasedRangePredicateEvaluator(String lowerBound, String upperBound, boolean lowerInclusive,
        boolean upperInclusive) {
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _lowerInclusive = lowerInclusive;
      _upperInclusive = upperInclusive;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      boolean result = true;
      if (_lowerBound != null) {
        if (_lowerInclusive) {
          result = _lowerBound.compareTo(value) <= 0;
        } else {
          result = _lowerBound.compareTo(value) < 0;
        }
      }
      if (_upperBound != null) {
        if (_upperInclusive) {
          result &= _upperBound.compareTo(value) >= 0;
        } else {
          result &= _upperBound.compareTo(value) > 0;
        }
      }
      return result;
    }
  }

  private static final class BytesRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final byte[] _lowerBound;
    final byte[] _upperBound;
    final boolean _lowerInclusive;
    final boolean _upperInclusive;

    BytesRawValueBasedRangePredicateEvaluator(byte[] lowerBound, byte[] upperBound, boolean lowerInclusive,
        boolean upperInclusive) {
      _lowerBound = lowerBound;
      _upperBound = upperBound;
      _lowerInclusive = lowerInclusive;
      _upperInclusive = upperInclusive;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public DataType getDataType() {
      return DataType.BYTES;
    }

    @Override
    public boolean applySV(byte[] value) {
      boolean result = true;
      if (_lowerBound != null) {
        if (_lowerInclusive) {
          result = ByteArray.compare(_lowerBound, value) <= 0;
        } else {
          result = ByteArray.compare(_lowerBound, value) < 0;
        }
      }
      if (_upperBound != null) {
        if (_upperInclusive) {
          result &= ByteArray.compare(_upperBound, value) >= 0;
        } else {
          result &= ByteArray.compare(_upperBound, value) > 0;
        }
      }
      return result;
    }
  }
}
