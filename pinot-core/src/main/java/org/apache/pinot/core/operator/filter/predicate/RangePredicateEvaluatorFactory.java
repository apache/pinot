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
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.predicate.RangePredicate;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;


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
      return new SortedDictionaryBasedRangePredicateEvaluator(rangePredicate, dictionary);
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
    switch (dataType) {
      case INT:
        return new IntRawValueBasedRangePredicateEvaluator(rangePredicate);
      case LONG:
        return new LongRawValueBasedRangePredicateEvaluator(rangePredicate);
      case FLOAT:
        return new FloatRawValueBasedRangePredicateEvaluator(rangePredicate);
      case DOUBLE:
        return new DoubleRawValueBasedRangePredicateEvaluator(rangePredicate);
      case STRING:
        return new StringRawValueBasedRangePredicateEvaluator(rangePredicate);
      case BYTES:
        return new BytesRawValueBasedRangePredicateEvaluator(rangePredicate);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  public static final class SortedDictionaryBasedRangePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _startDictId;
    // Exclusive
    final int _endDictId;
    final int _numMatchingDictIds;
    int[] _matchingDictIds;

    SortedDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, Dictionary dictionary) {
      String lowerBound = rangePredicate.getLowerBound();
      String upperBound = rangePredicate.getUpperBound();
      boolean lowerInclusive = rangePredicate.isLowerInclusive();
      boolean upperInclusive = rangePredicate.isUpperInclusive();

      if (lowerBound.equals(RangePredicate.UNBOUNDED)) {
        _startDictId = 0;
      } else {
        int insertionIndex = dictionary.insertionIndexOf(lowerBound);
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
        int insertionIndex = dictionary.insertionIndexOf(upperBound);
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
    final DataType _dataType;
    final boolean _dictIdSetBased;
    final IntSet _matchingDictIdSet;
    final BaseRawValueBasedPredicateEvaluator _rawValueBasedEvaluator;

    UnsortedDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, Dictionary dictionary,
        DataType dataType) {
      _dictionary = dictionary;
      _dataType = dataType;
      int cardinality = dictionary.length();
      if (cardinality < DICT_ID_SET_BASED_CARDINALITY_THRESHOLD) {
        _dictIdSetBased = true;
        _rawValueBasedEvaluator = null;
        _matchingDictIdSet = dictionary
            .getDictIdsInRange(rangePredicate.getLowerBound(), rangePredicate.getUpperBound(),
                rangePredicate.isLowerInclusive(), rangePredicate.isUpperInclusive());
        int numMatchingDictIds = _matchingDictIdSet.size();
        if (numMatchingDictIds == 0) {
          _alwaysFalse = true;
        } else if (numMatchingDictIds == cardinality) {
          _alwaysTrue = true;
        }
      } else {
        _dictIdSetBased = false;
        _matchingDictIdSet = null;
        switch (dataType) {
          case INT:
            _rawValueBasedEvaluator = new IntRawValueBasedRangePredicateEvaluator(rangePredicate);
            break;
          case LONG:
            _rawValueBasedEvaluator = new LongRawValueBasedRangePredicateEvaluator(rangePredicate);
            break;
          case FLOAT:
            _rawValueBasedEvaluator = new FloatRawValueBasedRangePredicateEvaluator(rangePredicate);
            break;
          case DOUBLE:
            _rawValueBasedEvaluator = new DoubleRawValueBasedRangePredicateEvaluator(rangePredicate);
            break;
          case STRING:
            _rawValueBasedEvaluator = new StringRawValueBasedRangePredicateEvaluator(rangePredicate);
            break;
          case BYTES:
            _rawValueBasedEvaluator = new BytesRawValueBasedRangePredicateEvaluator(rangePredicate);
            break;
          default:
            throw new IllegalStateException();
        }
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
        switch (_dataType) {
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

    IntRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBound = rangePredicate.getLowerBound();
      String upperBound = rangePredicate.getUpperBound();
      // NOTE: Handle unbounded as inclusive min/max value of the data type
      boolean lowerUnbounded = lowerBound.equals(RangePredicate.UNBOUNDED);
      boolean upperUnbounded = upperBound.equals(RangePredicate.UNBOUNDED);
      _lowerBound = lowerUnbounded ? Integer.MIN_VALUE : Integer.parseInt(lowerBound);
      _upperBound = upperUnbounded ? Integer.MAX_VALUE : Integer.parseInt(upperBound);
      _lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
      _upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();
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

    LongRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBound = rangePredicate.getLowerBound();
      String upperBound = rangePredicate.getUpperBound();
      // NOTE: Handle unbounded as inclusive min/max value of the data type
      boolean lowerUnbounded = lowerBound.equals(RangePredicate.UNBOUNDED);
      boolean upperUnbounded = upperBound.equals(RangePredicate.UNBOUNDED);
      _lowerBound = lowerUnbounded ? Long.MIN_VALUE : Long.parseLong(lowerBound);
      _upperBound = upperUnbounded ? Long.MAX_VALUE : Long.parseLong(upperBound);
      _lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
      _upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();
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

    FloatRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBound = rangePredicate.getLowerBound();
      String upperBound = rangePredicate.getUpperBound();
      // NOTE: Handle unbounded as inclusive min/max value of the data type
      boolean lowerUnbounded = lowerBound.equals(RangePredicate.UNBOUNDED);
      boolean upperUnbounded = upperBound.equals(RangePredicate.UNBOUNDED);
      _lowerBound = lowerUnbounded ? Float.NEGATIVE_INFINITY : Float.parseFloat(lowerBound);
      _upperBound = upperUnbounded ? Float.POSITIVE_INFINITY : Float.parseFloat(upperBound);
      _lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
      _upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();
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

    DoubleRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBound = rangePredicate.getLowerBound();
      String upperBound = rangePredicate.getUpperBound();
      // NOTE: Handle unbounded as inclusive min/max value of the data type
      boolean lowerUnbounded = lowerBound.equals(RangePredicate.UNBOUNDED);
      boolean upperUnbounded = upperBound.equals(RangePredicate.UNBOUNDED);
      _lowerBound = lowerUnbounded ? Double.NEGATIVE_INFINITY : Double.parseDouble(lowerBound);
      _upperBound = upperUnbounded ? Double.POSITIVE_INFINITY : Double.parseDouble(upperBound);
      _lowerInclusive = lowerUnbounded || rangePredicate.isLowerInclusive();
      _upperInclusive = upperUnbounded || rangePredicate.isUpperInclusive();
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

    StringRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBound = rangePredicate.getLowerBound();
      String upperBound = rangePredicate.getUpperBound();
      _lowerBound = lowerBound.equals(RangePredicate.UNBOUNDED) ? null : lowerBound;
      _upperBound = upperBound.equals(RangePredicate.UNBOUNDED) ? null : upperBound;
      _lowerInclusive = rangePredicate.isLowerInclusive();
      _upperInclusive = rangePredicate.isUpperInclusive();
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

    BytesRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBound = rangePredicate.getLowerBound();
      String upperBound = rangePredicate.getUpperBound();
      _lowerBound = lowerBound.equals(RangePredicate.UNBOUNDED) ? null : BytesUtils.toBytes(lowerBound);
      _upperBound = upperBound.equals(RangePredicate.UNBOUNDED) ? null : BytesUtils.toBytes(upperBound);
      _lowerInclusive = rangePredicate.isLowerInclusive();
      _upperInclusive = rangePredicate.isUpperInclusive();
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
