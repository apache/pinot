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
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.common.predicate.RangePredicate;
import org.apache.pinot.core.realtime.impl.dictionary.BaseMutableDictionary;
import org.apache.pinot.core.segment.index.readers.BaseImmutableDictionary;
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
    if (dictionary instanceof BaseImmutableDictionary) {
      return new OfflineDictionaryBasedRangePredicateEvaluator(rangePredicate, (BaseImmutableDictionary) dictionary);
    } else {
      return new RealtimeDictionaryBasedRangePredicateEvaluator(rangePredicate, (BaseMutableDictionary) dictionary,
          dataType);
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

  public static final class OfflineDictionaryBasedRangePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _startDictId;
    // Exclusive
    final int _endDictId;
    final int _numMatchingDictIds;
    int[] _matchingDictIds;

    OfflineDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, BaseImmutableDictionary dictionary) {
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      boolean includeLowerBoundary = rangePredicate.includeLowerBoundary();
      boolean includeUpperBoundary = rangePredicate.includeUpperBoundary();

      if (lowerBoundary.equals(RangePredicate.UNBOUNDED)) {
        _startDictId = 0;
      } else {
        int insertionIndex = dictionary.insertionIndexOf(lowerBoundary);
        if (insertionIndex < 0) {
          _startDictId = -(insertionIndex + 1);
        } else {
          if (includeLowerBoundary) {
            _startDictId = insertionIndex;
          } else {
            _startDictId = insertionIndex + 1;
          }
        }
      }
      if (upperBoundary.equals(RangePredicate.UNBOUNDED)) {
        _endDictId = dictionary.length();
      } else {
        int insertionIndex = dictionary.insertionIndexOf(upperBoundary);
        if (insertionIndex < 0) {
          _endDictId = -(insertionIndex + 1);
        } else {
          if (includeUpperBoundary) {
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

  private static final class RealtimeDictionaryBasedRangePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    // When the cardinality of the column is lower than this threshold, pre-calculate the matching dictionary ids;
    // otherwise, fetch the value when evaluating each dictionary id.
    // TODO: Tune this threshold
    private static final int DICT_ID_SET_BASED_CARDINALITY_THRESHOLD = 1000;

    final BaseMutableDictionary _dictionary;
    final DataType _dataType;
    final boolean _dictIdSetBased;
    final IntSet _matchingDictIdSet;
    final BaseRawValueBasedPredicateEvaluator _rawValueBasedEvaluator;

    RealtimeDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, BaseMutableDictionary dictionary,
        DataType dataType) {
      _dictionary = dictionary;
      _dataType = dataType;
      int cardinality = dictionary.length();
      if (cardinality < DICT_ID_SET_BASED_CARDINALITY_THRESHOLD) {
        _dictIdSetBased = true;
        _rawValueBasedEvaluator = null;
        _matchingDictIdSet = dictionary
            .getDictIdsInRange(rangePredicate.getLowerBoundary(), rangePredicate.getUpperBoundary(),
                rangePredicate.includeLowerBoundary(), rangePredicate.includeUpperBoundary());
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

  private static final class IntRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final int _lowerBoundary;
    final int _upperBoundary;
    final boolean _includeLowerBoundary;
    final boolean _includeUpperBoundary;

    IntRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary =
          lowerBoundary.equals(RangePredicate.UNBOUNDED) ? Integer.MIN_VALUE : Integer.parseInt(lowerBoundary);
      _upperBoundary =
          upperBoundary.equals(RangePredicate.UNBOUNDED) ? Integer.MAX_VALUE : Integer.parseInt(upperBoundary);
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
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
      if (_includeLowerBoundary) {
        result = _lowerBoundary <= value;
      } else {
        result = _lowerBoundary < value;
      }
      if (_includeUpperBoundary) {
        result &= _upperBoundary >= value;
      } else {
        result &= _upperBoundary > value;
      }
      return result;
    }
  }

  private static final class LongRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final long _lowerBoundary;
    final long _upperBoundary;
    final boolean _includeLowerBoundary;
    final boolean _includeUpperBoundary;

    LongRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary = lowerBoundary.equals(RangePredicate.UNBOUNDED) ? Long.MIN_VALUE : Long.parseLong(lowerBoundary);
      _upperBoundary = upperBoundary.equals(RangePredicate.UNBOUNDED) ? Long.MAX_VALUE : Long.parseLong(upperBoundary);
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
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
      if (_includeLowerBoundary) {
        result = _lowerBoundary <= value;
      } else {
        result = _lowerBoundary < value;
      }
      if (_includeUpperBoundary) {
        result &= _upperBoundary >= value;
      } else {
        result &= _upperBoundary > value;
      }
      return result;
    }
  }

  private static final class FloatRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final float _lowerBoundary;
    final float _upperBoundary;
    final boolean _includeLowerBoundary;
    final boolean _includeUpperBoundary;

    FloatRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary =
          lowerBoundary.equals(RangePredicate.UNBOUNDED) ? Float.NEGATIVE_INFINITY : Float.parseFloat(lowerBoundary);
      _upperBoundary =
          upperBoundary.equals(RangePredicate.UNBOUNDED) ? Float.POSITIVE_INFINITY : Float.parseFloat(upperBoundary);
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
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
      if (_includeLowerBoundary) {
        result = _lowerBoundary <= value;
      } else {
        result = _lowerBoundary < value;
      }
      if (_includeUpperBoundary) {
        result &= _upperBoundary >= value;
      } else {
        result &= _upperBoundary > value;
      }
      return result;
    }
  }

  private static final class DoubleRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final double _lowerBoundary;
    final double _upperBoundary;
    final boolean _includeLowerBoundary;
    final boolean _includeUpperBoundary;

    DoubleRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary =
          lowerBoundary.equals(RangePredicate.UNBOUNDED) ? Double.NEGATIVE_INFINITY : Double.parseDouble(lowerBoundary);
      _upperBoundary =
          upperBoundary.equals(RangePredicate.UNBOUNDED) ? Double.POSITIVE_INFINITY : Double.parseDouble(upperBoundary);
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
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
      if (_includeLowerBoundary) {
        result = _lowerBoundary <= value;
      } else {
        result = _lowerBoundary < value;
      }
      if (_includeUpperBoundary) {
        result &= _upperBoundary >= value;
      } else {
        result &= _upperBoundary > value;
      }
      return result;
    }
  }

  private static final class StringRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final String _lowerBoundary;
    final String _upperBoundary;
    final boolean _includeLowerBoundary;
    final boolean _includeUpperBoundary;

    StringRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary = lowerBoundary.equals(RangePredicate.UNBOUNDED) ? null : lowerBoundary;
      _upperBoundary = upperBoundary.equals(RangePredicate.UNBOUNDED) ? null : upperBoundary;
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
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
      if (_lowerBoundary != null) {
        if (_includeLowerBoundary) {
          result = _lowerBoundary.compareTo(value) <= 0;
        } else {
          result = _lowerBoundary.compareTo(value) < 0;
        }
      }
      if (_upperBoundary != null) {
        if (_includeUpperBoundary) {
          result &= _upperBoundary.compareTo(value) >= 0;
        } else {
          result &= _upperBoundary.compareTo(value) > 0;
        }
      }
      return result;
    }
  }

  private static final class BytesRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final byte[] _lowerBoundary;
    final byte[] _upperBoundary;
    final boolean _includeLowerBoundary;
    final boolean _includeUpperBoundary;

    BytesRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary = lowerBoundary.equals(RangePredicate.UNBOUNDED) ? null : BytesUtils.toBytes(lowerBoundary);
      _upperBoundary = upperBoundary.equals(RangePredicate.UNBOUNDED) ? null : BytesUtils.toBytes(upperBoundary);
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
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
      if (_lowerBoundary != null) {
        if (_includeLowerBoundary) {
          result = ByteArray.compare(_lowerBoundary, value) <= 0;
        } else {
          result = ByteArray.compare(_lowerBoundary, value) < 0;
        }
      }
      if (_upperBoundary != null) {
        if (_includeUpperBoundary) {
          result &= ByteArray.compare(_upperBoundary, value) >= 0;
        } else {
          result &= ByteArray.compare(_upperBoundary, value) > 0;
        }
      }
      return result;
    }
  }
}
