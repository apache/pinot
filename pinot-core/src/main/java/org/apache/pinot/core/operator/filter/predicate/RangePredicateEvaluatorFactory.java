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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.utils.BytesUtils;
import org.apache.pinot.common.utils.primitive.ByteArray;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.common.predicate.RangePredicate;
import org.apache.pinot.core.realtime.impl.dictionary.MutableDictionary;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.ImmutableDictionaryReader;


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
   * @return Dictionary based RANGE predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(RangePredicate rangePredicate,
      Dictionary dictionary) {
    if (dictionary instanceof ImmutableDictionaryReader) {
      return new OfflineDictionaryBasedRangePredicateEvaluator(rangePredicate, (ImmutableDictionaryReader) dictionary);
    } else {
      return new RealtimeDictionaryBasedRangePredicateEvaluator(rangePredicate, (MutableDictionary) dictionary);
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
      FieldSpec.DataType dataType) {
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

  private static final class OfflineDictionaryBasedRangePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _startDictId;
    // Exclusive
    final int _endDictId;
    final int _numMatchingDictIds;
    int[] _matchingDictIds;

    OfflineDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, ImmutableDictionaryReader dictionary) {
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      boolean includeLowerBoundary = rangePredicate.includeLowerBoundary();
      boolean includeUpperBoundary = rangePredicate.includeUpperBoundary();

      if (lowerBoundary.equals("*")) {
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
      if (upperBoundary.equals("*")) {
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
    final IntSet _matchingDictIdSet;
    final int _numMatchingDictIds;
    int[] _matchingDictIds;

    RealtimeDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, MutableDictionary dictionary) {
      _matchingDictIdSet = new IntOpenHashSet();

      int dictionarySize = dictionary.length();
      if (dictionarySize == 0) {
        _numMatchingDictIds = 0;
        _alwaysFalse = true;
        return;
      }

      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      boolean includeLowerBoundary = rangePredicate.includeLowerBoundary();
      boolean includeUpperBoundary = rangePredicate.includeUpperBoundary();

      if (lowerBoundary.equals("*")) {
        lowerBoundary = dictionary.getMinVal().toString();
      }
      if (upperBoundary.equals("*")) {
        upperBoundary = dictionary.getMaxVal().toString();
      }

      for (int dictId = 0; dictId < dictionarySize; dictId++) {
        if (dictionary.inRange(lowerBoundary, upperBoundary, dictId, includeLowerBoundary, includeUpperBoundary)) {
          _matchingDictIdSet.add(dictId);
        }
      }

      _numMatchingDictIds = _matchingDictIdSet.size();
      if (_numMatchingDictIds == 0) {
        _alwaysFalse = true;
      } else if (dictionarySize == _numMatchingDictIds) {
        _alwaysTrue = true;
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public boolean applySV(int dictId) {
      return _matchingDictIdSet.contains(dictId);
    }

    @Override
    public int getNumMatchingDictIds() {
      return _numMatchingDictIds;
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        _matchingDictIds = _matchingDictIdSet.toIntArray();
      }
      return _matchingDictIds;
    }
  }

  private static final class IntRawValueBasedRangePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final int _lowerBoundary;
    final int _upperBoundary;
    final boolean _includeLowerBoundary;
    final boolean _includeUpperBoundary;

    IntRawValueBasedRangePredicateEvaluator(RangePredicate rangePredicate) {
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary = lowerBoundary.equals("*") ? Integer.MIN_VALUE : Integer.parseInt(lowerBoundary);
      _upperBoundary = upperBoundary.equals("*") ? Integer.MAX_VALUE : Integer.parseInt(upperBoundary);
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
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
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary = lowerBoundary.equals("*") ? Integer.MIN_VALUE : Long.parseLong(lowerBoundary);
      _upperBoundary = upperBoundary.equals("*") ? Integer.MAX_VALUE : Long.parseLong(upperBoundary);
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
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
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary = lowerBoundary.equals("*") ? Integer.MIN_VALUE : Float.parseFloat(lowerBoundary);
      _upperBoundary = upperBoundary.equals("*") ? Integer.MAX_VALUE : Float.parseFloat(upperBoundary);
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
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
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
      String lowerBoundary = rangePredicate.getLowerBoundary();
      String upperBoundary = rangePredicate.getUpperBoundary();
      _lowerBoundary = lowerBoundary.equals("*") ? Integer.MIN_VALUE : Double.parseDouble(lowerBoundary);
      _upperBoundary = upperBoundary.equals("*") ? Integer.MAX_VALUE : Double.parseDouble(upperBoundary);
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
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
      _lowerBoundary = rangePredicate.getLowerBoundary();
      _upperBoundary = rangePredicate.getUpperBoundary();
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public boolean applySV(String value) {
      boolean result = true;
      if (!_lowerBoundary.equals("*")) {
        if (_includeLowerBoundary) {
          result = _lowerBoundary.compareTo(value) <= 0;
        } else {
          result = _lowerBoundary.compareTo(value) < 0;
        }
      }
      if (!_upperBoundary.equals("*")) {
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
      if (!"*".equals(rangePredicate.getLowerBoundary())) {
        _lowerBoundary = BytesUtils.toBytes(rangePredicate.getLowerBoundary());
      } else {
        _lowerBoundary = null;
      }
      if (!"*".equals(rangePredicate.getUpperBoundary())) {
        _upperBoundary = BytesUtils.toBytes(rangePredicate.getUpperBoundary());
      } else {
        _upperBoundary = null;
      }
      _includeLowerBoundary = rangePredicate.includeLowerBoundary();
      _includeUpperBoundary = rangePredicate.includeUpperBoundary();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
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
