/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionary;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;


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
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private static final class OfflineDictionaryBasedRangePredicateEvaluator
      extends BaseDictionaryBasedPredicateEvaluator {
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
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.RANGE;
    }

    @Override
    public boolean isAlwaysFalse() {
      return _startDictId >= _endDictId;
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

  private static final class RealtimeDictionaryBasedRangePredicateEvaluator
      extends BaseDictionaryBasedPredicateEvaluator {
    final IntSet _matchingDictIdSet;
    int[] _matchingDictIds;

    RealtimeDictionaryBasedRangePredicateEvaluator(RangePredicate rangePredicate, MutableDictionary dictionary) {
      _matchingDictIdSet = new IntOpenHashSet();

      int dictionarySize = dictionary.length();
      if (dictionarySize == 0) {
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
      return _matchingDictIdSet.size();
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        _matchingDictIds = _matchingDictIdSet.toIntArray();
      }
      return _matchingDictIds;
    }

    @Override
    public int[] getNonMatchingDictIds() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAlwaysFalse() {
      return _matchingDictIdSet.isEmpty();
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
}
