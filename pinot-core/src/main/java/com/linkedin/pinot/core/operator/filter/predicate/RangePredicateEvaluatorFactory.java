/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.ArrayList;
import java.util.List;


/**
 * Factory for Range _predicate evaluators with offline dictionary.
 */
public class RangePredicateEvaluatorFactory {

  // Private constructor
  private RangePredicateEvaluatorFactory() {

  }

  /**
   * Returns a new instance of dictionary based equality _predicate evaluator.
   * @param predicate Predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based equality _predicate evaluator
   */
  public static PredicateEvaluator newOfflineDictionaryBasedEvaluator(RangePredicate predicate,
      ImmutableDictionaryReader dictionary) {
    return new OfflineDictionaryBasedPredicateEvaluator(predicate, dictionary);
  }

  /**
   * Returns a new instance of dictionary based equality _predicate evaluator.
   * @param predicate Predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based equality _predicate evaluator
   */
  public static PredicateEvaluator newRealtimeDictionaryBasedEvaluator(RangePredicate predicate,
      MutableDictionaryReader dictionary) {
    return new RealtimeDictionaryBasedPredicateEvaluator(predicate, dictionary);
  }

  /**
   * Returns a new instance of no-dictionary based equality _predicate evaluator.
   * @param predicate Predicate to evaluate
   * @param dataType Data type for the column
   * @return No Dictionary based equality _predicate evaluator
   */
  public static PredicateEvaluator newNoDictionaryBasedEvaluator(RangePredicate predicate,
      FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntNoDictionaryBasedRangeEvaluator(predicate);

      case LONG:
        return new LongNoDictionaryBasedRangeEvaluator(predicate);

      case FLOAT:
        return new FloatNoDictionaryBasedRangeEvaluator(predicate);

      case DOUBLE:
        return new DoubleNoDictionaryBasedRangeEvaluator(predicate);

      case STRING:
        return new StringNoDictionaryBasedRangeEvaluator(predicate);

      default:
        throw new UnsupportedOperationException(
            "No dictionary based Equals predicate evaluator not supported for datatype:" + dataType);
    }
  }

  public static class OfflineDictionaryBasedPredicateEvaluator extends BasePredicateEvaluator {
    private int[] _matchingIds;
    private RangePredicate _predicate;
    private int _rangeStartIndex = 0;
    private int _rangeEndIndex = 0;
    int _matchingSize;

    public OfflineDictionaryBasedPredicateEvaluator(RangePredicate predicate,
        ImmutableDictionaryReader dictionary) {
      this._predicate = predicate;

      final boolean incLower = predicate.includeLowerBoundary();
      final boolean incUpper = predicate.includeUpperBoundary();
      final String lower = predicate.getLowerBoundary();
      final String upper = predicate.getUpperBoundary();

      if (lower.equals("*")) {
        _rangeStartIndex = 0;
      } else {
        _rangeStartIndex = dictionary.indexOf(lower);
      }
      if (upper.equals("*")) {
        _rangeEndIndex = dictionary.length() - 1;
      } else {
        _rangeEndIndex = dictionary.indexOf(upper);
      }

      if (_rangeStartIndex < 0) {
        _rangeStartIndex = -(_rangeStartIndex + 1);
      } else if (!incLower && !lower.equals("*")) {
        _rangeStartIndex += 1;
      }

      if (_rangeEndIndex < 0) {
        _rangeEndIndex = -(_rangeEndIndex + 1) - 1;
      } else if (!incUpper && !upper.equals("*")) {
        _rangeEndIndex -= 1;
      }

      _matchingSize = (_rangeEndIndex - _rangeStartIndex) + 1;
      if (_matchingSize < 0) {
        _matchingSize = 0;
      }
    }

    @Override
    public boolean apply(int dictionaryId) {
      return dictionaryId >= _rangeStartIndex && dictionaryId <= _rangeEndIndex;
    }

    @Override
    public boolean apply(int[] dictionaryIds) {
      for (int dictId : dictionaryIds) {
        if (dictId >= _rangeStartIndex && dictId <= _rangeEndIndex) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int[] getMatchingDictionaryIds() {
      if (_matchingIds == null) {
        _matchingIds = new int[_matchingSize];
        int counter = 0;
        for (int i = _rangeStartIndex; i <= _rangeEndIndex; i++) {
          _matchingIds[counter++] = i;
        }
      }
      return _matchingIds;
    }

    @Override
    public int[] getNonMatchingDictionaryIds() {
      throw new UnsupportedOperationException(
          "Returning non matching values is expensive for predicateType:" + _predicate.getType());
    }

    @Override
    public boolean apply(int[] dictionaryIds, int length) {
      for (int i = 0; i < length; i++) {
        int dictId = dictionaryIds[i];
        if (dictId >= _rangeStartIndex && dictId <= _rangeEndIndex) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean alwaysFalse() {
      return ((_rangeEndIndex - _rangeStartIndex) + 1) <= 0;
    }
  }

  public static class RealtimeDictionaryBasedPredicateEvaluator extends BasePredicateEvaluator {

    private int[] _matchingIds;
    private IntSet _dictIdSet;
    private RangePredicate _predicate;

    public RealtimeDictionaryBasedPredicateEvaluator(RangePredicate predicate,
        MutableDictionaryReader dictionary) {
      this._predicate = predicate;
      List<Integer> ids = new ArrayList<>();
      String rangeStart;
      String rangeEnd;

      if (dictionary.isEmpty()) {
        _matchingIds = new int[0];
        return;
      }

      final boolean incLower = predicate.includeLowerBoundary();
      final boolean incUpper = predicate.includeUpperBoundary();
      final String lower = predicate.getLowerBoundary();
      final String upper = predicate.getUpperBoundary();

      if (lower.equals("*")) {
        rangeStart = dictionary.getMinVal().toString();
      } else {
        rangeStart = lower;
      }

      if (upper.equals("*")) {
        rangeEnd = dictionary.getMaxVal().toString();
      } else {
        rangeEnd = upper;
      }

      for (int dicId = 0; dicId < dictionary.length(); dicId++) {
        if (dictionary.inRange(rangeStart, rangeEnd, dicId, incLower, incUpper)) {
          ids.add(dicId);
        }
      }
      _matchingIds = new int[ids.size()];
      _dictIdSet = new IntOpenHashSet(ids.size());
      for (int i = 0; i < _matchingIds.length; i++) {
        _matchingIds[i] = ids.get(i);
        _dictIdSet.add(ids.get(i));
      }
    }

    @Override
    public boolean apply(int dictionaryId) {
      return _dictIdSet.contains(dictionaryId);
    }

    @Override
    public boolean apply(int[] dictionaryIds) {
      for (int dictId : dictionaryIds) {
        if (_dictIdSet.contains(dictId)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public int[] getMatchingDictionaryIds() {
      return _matchingIds;
    }

    @Override
    public int[] getNonMatchingDictionaryIds() {
      throw new UnsupportedOperationException(
          "Returning non matching values is expensive for predicateType:" + _predicate.getType());
    }

    @Override
    public boolean apply(int[] dictionaryIds, int length) {
      for (int i = 0; i < length; i++) {
        int dictId = dictionaryIds[i];
        if (_dictIdSet.contains(dictId)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean alwaysFalse() {
      return _matchingIds.length == 0;
    }
  }

  /**
   * No dictionary implementation of range _predicate evaluator for INT data type.
   */
  public static class IntNoDictionaryBasedRangeEvaluator extends BasePredicateEvaluator {
    private final int _rangeStart;
    private final int _rangeEnd;
    private final boolean _incLower;
    private final boolean _incUpper;

    public IntNoDictionaryBasedRangeEvaluator(RangePredicate predicate) {
      _incLower = predicate.includeLowerBoundary();
      _incUpper = predicate.includeUpperBoundary();
      final String lower = predicate.getLowerBoundary();
      final String upper = predicate.getUpperBoundary();

      _rangeStart = lower.equals("*") ? Integer.MIN_VALUE : Integer.parseInt(lower);
      _rangeEnd = upper.equals("*") ? Integer.MAX_VALUE : Integer.parseInt(upper);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public boolean apply(int inputValue) {
      if (_incLower) {
        if (_incUpper) {
          return (inputValue >= _rangeStart && inputValue <= _rangeEnd);
        } else {
          return (inputValue >= _rangeStart && inputValue < _rangeEnd);
        }
      } else {
        if (_incUpper) {
          return (inputValue > _rangeStart && inputValue <= _rangeEnd);
        } else {
          return (inputValue > _rangeStart && inputValue < _rangeEnd);
        }
      }
    }

    @Override
    public boolean apply(int[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(int[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        int inputValue = inputValues[i];
        if (apply(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * No dictionary implementation of range _predicate evaluator for LONG data type.
   */
  public static class LongNoDictionaryBasedRangeEvaluator extends BasePredicateEvaluator {
    private final long _rangeStart;
    private final long _rangeEnd;
    private final boolean _incLower;
    private final boolean _incUpper;

    public LongNoDictionaryBasedRangeEvaluator(RangePredicate predicate) {
      _incLower = predicate.includeLowerBoundary();
      _incUpper = predicate.includeUpperBoundary();
      final String lower = predicate.getLowerBoundary();
      final String upper = predicate.getUpperBoundary();

      _rangeStart = lower.equals("*") ? Long.MIN_VALUE : Long.parseLong(lower);
      _rangeEnd = upper.equals("*") ? Long.MAX_VALUE : Long.parseLong(upper);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public boolean apply(long inputValue) {
      if (_incLower) {
        if (_incUpper) {
          return (inputValue >= _rangeStart && inputValue <= _rangeEnd);
        } else {
          return (inputValue >= _rangeStart && inputValue < _rangeEnd);
        }
      } else {
        if (_incUpper) {
          return (inputValue > _rangeStart && inputValue <= _rangeEnd);
        } else {
          return (inputValue > _rangeStart && inputValue < _rangeEnd);
        }
      }
    }

    @Override
    public boolean apply(long[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(int[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        long inputValue = inputValues[i];
        if (apply(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * No dictionary implementation of range _predicate evaluator for FLOAT data type.
   */
  public static class FloatNoDictionaryBasedRangeEvaluator extends BasePredicateEvaluator {
    private final float _rangeStart;
    private final float _rangeEnd;
    private final boolean _incLower;
    private final boolean _incUpper;

    public FloatNoDictionaryBasedRangeEvaluator(RangePredicate predicate) {
      _incLower = predicate.includeLowerBoundary();
      _incUpper = predicate.includeUpperBoundary();
      final String lower = predicate.getLowerBoundary();
      final String upper = predicate.getUpperBoundary();

      _rangeStart = lower.equals("*") ? Float.NEGATIVE_INFINITY : Float.parseFloat(lower);
      _rangeEnd = upper.equals("*") ? Float.POSITIVE_INFINITY : Float.parseFloat(upper);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public boolean apply(float inputValue) {
      if (_incLower) {
        if (_incUpper) {
          return (inputValue >= _rangeStart && inputValue <= _rangeEnd);
        } else {
          return (inputValue >= _rangeStart && inputValue < _rangeEnd);
        }
      } else {
        if (_incUpper) {
          return (inputValue > _rangeStart && inputValue <= _rangeEnd);
        } else {
          return (inputValue > _rangeStart && inputValue < _rangeEnd);
        }
      }
    }

    @Override
    public boolean apply(float[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(int[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        float inputValue = inputValues[i];
        if (apply(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * No dictionary implementation of range _predicate evaluator for DOUBLE data type.
   */
  public static class DoubleNoDictionaryBasedRangeEvaluator extends BasePredicateEvaluator {
    private final double _rangeStart;
    private final double _rangeEnd;
    private final boolean _incLower;
    private final boolean _incUpper;

    public DoubleNoDictionaryBasedRangeEvaluator(RangePredicate predicate) {
      _incLower = predicate.includeLowerBoundary();
      _incUpper = predicate.includeUpperBoundary();
      final String lower = predicate.getLowerBoundary();
      final String upper = predicate.getUpperBoundary();

      _rangeStart = lower.equals("*") ? Double.NEGATIVE_INFINITY : Double.parseDouble(lower);
      _rangeEnd = upper.equals("*") ? Double.POSITIVE_INFINITY : Double.parseDouble(upper);
    }

    @Override
    @SuppressWarnings("Duplicates")
    public boolean apply(double inputValue) {
      if (_incLower) {
        if (_incUpper) {
          return (inputValue >= _rangeStart && inputValue <= _rangeEnd);
        } else {
          return (inputValue >= _rangeStart && inputValue < _rangeEnd);
        }
      } else {
        if (_incUpper) {
          return (inputValue > _rangeStart && inputValue <= _rangeEnd);
        } else {
          return (inputValue > _rangeStart && inputValue < _rangeEnd);
        }
      }
    }

    @Override
    public boolean apply(double[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(double[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        double inputValue = inputValues[i];
        if (apply(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * No dictionary implementation of range _predicate evaluator for STRING data type.
   */
  public static class StringNoDictionaryBasedRangeEvaluator extends BasePredicateEvaluator {
    private final String _rangeStart;
    private final String _rangeEnd;
    private final boolean _incLower;
    private final boolean _incUpper;

    public StringNoDictionaryBasedRangeEvaluator(RangePredicate predicate) {
      _incLower = predicate.includeLowerBoundary();
      _incUpper = predicate.includeUpperBoundary();

      String lower = predicate.getLowerBoundary();
      String upper = predicate.getUpperBoundary();
      _rangeStart = lower.equals("*") ? null : lower;
      _rangeEnd = upper.equals("*") ? null : upper;
    }

    @Override
    public boolean apply(String inputValue) {
      int compareLower = (_rangeStart != null) ? inputValue.compareTo(_rangeStart) : 1;
      int compareUpper = (_rangeEnd != null) ? inputValue.compareTo(_rangeEnd) : -1;

      if (_incLower) {
        if (_incUpper) {
          return compareLower >= 0 && compareUpper <= 0;
        } else {
          return compareLower >= 0 && compareUpper < 0;
        }
      } else {
        if (_incUpper) {
          return compareLower > 0 && compareUpper <= 0;
        } else {
          return compareLower > 0 && compareUpper < 0;
        }
      }
    }

    @Override
    public boolean apply(String[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(String[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        String inputValue = inputValues[i];
        if (apply(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }
}
