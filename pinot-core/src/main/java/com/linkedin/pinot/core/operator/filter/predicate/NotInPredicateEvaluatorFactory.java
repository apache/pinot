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
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * Factory for Not-in predicate evaluators.
 */
public class NotInPredicateEvaluatorFactory {

  // Private constructor
  private NotInPredicateEvaluatorFactory() {

  }

  /**
   * Returns a new instance of dictionary based equality predicate evaluator.
   * @param predicate Predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based equality predicate evaluator
   */
  public static PredicateEvaluator newDictionaryBasedEvaluator(NotInPredicate predicate, Dictionary dictionary) {
    return new DictionaryBasedNotInPredicateEvaluator(predicate, dictionary);
  }

  /**
   * Returns a new instance of no-dictionary based equality predicate evaluator.
   * @param predicate Predicate to evaluate
   * @param dataType Data type for the column
   * @return No Dictionary based equality predicate evaluator
   */
  public static PredicateEvaluator newNoDictionaryBasedEvaluator(NotInPredicate predicate, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntNoDictionaryBasedNotInEvaluator(predicate);

      case LONG:
        return new LongNoDictionaryBasedNotInEvaluator(predicate);

      case FLOAT:
        return new FloatNoDictionaryBasedNotInEvaluator(predicate);

      case DOUBLE:
        return new DoubleNoDictionaryBasedNotInEvaluator(predicate);

      case STRING:
        return new StringNoDictionaryBasedNotInEvaluator(predicate);

      default:
        throw new UnsupportedOperationException(
            "No dictionary based Equals predicate evaluator not supported for datatype:" + dataType);
    }
  }

  /**
   * Dictionary based implementation of not-in predicate evaluator.
   */
  public static class DictionaryBasedNotInPredicateEvaluator extends BasePredicateEvaluator {

    private int[] _matchingIds;
    private int[] _nonMatchingIds;
    private Dictionary _dictionary;
    private IntSet _nonMatchingDictIdSet;

    public DictionaryBasedNotInPredicateEvaluator(NotInPredicate predicate, Dictionary dictionary) {
      _dictionary = dictionary;
      final String[] notInValues = predicate.getNotInRange();

      _nonMatchingDictIdSet = new IntOpenHashSet(notInValues.length);
      for (final String notInValue : notInValues) {
        int dictId = dictionary.indexOf(notInValue);
        if (dictId >= 0) {
          _nonMatchingDictIdSet.add(dictId);
        }
      }

      _nonMatchingIds = new int[_nonMatchingDictIdSet.size()];
      int index = 0;
      for (int dictId : _nonMatchingDictIdSet) {
        _nonMatchingIds[index] = dictId;
        index = index + 1;
      }
    }

    @Override
    public boolean apply(int dictionaryId) {
      return (!_nonMatchingDictIdSet.contains(dictionaryId));
    }

    @Override
    public boolean apply(int[] dictionaryIds) {
      for (int dictId : dictionaryIds) {
        if (_nonMatchingDictIdSet.contains(dictId)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int[] getMatchingDictionaryIds() {
      //This is expensive for NOT IN predicate, some operators need this for now. Eventually we should remove the need for exposing matching dict ids
      if (_matchingIds == null) {
        int count = 0;
        _matchingIds = new int[_dictionary.length() - _nonMatchingDictIdSet.size()];
        for (int i = 0; i < _dictionary.length(); i++) {
          if (!_nonMatchingDictIdSet.contains(i)) {
            _matchingIds[count] = i;
            count = count + 1;
          }
        }
      }
      return _matchingIds;
    }

    @Override
    public int[] getNonMatchingDictionaryIds() {
      return _nonMatchingIds;
    }

    @Override
    public boolean apply(int[] dictionaryIds, int length) {
      for (int i = 0; i < length; i++) {
        int dictId = dictionaryIds[i];
        if (_nonMatchingDictIdSet.contains(dictId)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean alwaysFalse() {
      return _nonMatchingIds.length == _dictionary.length();
    }
  }

  /**
   * No dictionary implementation of not-in predicate evaluator for INT data type.
   */
  private static class IntNoDictionaryBasedNotInEvaluator extends BasePredicateEvaluator {
    IntSet _nonMatchingValues;

    public IntNoDictionaryBasedNotInEvaluator(NotInPredicate predicate) {
      _nonMatchingValues = new IntOpenHashSet();
      for (String valueString : predicate.getNotInRange()) {
        _nonMatchingValues.add(Integer.parseInt(valueString));
      }
    }

    @Override
    public boolean apply(int inputValue) {
      return (!_nonMatchingValues.contains(inputValue));
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
        if (_nonMatchingValues.contains(inputValue)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * No dictionary implementation of not-in predicate evaluator for LONG data type.
   */
  private static class LongNoDictionaryBasedNotInEvaluator extends BasePredicateEvaluator {
    LongSet _nonMatchingValues;

    public LongNoDictionaryBasedNotInEvaluator(NotInPredicate predicate) {
      _nonMatchingValues = new LongOpenHashSet();
      for (String valueString : predicate.getNotInRange()) {
        _nonMatchingValues.add(Long.parseLong(valueString));
      }
    }

    @Override
    public boolean apply(long inputValue) {
      return (!_nonMatchingValues.contains(inputValue));
    }

    @Override
    public boolean apply(long[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(long[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        long inputValue = inputValues[i];
        if (_nonMatchingValues.contains(inputValue)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * No dictionary implementation of not-in predicate evaluator for FLOAT data type.
   */
  private static class FloatNoDictionaryBasedNotInEvaluator extends BasePredicateEvaluator {
    FloatSet _nonMatchingValues;

    public FloatNoDictionaryBasedNotInEvaluator(NotInPredicate predicate) {
      _nonMatchingValues = new FloatOpenHashSet();
      for (String valueString : predicate.getNotInRange()) {
        _nonMatchingValues.add(Float.parseFloat(valueString));
      }
    }

    @Override
    public boolean apply(float inputValue) {
      return (!_nonMatchingValues.contains(inputValue));
    }

    @Override
    public boolean apply(float[] inputValues) {
      return apply(inputValues, inputValues.length);
    }

    @Override
    public boolean apply(float[] inputValues, int length) {

      // we cannot do binary search since the multi-value columns are not sorted in the raw segment
      for (int i = 0; i < length; i++) {
        float inputValue = inputValues[i];
        if (_nonMatchingValues.contains(inputValue)) {
          return false;
        }
      }
      return true;
    }
  }


  /**
   * No dictionary implementation of not-in predicate evaluator for DOUBLE data type.
   */
  private static class DoubleNoDictionaryBasedNotInEvaluator extends BasePredicateEvaluator {
    DoubleSet _nonMatchingValues;

    public DoubleNoDictionaryBasedNotInEvaluator(NotInPredicate predicate) {
      _nonMatchingValues = new DoubleOpenHashSet();
      for (String valueString : predicate.getNotInRange()) {
        _nonMatchingValues.add(Double.parseDouble(valueString));
      }
    }

    @Override
    public boolean apply(double inputValue) {
      return (!_nonMatchingValues.contains(inputValue));
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
        if (_nonMatchingValues.contains(inputValue)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * No dictionary implementation of not-in predicate evaluator for STRING data type.
   */
  private static class StringNoDictionaryBasedNotInEvaluator extends BasePredicateEvaluator {
    Set<String> _nonMatchingValues;

    public StringNoDictionaryBasedNotInEvaluator(NotInPredicate predicate) {
      _nonMatchingValues = new HashSet<>();
      Collections.addAll(_nonMatchingValues, predicate.getNotInRange());
    }

    @Override
    public boolean apply(String inputValue) {
      return (!_nonMatchingValues.contains(inputValue));
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
        if (_nonMatchingValues.contains(inputValue)) {
          return false;
        }
      }
      return true;
    }
  }
}
