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
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * Factory for IN predicate evaluators.
 */
public class InPredicateEvaluatorFactory extends BasePredicateEvaluator {

  // Private constructor
  private InPredicateEvaluatorFactory() {

  }

  /**
   * Returns a new instance of dictionary based IN predicate evaluator.
   * @param predicate Predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based equality predicate evaluator
   */
  public static PredicateEvaluator newDictionaryBasedEvaluator(InPredicate predicate, Dictionary dictionary) {
    return new DictionaryBasedInPredicateEvaluator(predicate, dictionary);
  }

  /**
   * Returns a new instance of no-dictionary based IN predicate evaluator.
   * @param predicate Predicate to evaluate
   * @param dataType Data type for the column
   * @return No Dictionary based equality predicate evaluator
   */
  public static PredicateEvaluator newNoDictionaryBasedEvaluator(InPredicate predicate, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntNoDictionaryBasedInPredicateEvaluator(predicate);

      case LONG:
        return new LongNoDictionaryBasedInPredicateEvaluator(predicate);

      case FLOAT:
        return new FloatNoDictionaryBasedInPredicateEvaluator(predicate);

      case DOUBLE:
        return new DoubleNoDictionaryBasedInPredicateEvaluator(predicate);

      case STRING:
        return new StringNoDictionaryBasedInPredicateEvaluator(predicate);

      default:
        throw new UnsupportedOperationException(
            "No dictionary based Equals predicate evaluator not supported for datatype:" + dataType);
    }
  }

  /**
   * Dictionary based implementation for IN predicate evaluator.
   */
  public static final class DictionaryBasedInPredicateEvaluator extends BasePredicateEvaluator {
    private int[] _matchingIds;
    private IntSet _dictIdSet;
    private InPredicate _predicate;

    public DictionaryBasedInPredicateEvaluator(InPredicate predicate, Dictionary dictionary) {

      _predicate = predicate;
      _dictIdSet = new IntOpenHashSet();
      final String[] inValues = predicate.getInRange();
      for (final String value : inValues) {
        final int index = dictionary.indexOf(value);
        if (index >= 0) {
          _dictIdSet.add(index);
        }
      }
      _matchingIds = new int[_dictIdSet.size()];
      int i = 0;
      for (int dictId : _dictIdSet) {
        _matchingIds[i++] = dictId;
      }
      Arrays.sort(_matchingIds);
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
      return _matchingIds == null || _matchingIds.length == 0;
    }
  }

  /**
   * No dictionary implementation of IN predicate evaluator for INT data type.
   */
  private static class IntNoDictionaryBasedInPredicateEvaluator extends BasePredicateEvaluator {
    IntSet _matchingValues;

    public IntNoDictionaryBasedInPredicateEvaluator(InPredicate predicate) {
      _matchingValues = new IntOpenHashSet();
      for (String valueString : predicate.getInRange()) {
        _matchingValues.add(Integer.parseInt(valueString));
      }
    }

    @Override
    public boolean apply(int inputValue) {
      return (_matchingValues.contains(inputValue));
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
        if (_matchingValues.contains(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * No dictionary implementation of IN predicate evaluator for LONG data type.
   */
  private static class LongNoDictionaryBasedInPredicateEvaluator extends BasePredicateEvaluator {
    LongSet _matchingValues;

    public LongNoDictionaryBasedInPredicateEvaluator(InPredicate predicate) {
      _matchingValues = new LongOpenHashSet();
      for (String valueString : predicate.getInRange()) {
        _matchingValues.add(Long.parseLong(valueString));
      }
    }

    @Override
    public boolean apply(long inputValue) {
      return (_matchingValues.contains(inputValue));
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
        if (_matchingValues.contains(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * No dictionary implementation of IN predicate evaluator for FLOAT data type.
   */
  private static class FloatNoDictionaryBasedInPredicateEvaluator extends BasePredicateEvaluator {
    FloatSet _matchingValues;

    public FloatNoDictionaryBasedInPredicateEvaluator(InPredicate predicate) {
      _matchingValues = new FloatOpenHashSet();
      for (String valueString : predicate.getInRange()) {
        _matchingValues.add(Float.parseFloat(valueString));
      }
    }

    @Override
    public boolean apply(float inputValue) {
      return (_matchingValues.contains(inputValue));
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
        if (_matchingValues.contains(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * No dictionary implementation of IN predicate evaluator for DOUBLE data type.
   */
  private static class DoubleNoDictionaryBasedInPredicateEvaluator extends BasePredicateEvaluator {
    DoubleSet _matchingValues;

    public DoubleNoDictionaryBasedInPredicateEvaluator(InPredicate predicate) {
      _matchingValues = new DoubleOpenHashSet();
      for (String valueString : predicate.getInRange()) {
        _matchingValues.add(Double.parseDouble(valueString));
      }
    }

    @Override
    public boolean apply(double inputValue) {
      return (_matchingValues.contains(inputValue));
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
        if (_matchingValues.contains(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }

  /**
   * No dictionary implementation of IN predicate evaluator for DOUBLE data type.
   */
  private static class StringNoDictionaryBasedInPredicateEvaluator extends BasePredicateEvaluator {
    Set<String> _matchingValues;

    public StringNoDictionaryBasedInPredicateEvaluator(InPredicate predicate) {
      _matchingValues = new HashSet<>();
      Collections.addAll(_matchingValues, predicate.getInRange());
    }

    @Override
    public boolean apply(String inputValue) {
      return (_matchingValues.contains(inputValue));
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
        if (_matchingValues.contains(inputValue)) {
          return true;
        }
      }
      return false;
    }
  }
}
