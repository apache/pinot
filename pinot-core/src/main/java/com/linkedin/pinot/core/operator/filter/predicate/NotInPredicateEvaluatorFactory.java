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
import com.linkedin.pinot.core.common.Predicate;
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
 * Factory for NOT_IN predicate evaluators.
 */
public class NotInPredicateEvaluatorFactory {
  private NotInPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based NOT_IN predicate evaluator.
   *
   * @param notInPredicate NOT_IN predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based NOT_IN predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(NotInPredicate notInPredicate,
      Dictionary dictionary) {
    return new DictionaryBasedNotInPredicateEvaluator(notInPredicate, dictionary);
  }

  /**
   * Create a new instance of raw value based NOT_IN predicate evaluator.
   *
   * @param notInPredicate NOT_IN predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based NOT_IN predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(NotInPredicate notInPredicate,
      FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntRawValueBasedNotInPredicateEvaluator(notInPredicate);
      case LONG:
        return new LongRawValueBasedNotInPredicateEvaluator(notInPredicate);
      case FLOAT:
        return new FloatRawValueBasedNotInPredicateEvaluator(notInPredicate);
      case DOUBLE:
        return new DoubleRawValueBasedNotInPredicateEvaluator(notInPredicate);
      case STRING:
        return new StringRawValueBasedNotInPredicateEvaluator(notInPredicate);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  public static final class DictionaryBasedNotInPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final IntSet _nonMatchingDictIdSet;
    final Dictionary _dictionary;
    int[] _matchingDictIds;
    int[] _nonMatchingDictIds;

    DictionaryBasedNotInPredicateEvaluator(NotInPredicate notInPredicate, Dictionary dictionary) {
      String[] values = notInPredicate.getValues();
      _nonMatchingDictIdSet = new IntOpenHashSet(values.length);
      for (String value : values) {
        int dictId = dictionary.indexOf(value);
        if (dictId >= 0) {
          _nonMatchingDictIdSet.add(dictId);
        }
      }
      _dictionary = dictionary;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public boolean isAlwaysFalse() {
      return _nonMatchingDictIdSet.size() == _dictionary.length();
    }

    @Override
    public boolean applySV(int dictId) {
      return !_nonMatchingDictIdSet.contains(dictId);
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        int dictionarySize = _dictionary.length();
        _matchingDictIds = new int[dictionarySize - _nonMatchingDictIdSet.size()];
        int index = 0;
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          if (!_nonMatchingDictIdSet.contains(dictId)) {
            _matchingDictIds[index++] = dictId;
          }
        }
      }
      return _matchingDictIds;
    }

    @Override
    public int[] getNonMatchingDictIds() {
      if (_nonMatchingDictIds == null) {
        _nonMatchingDictIds = _nonMatchingDictIdSet.toIntArray();
      }
      return _nonMatchingDictIds;
    }
  }

  private static final class IntRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final IntSet _nonMatchingValues;

    IntRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate) {
      String[] values = notInPredicate.getValues();
      _nonMatchingValues = new IntOpenHashSet(values.length);
      for (String value : values) {
        _nonMatchingValues.add(Integer.parseInt(value));
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public boolean applySV(int value) {
      return !_nonMatchingValues.contains(value);
    }
  }

  private static final class LongRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final LongSet _nonMatchingValues;

    LongRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate) {
      String[] values = notInPredicate.getValues();
      _nonMatchingValues = new LongOpenHashSet(values.length);
      for (String value : values) {
        _nonMatchingValues.add(Long.parseLong(value));
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public boolean applySV(long value) {
      return !_nonMatchingValues.contains(value);
    }
  }

  private static final class FloatRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final FloatSet _nonMatchingValues;

    FloatRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate) {
      String[] values = notInPredicate.getValues();
      _nonMatchingValues = new FloatOpenHashSet(values.length);
      for (String value : values) {
        _nonMatchingValues.add(Float.parseFloat(value));
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public boolean applySV(float value) {
      return !_nonMatchingValues.contains(value);
    }
  }

  private static final class DoubleRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final DoubleSet _nonMatchingValues;

    DoubleRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate) {
      String[] values = notInPredicate.getValues();
      _nonMatchingValues = new DoubleOpenHashSet(values.length);
      for (String value : values) {
        _nonMatchingValues.add(Double.parseDouble(value));
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public boolean applySV(double value) {
      return !_nonMatchingValues.contains(value);
    }
  }

  private static final class StringRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final Set<String> _nonMatchingValues;

    StringRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate) {
      String[] values = notInPredicate.getValues();
      _nonMatchingValues = new HashSet<>(values.length);
      Collections.addAll(_nonMatchingValues, values);
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public boolean applySV(String value) {
      return !_nonMatchingValues.contains(value);
    }
  }
}
