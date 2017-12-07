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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;


/**
 * Factory for IN predicate evaluators.
 */
public class InPredicateEvaluatorFactory {
  private InPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based IN predicate evaluator.
   *
   * @param inPredicate IN predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based IN predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(InPredicate inPredicate,
      Dictionary dictionary) {
    return new DictionaryBasedInPredicateEvaluator(inPredicate, dictionary);
  }

  /**
   * Create a new instance of raw value based IN predicate evaluator.
   *
   * @param inPredicate IN predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based IN predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(InPredicate inPredicate,
      FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntRawValueBasedInPredicateEvaluator(inPredicate);
      case LONG:
        return new LongRawValueBasedInPredicateEvaluator(inPredicate);
      case FLOAT:
        return new FloatRawValueBasedInPredicateEvaluator(inPredicate);
      case DOUBLE:
        return new DoubleRawValueBasedInPredicateEvaluator(inPredicate);
      case STRING:
        return new StringRawValueBasedInPredicateEvaluator(inPredicate);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedInPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final IntSet _matchingDictIdSet;
    int[] _matchingDictIds;

    DictionaryBasedInPredicateEvaluator(InPredicate inPredicate, Dictionary dictionary) {
      String[] values = inPredicate.getValues();
      _matchingDictIdSet = new IntOpenHashSet();
      for (String value : values) {
        int dictId = dictionary.indexOf(value);
        if (dictId >= 0) {
          _matchingDictIdSet.add(dictId);
        }
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public boolean isAlwaysFalse() {
      return _matchingDictIdSet.isEmpty();
    }

    @Override
    public boolean applySV(int dictId) {
      return _matchingDictIdSet.contains(dictId);
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        _matchingDictIds = _matchingDictIdSet.toIntArray();
      }
      return _matchingDictIds;
    }
  }

  private static final class IntRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final IntSet _matchingValues;

    IntRawValueBasedInPredicateEvaluator(InPredicate inPredicate) {
      String[] values = inPredicate.getValues();
      _matchingValues = new IntOpenHashSet(values.length);
      for (String value : values) {
        _matchingValues.add(Integer.parseInt(value));
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public boolean applySV(int value) {
      return _matchingValues.contains(value);
    }
  }

  private static final class LongRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final LongSet _matchingValues;

    LongRawValueBasedInPredicateEvaluator(InPredicate predicate) {
      String[] values = predicate.getValues();
      _matchingValues = new LongOpenHashSet(values.length);
      for (String value : values) {
        _matchingValues.add(Long.parseLong(value));
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public boolean applySV(long value) {
      return _matchingValues.contains(value);
    }
  }

  private static final class FloatRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final FloatSet _matchingValues;

    FloatRawValueBasedInPredicateEvaluator(InPredicate inPredicate) {
      String[] values = inPredicate.getValues();
      _matchingValues = new FloatOpenHashSet(values.length);
      for (String value : values) {
        _matchingValues.add(Float.parseFloat(value));
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public boolean applySV(float value) {
      return _matchingValues.contains(value);
    }
  }

  private static final class DoubleRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final DoubleSet _matchingValues;

    DoubleRawValueBasedInPredicateEvaluator(InPredicate inPredicate) {
      String[] values = inPredicate.getValues();
      _matchingValues = new DoubleOpenHashSet(values.length);
      for (String value : values) {
        _matchingValues.add(Double.parseDouble(value));
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public boolean applySV(double value) {
      return _matchingValues.contains(value);
    }
  }

  private static final class StringRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final Set<String> _matchingValues;

    StringRawValueBasedInPredicateEvaluator(InPredicate inPredicate) {
      String[] values = inPredicate.getValues();
      _matchingValues = new HashSet<>(values.length);
      Collections.addAll(_matchingValues, values);
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public boolean applySV(String value) {
      return _matchingValues.contains(value);
    }
  }
}
