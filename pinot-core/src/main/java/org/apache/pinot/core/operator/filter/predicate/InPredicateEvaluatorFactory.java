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

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.doubles.DoubleSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


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
   * @param dataType Data type for the column
   * @return Dictionary based IN predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(InPredicate inPredicate,
      Dictionary dictionary, DataType dataType) {
    return new DictionaryBasedInPredicateEvaluator(inPredicate, dictionary, dataType);
  }

  /**
   * Create a new instance of raw value based IN predicate evaluator.
   *
   * @param inPredicate IN predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based IN predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(InPredicate inPredicate,
      DataType dataType) {
    List<String> values = inPredicate.getValues();
    int hashSetSize = HashUtil.getMinHashSetSize(values.size());
    switch (dataType) {
      case INT: {
        IntSet matchingValues = new IntOpenHashSet(hashSetSize);
        for (String value : values) {
          matchingValues.add(Integer.parseInt(value));
        }
        return new IntRawValueBasedInPredicateEvaluator(matchingValues);
      }
      case LONG: {
        LongSet matchingValues = new LongOpenHashSet(hashSetSize);
        for (String value : values) {
          matchingValues.add(Long.parseLong(value));
        }
        return new LongRawValueBasedInPredicateEvaluator(matchingValues);
      }
      case FLOAT: {
        FloatSet matchingValues = new FloatOpenHashSet(hashSetSize);
        for (String value : values) {
          matchingValues.add(Float.parseFloat(value));
        }
        return new FloatRawValueBasedInPredicateEvaluator(matchingValues);
      }
      case DOUBLE: {
        DoubleSet matchingValues = new DoubleOpenHashSet(hashSetSize);
        for (String value : values) {
          matchingValues.add(Double.parseDouble(value));
        }
        return new DoubleRawValueBasedInPredicateEvaluator(matchingValues);
      }
      case BOOLEAN: {
        IntSet matchingValues = new IntOpenHashSet(hashSetSize);
        for (String value : values) {
          matchingValues.add(BooleanUtils.toInt(value));
        }
        return new IntRawValueBasedInPredicateEvaluator(matchingValues);
      }
      case TIMESTAMP: {
        LongSet matchingValues = new LongOpenHashSet(hashSetSize);
        for (String value : values) {
          matchingValues.add(TimestampUtils.toMillisSinceEpoch(value));
        }
        return new LongRawValueBasedInPredicateEvaluator(matchingValues);
      }
      case STRING: {
        Set<String> matchingValues = new ObjectOpenHashSet<>(hashSetSize);
        matchingValues.addAll(values);
        return new StringRawValueBasedInPredicateEvaluator(matchingValues);
      }
      case BYTES: {
        Set<ByteArray> matchingValues = new ObjectOpenHashSet<>(hashSetSize);
        for (String value : values) {
          matchingValues.add(BytesUtils.toByteArray(value));
        }
        return new BytesRawValueBasedInPredicateEvaluator(matchingValues);
      }
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedInPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final IntSet _matchingDictIdSet;
    final int _numMatchingDictIds;
    int[] _matchingDictIds;

    DictionaryBasedInPredicateEvaluator(InPredicate inPredicate, Dictionary dictionary, DataType dataType) {
      List<String> values = inPredicate.getValues();
      _matchingDictIdSet = new IntOpenHashSet(HashUtil.getMinHashSetSize(values.size()));
      for (String value : values) {
        int dictId = dictionary.indexOf(PredicateUtils.getStoredValue(value, dataType));
        if (dictId >= 0) {
          _matchingDictIdSet.add(dictId);
        }
      }
      _numMatchingDictIds = _matchingDictIdSet.size();
      if (_numMatchingDictIds == 0) {
        _alwaysFalse = true;
      } else if (dictionary.length() == _numMatchingDictIds) {
        _alwaysTrue = true;
      }
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
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

  private static final class IntRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final IntSet _matchingValues;

    IntRawValueBasedInPredicateEvaluator(IntSet matchingValues) {
      _matchingValues = matchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return _matchingValues.contains(value);
    }
  }

  private static final class LongRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final LongSet _matchingValues;

    LongRawValueBasedInPredicateEvaluator(LongSet matchingValues) {
      _matchingValues = matchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      return _matchingValues.contains(value);
    }
  }

  private static final class FloatRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final FloatSet _matchingValues;

    FloatRawValueBasedInPredicateEvaluator(FloatSet matchingValues) {
      _matchingValues = matchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      return _matchingValues.contains(value);
    }
  }

  private static final class DoubleRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final DoubleSet _matchingValues;

    DoubleRawValueBasedInPredicateEvaluator(DoubleSet matchingValues) {
      _matchingValues = matchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      return _matchingValues.contains(value);
    }
  }

  private static final class StringRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final Set<String> _matchingValues;

    StringRawValueBasedInPredicateEvaluator(Set<String> matchingValues) {
      _matchingValues = matchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return _matchingValues.contains(value);
    }
  }

  private static final class BytesRawValueBasedInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final Set<ByteArray> _matchingValues;

    BytesRawValueBasedInPredicateEvaluator(Set<ByteArray> matchingValues) {
      _matchingValues = matchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.BYTES;
    }

    @Override
    public boolean applySV(byte[] value) {
      return _matchingValues.contains(new ByteArray(value));
    }
  }
}
