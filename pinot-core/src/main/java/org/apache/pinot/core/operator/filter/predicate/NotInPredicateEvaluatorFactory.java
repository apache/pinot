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
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.BooleanUtils;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.TimestampUtils;


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
   * @param dataType Data type for the column
   * @return Dictionary based NOT_IN predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(NotInPredicate notInPredicate,
      Dictionary dictionary, DataType dataType) {
    return new DictionaryBasedNotInPredicateEvaluator(notInPredicate, dictionary, dataType);
  }

  /**
   * Create a new instance of raw value based NOT_IN predicate evaluator.
   *
   * @param notInPredicate NOT_IN predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based NOT_IN predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(NotInPredicate notInPredicate,
      DataType dataType) {
    List<String> values = notInPredicate.getValues();
    int hashSetSize = HashUtil.getMinHashSetSize(values.size());
    switch (dataType) {
      case INT: {
        IntSet nonMatchingValues = new IntOpenHashSet(hashSetSize);
        for (String value : values) {
          nonMatchingValues.add(Integer.parseInt(value));
        }
        return new IntRawValueBasedNotInPredicateEvaluator(nonMatchingValues);
      }
      case LONG: {
        LongSet nonMatchingValues = new LongOpenHashSet(hashSetSize);
        for (String value : values) {
          nonMatchingValues.add(Long.parseLong(value));
        }
        return new LongRawValueBasedNotInPredicateEvaluator(nonMatchingValues);
      }
      case FLOAT: {
        FloatSet nonMatchingValues = new FloatOpenHashSet(hashSetSize);
        for (String value : values) {
          nonMatchingValues.add(Float.parseFloat(value));
        }
        return new FloatRawValueBasedNotInPredicateEvaluator(nonMatchingValues);
      }
      case DOUBLE: {
        DoubleSet nonMatchingValues = new DoubleOpenHashSet(hashSetSize);
        for (String value : values) {
          nonMatchingValues.add(Double.parseDouble(value));
        }
        return new DoubleRawValueBasedNotInPredicateEvaluator(nonMatchingValues);
      }
      case BOOLEAN: {
        IntSet nonMatchingValues = new IntOpenHashSet(hashSetSize);
        for (String value : values) {
          nonMatchingValues.add(BooleanUtils.toInt(value));
        }
        return new IntRawValueBasedNotInPredicateEvaluator(nonMatchingValues);
      }
      case TIMESTAMP: {
        LongSet nonMatchingValues = new LongOpenHashSet(hashSetSize);
        for (String value : values) {
          nonMatchingValues.add(TimestampUtils.toMillisSinceEpoch(value));
        }
        return new LongRawValueBasedNotInPredicateEvaluator(nonMatchingValues);
      }
      case STRING: {
        Set<String> nonMatchingValues = new ObjectOpenHashSet<>(hashSetSize);
        nonMatchingValues.addAll(values);
        return new StringRawValueBasedNotInPredicateEvaluator(nonMatchingValues);
      }
      case BYTES: {
        Set<ByteArray> nonMatchingValues = new ObjectOpenHashSet<>(hashSetSize);
        for (String value : values) {
          nonMatchingValues.add(BytesUtils.toByteArray(value));
        }
        return new BytesRawValueBasedNotInPredicateEvaluator(nonMatchingValues);
      }
      case BIGDECIMAL: {
        Set<BigDecimal> nonMatchingValues = new ObjectOpenHashSet<>(hashSetSize);
        for (String value : values) {
          nonMatchingValues.add(BigDecimalUtils.toBigDecimal(value));
        }
        return new BigDecimalRawValueBasedNotInPredicateEvaluator(nonMatchingValues);
      }
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  public static final class DictionaryBasedNotInPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final IntSet _nonMatchingDictIdSet;
    final int _numNonMatchingDictIds;
    final Dictionary _dictionary;
    int[] _matchingDictIds;
    int[] _nonMatchingDictIds;

    DictionaryBasedNotInPredicateEvaluator(NotInPredicate notInPredicate, Dictionary dictionary, DataType dataType) {
      List<String> values = notInPredicate.getValues();
      _nonMatchingDictIdSet = new IntOpenHashSet(HashUtil.getMinHashSetSize(values.size()));
      for (String value : values) {
        int dictId = dictionary.indexOf(PredicateUtils.getStoredValue(value, dataType));
        if (dictId >= 0) {
          _nonMatchingDictIdSet.add(dictId);
        }
      }
      _numNonMatchingDictIds = _nonMatchingDictIdSet.size();
      if (_numNonMatchingDictIds == 0) {
        _alwaysTrue = true;
      } else if (dictionary.length() == _numNonMatchingDictIds) {
        _alwaysFalse = true;
      }
      _dictionary = dictionary;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public boolean applySV(int dictId) {
      return !_nonMatchingDictIdSet.contains(dictId);
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        int dictionarySize = _dictionary.length();
        _matchingDictIds = new int[dictionarySize - _numNonMatchingDictIds];
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
    public int getNumNonMatchingDictIds() {
      return _numNonMatchingDictIds;
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

    IntRawValueBasedNotInPredicateEvaluator(IntSet nonMatchingValues) {
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return !_nonMatchingValues.contains(value);
    }
  }

  private static final class LongRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final LongSet _nonMatchingValues;

    LongRawValueBasedNotInPredicateEvaluator(LongSet nonMatchingValues) {
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      return !_nonMatchingValues.contains(value);
    }
  }

  private static final class FloatRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final FloatSet _nonMatchingValues;

    FloatRawValueBasedNotInPredicateEvaluator(FloatSet nonMatchingValues) {
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      return !_nonMatchingValues.contains(value);
    }
  }

  private static final class DoubleRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final DoubleSet _nonMatchingValues;

    DoubleRawValueBasedNotInPredicateEvaluator(DoubleSet nonMatchingValues) {
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      return !_nonMatchingValues.contains(value);
    }
  }

  private static final class StringRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final Set<String> _nonMatchingValues;

    StringRawValueBasedNotInPredicateEvaluator(Set<String> nonMatchingValues) {
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return !_nonMatchingValues.contains(value);
    }
  }

  private static final class BytesRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final Set<ByteArray> _nonMatchingValues;

    BytesRawValueBasedNotInPredicateEvaluator(Set<ByteArray> nonMatchingValues) {
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.BYTES;
    }

    @Override
    public boolean applySV(byte[] value) {
      return !_nonMatchingValues.contains(new ByteArray(value));
    }
  }

  private static final class BigDecimalRawValueBasedNotInPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final Set<BigDecimal> _nonMatchingValues;

    BigDecimalRawValueBasedNotInPredicateEvaluator(Set<BigDecimal> nonMatchingValues) {
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_IN;
    }

    @Override
    public DataType getDataType() {
      return DataType.BIGDECIMAL;
    }

    @Override
    public boolean applySV(BigDecimal value) {
      return !_nonMatchingValues.contains(value);
    }
  }
}
