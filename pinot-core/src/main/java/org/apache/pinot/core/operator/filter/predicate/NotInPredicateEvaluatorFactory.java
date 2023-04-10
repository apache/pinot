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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MultiValueVisitor;
import org.apache.pinot.spi.utils.ByteArray;


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
   * @param dictionary     Dictionary for the column
   * @param dataType       Data type for the column
   * @param queryContext   Query context
   * @return Dictionary based NOT_IN predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(NotInPredicate notInPredicate,
      Dictionary dictionary, DataType dataType, @Nullable QueryContext queryContext) {
    return new DictionaryBasedNotInPredicateEvaluator(notInPredicate, dictionary, dataType, queryContext);
  }

  /**
   * Create a new instance of raw value based NOT_IN predicate evaluator.
   *
   * @param notInPredicate NOT_IN predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based NOT_IN predicate evaluator
   */
  public static NotInRawPredicateEvaluator newRawValueBasedEvaluator(NotInPredicate notInPredicate,
      DataType dataType) {
    switch (dataType) {
      case INT: {
        int[] intValues = notInPredicate.getIntValues();
        IntSet nonMatchingValues = new IntOpenHashSet(HashUtil.getMinHashSetSize(intValues.length));
        for (int value : intValues) {
          nonMatchingValues.add(value);
        }
        return new IntRawValueBasedNotInPredicateEvaluator(notInPredicate, nonMatchingValues);
      }
      case LONG: {
        long[] longValues = notInPredicate.getLongValues();
        LongSet nonMatchingValues = new LongOpenHashSet(HashUtil.getMinHashSetSize(longValues.length));
        for (long value : longValues) {
          nonMatchingValues.add(value);
        }
        return new LongRawValueBasedNotInPredicateEvaluator(notInPredicate, nonMatchingValues);
      }
      case FLOAT: {
        float[] floatValues = notInPredicate.getFloatValues();
        FloatSet nonMatchingValues = new FloatOpenHashSet(HashUtil.getMinHashSetSize(floatValues.length));
        for (float value : floatValues) {
          nonMatchingValues.add(value);
        }
        return new FloatRawValueBasedNotInPredicateEvaluator(notInPredicate, nonMatchingValues);
      }
      case DOUBLE: {
        double[] doubleValues = notInPredicate.getDoubleValues();
        DoubleSet nonMatchingValues = new DoubleOpenHashSet(HashUtil.getMinHashSetSize(doubleValues.length));
        for (double value : doubleValues) {
          nonMatchingValues.add(value);
        }
        return new DoubleRawValueBasedNotInPredicateEvaluator(notInPredicate, nonMatchingValues);
      }
      case BIG_DECIMAL: {
        BigDecimal[] bigDecimalValues = notInPredicate.getBigDecimalValues();
        // NOTE: Use TreeSet because BigDecimal's compareTo() is not consistent with equals()
        //       E.g. compareTo(3.0, 3) returns 0 but equals(3.0, 3) returns false
        TreeSet<BigDecimal> nonMatchingValues = new TreeSet<>(Arrays.asList(bigDecimalValues));
        return new BigDecimalRawValueBasedNotInPredicateEvaluator(notInPredicate, nonMatchingValues);
      }
      case BOOLEAN: {
        int[] booleanValues = notInPredicate.getBooleanValues();
        IntSet nonMatchingValues = new IntOpenHashSet(HashUtil.getMinHashSetSize(booleanValues.length));
        for (int value : booleanValues) {
          nonMatchingValues.add(value);
        }
        return new IntRawValueBasedNotInPredicateEvaluator(notInPredicate, nonMatchingValues);
      }
      case TIMESTAMP: {
        long[] timestampValues = notInPredicate.getTimestampValues();
        LongSet nonMatchingValues = new LongOpenHashSet(HashUtil.getMinHashSetSize(timestampValues.length));
        for (long value : timestampValues) {
          nonMatchingValues.add(value);
        }
        return new LongRawValueBasedNotInPredicateEvaluator(notInPredicate, nonMatchingValues);
      }
      case STRING: {
        List<String> stringValues = notInPredicate.getValues();
        Set<String> nonMatchingValues = new ObjectOpenHashSet<>(HashUtil.getMinHashSetSize(stringValues.size()));
        // NOTE: Add value-by-value to avoid overhead
        for (String value : stringValues) {
          //noinspection UseBulkOperation
          nonMatchingValues.add(value);
        }
        return new StringRawValueBasedNotInPredicateEvaluator(notInPredicate, nonMatchingValues);
      }
      case BYTES: {
        ByteArray[] bytesValues = notInPredicate.getBytesValues();
        Set<ByteArray> nonMatchingValues = new ObjectOpenHashSet<>(HashUtil.getMinHashSetSize(bytesValues.length));
        // NOTE: Add value-by-value to avoid overhead
        //noinspection ManualArrayToCollectionCopy
        for (ByteArray value : bytesValues) {
          //noinspection UseBulkOperation
          nonMatchingValues.add(value);
        }
        return new BytesRawValueBasedNotInPredicateEvaluator(notInPredicate, nonMatchingValues);
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

    DictionaryBasedNotInPredicateEvaluator(NotInPredicate notInPredicate, Dictionary dictionary, DataType dataType,
        @Nullable QueryContext queryContext) {
      super(notInPredicate);
      _nonMatchingDictIdSet = PredicateUtils.getDictIdSet(notInPredicate, dictionary, dataType, queryContext);
      _numNonMatchingDictIds = _nonMatchingDictIdSet.size();
      if (_numNonMatchingDictIds == 0) {
        _alwaysTrue = true;
      } else if (dictionary.length() == _numNonMatchingDictIds) {
        _alwaysFalse = true;
      }
      _dictionary = dictionary;
    }

    @Override
    public int getNumMatchingItems() {
      return -_numNonMatchingDictIds;
    }

    @Override
    public boolean applySV(int dictId) {
      return !_nonMatchingDictIdSet.contains(dictId);
    }

    @Override
    public int applySV(int limit, int[] docIds, int[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        int value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
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

  public static abstract class NotInRawPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    public NotInRawPredicateEvaluator(Predicate predicate) {
      super(predicate);
    }

    /**
     * Visits the not matching value of this predicate.
     */
    public abstract <R> R accept(MultiValueVisitor<R> visitor);
  }

  private static final class IntRawValueBasedNotInPredicateEvaluator extends NotInRawPredicateEvaluator {
    final IntSet _nonMatchingValues;

    IntRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate, IntSet nonMatchingValues) {
      super(notInPredicate);
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return -_nonMatchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return !_nonMatchingValues.contains(value);
    }

    @Override
    public int applySV(int limit, int[] docIds, int[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        int value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return visitor.visitInt(_nonMatchingValues.toIntArray());
    }
  }

  private static final class LongRawValueBasedNotInPredicateEvaluator extends NotInRawPredicateEvaluator {
    final LongSet _nonMatchingValues;

    LongRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate, LongSet nonMatchingValues) {
      super(notInPredicate);
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return -_nonMatchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      return !_nonMatchingValues.contains(value);
    }

    @Override
    public int applySV(int limit, int[] docIds, long[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        long value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return visitor.visitLong(_nonMatchingValues.toLongArray());
    }
  }

  private static final class FloatRawValueBasedNotInPredicateEvaluator extends NotInRawPredicateEvaluator {
    final FloatSet _nonMatchingValues;

    FloatRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate, FloatSet nonMatchingValues) {
      super(notInPredicate);
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return -_nonMatchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      return !_nonMatchingValues.contains(value);
    }

    @Override
    public int applySV(int limit, int[] docIds, float[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        float value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return visitor.visitFloat(_nonMatchingValues.toFloatArray());
    }
  }

  private static final class DoubleRawValueBasedNotInPredicateEvaluator extends NotInRawPredicateEvaluator {
    final DoubleSet _nonMatchingValues;

    DoubleRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate, DoubleSet nonMatchingValues) {
      super(notInPredicate);
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return -_nonMatchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      return !_nonMatchingValues.contains(value);
    }

    @Override
    public int applySV(int limit, int[] docIds, double[] values) {
      // reimplemented here to ensure applySV can be inlined
      int matches = 0;
      for (int i = 0; i < limit; i++) {
        double value = values[i];
        if (applySV(value)) {
          docIds[matches++] = docIds[i];
        }
      }
      return matches;
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return visitor.visitDouble(_nonMatchingValues.toDoubleArray());
    }
  }

  private static final class BigDecimalRawValueBasedNotInPredicateEvaluator extends NotInRawPredicateEvaluator {
    // See: BigDecimalRawValueBasedInPredicateEvaluator.
    final TreeSet<BigDecimal> _nonMatchingValues;

    BigDecimalRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate,
        TreeSet<BigDecimal> nonMatchingValues) {
      super(notInPredicate);
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return -_nonMatchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.BIG_DECIMAL;
    }

    @Override
    public boolean applySV(BigDecimal value) {
      return !_nonMatchingValues.contains(value);
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return visitor.visitBigDecimal(_nonMatchingValues.toArray(new BigDecimal[0]));
    }
  }

  private static final class StringRawValueBasedNotInPredicateEvaluator extends NotInRawPredicateEvaluator {
    final Set<String> _nonMatchingValues;

    StringRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate, Set<String> nonMatchingValues) {
      super(notInPredicate);
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return -_nonMatchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return !_nonMatchingValues.contains(value);
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return visitor.visitString(_nonMatchingValues.toArray(new String[0]));
    }
  }

  private static final class BytesRawValueBasedNotInPredicateEvaluator extends NotInRawPredicateEvaluator {
    final Set<ByteArray> _nonMatchingValues;

    BytesRawValueBasedNotInPredicateEvaluator(NotInPredicate notInPredicate, Set<ByteArray> nonMatchingValues) {
      super(notInPredicate);
      _nonMatchingValues = nonMatchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return -_nonMatchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.BYTES;
    }

    @Override
    public boolean applySV(byte[] value) {
      return !_nonMatchingValues.contains(new ByteArray(value));
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      byte[][] bytes = _nonMatchingValues.stream()
          .map(ByteArray::getBytes)
          .toArray(byte[][]::new);
      return visitor.visitBytes(bytes);
    }
  }
}
