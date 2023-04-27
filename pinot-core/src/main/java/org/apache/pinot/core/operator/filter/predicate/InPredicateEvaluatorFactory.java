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
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MultiValueVisitor;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Factory for IN predicate evaluators.
 */
public class InPredicateEvaluatorFactory {
  private InPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based IN predicate evaluator.
   *
   * @param inPredicate  IN predicate to evaluate
   * @param dictionary   Dictionary for the column
   * @param dataType     Data type for the column
   * @param queryContext Query context
   * @return Dictionary based IN predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(InPredicate inPredicate,
      Dictionary dictionary, DataType dataType, @Nullable QueryContext queryContext) {
    return new DictionaryBasedInPredicateEvaluator(inPredicate, dictionary, dataType, queryContext);
  }

  /**
   * Create a new instance of raw value based IN predicate evaluator.
   *
   * @param inPredicate IN predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based IN predicate evaluator
   */
  public static InRawPredicateEvaluator newRawValueBasedEvaluator(InPredicate inPredicate,
      DataType dataType) {
    switch (dataType) {
      case INT: {
        int[] intValues = inPredicate.getIntValues();
        IntSet matchingValues = new IntOpenHashSet(HashUtil.getMinHashSetSize(intValues.length));
        for (int value : intValues) {
          matchingValues.add(value);
        }
        return new IntRawValueBasedInPredicateEvaluator(inPredicate, matchingValues);
      }
      case LONG: {
        long[] longValues = inPredicate.getLongValues();
        LongSet matchingValues = new LongOpenHashSet(HashUtil.getMinHashSetSize(longValues.length));
        for (long value : longValues) {
          matchingValues.add(value);
        }
        return new LongRawValueBasedInPredicateEvaluator(inPredicate, matchingValues);
      }
      case FLOAT: {
        float[] floatValues = inPredicate.getFloatValues();
        FloatSet matchingValues = new FloatOpenHashSet(HashUtil.getMinHashSetSize(floatValues.length));
        for (float value : floatValues) {
          matchingValues.add(value);
        }
        return new FloatRawValueBasedInPredicateEvaluator(inPredicate, matchingValues);
      }
      case DOUBLE: {
        double[] doubleValues = inPredicate.getDoubleValues();
        DoubleSet matchingValues = new DoubleOpenHashSet(HashUtil.getMinHashSetSize(doubleValues.length));
        for (double value : doubleValues) {
          matchingValues.add(value);
        }
        return new DoubleRawValueBasedInPredicateEvaluator(inPredicate, matchingValues);
      }
      case BIG_DECIMAL: {
        BigDecimal[] bigDecimalValues = inPredicate.getBigDecimalValues();
        // NOTE: Use TreeSet because BigDecimal's compareTo() is not consistent with equals()
        //       E.g. compareTo(3.0, 3) returns 0 but equals(3.0, 3) returns false
        TreeSet<BigDecimal> matchingValues = new TreeSet<>(Arrays.asList(bigDecimalValues));
        return new BigDecimalRawValueBasedInPredicateEvaluator(inPredicate, matchingValues);
      }
      case BOOLEAN: {
        int[] booleanValues = inPredicate.getBooleanValues();
        IntSet matchingValues = new IntOpenHashSet(HashUtil.getMinHashSetSize(booleanValues.length));
        for (int value : booleanValues) {
          matchingValues.add(value);
        }
        return new IntRawValueBasedInPredicateEvaluator(inPredicate, matchingValues);
      }
      case TIMESTAMP: {
        long[] timestampValues = inPredicate.getTimestampValues();
        LongSet matchingValues = new LongOpenHashSet(HashUtil.getMinHashSetSize(timestampValues.length));
        for (long value : timestampValues) {
          matchingValues.add(value);
        }
        return new LongRawValueBasedInPredicateEvaluator(inPredicate, matchingValues);
      }
      case STRING: {
        List<String> stringValues = inPredicate.getValues();
        Set<String> matchingValues = new ObjectOpenHashSet<>(HashUtil.getMinHashSetSize(stringValues.size()));
        // NOTE: Add value-by-value to avoid overhead
        for (String value : stringValues) {
          //noinspection UseBulkOperation
          matchingValues.add(value);
        }
        return new StringRawValueBasedInPredicateEvaluator(inPredicate, matchingValues);
      }
      case BYTES: {
        ByteArray[] bytesValues = inPredicate.getBytesValues();
        Set<ByteArray> matchingValues = new ObjectOpenHashSet<>(HashUtil.getMinHashSetSize(bytesValues.length));
        // NOTE: Add value-by-value to avoid overhead
        //noinspection ManualArrayToCollectionCopy
        for (ByteArray value : bytesValues) {
          //noinspection UseBulkOperation
          matchingValues.add(value);
        }
        return new BytesRawValueBasedInPredicateEvaluator(inPredicate, matchingValues);
      }
      default:
        throw new IllegalStateException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedInPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final IntSet _matchingDictIdSet;
    final int _numMatchingDictIds;
    int[] _matchingDictIds;

    DictionaryBasedInPredicateEvaluator(InPredicate inPredicate, Dictionary dictionary, DataType dataType,
        @Nullable QueryContext queryContext) {
      super(inPredicate);
      _matchingDictIdSet = PredicateUtils.getDictIdSet(inPredicate, dictionary, dataType, queryContext);
      _numMatchingDictIds = _matchingDictIdSet.size();
      if (_numMatchingDictIds == 0) {
        _alwaysFalse = true;
      } else if (dictionary.length() == _numMatchingDictIds) {
        _alwaysTrue = true;
      }
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
    public int getNumMatchingItems() {
      return getNumMatchingDictIds();
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        _matchingDictIds = _matchingDictIdSet.toIntArray();
      }
      return _matchingDictIds;
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
  }

  public static abstract class InRawPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    public InRawPredicateEvaluator(Predicate predicate) {
      super(predicate);
    }

    /**
     * Visits the matching value of this predicate.
     */
    public abstract <R> R accept(MultiValueVisitor<R> visitor);
  }

  private static final class IntRawValueBasedInPredicateEvaluator extends InRawPredicateEvaluator {
    final IntSet _matchingValues;

    IntRawValueBasedInPredicateEvaluator(InPredicate inPredicate, IntSet matchingValues) {
      super(inPredicate);
      _matchingValues = matchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return _matchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return _matchingValues.contains(value);
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
      return visitor.visitInt(_matchingValues.toIntArray());
    }
  }

  private static final class LongRawValueBasedInPredicateEvaluator extends InRawPredicateEvaluator {
    final LongSet _matchingValues;

    LongRawValueBasedInPredicateEvaluator(InPredicate inPredicate, LongSet matchingValues) {
      super(inPredicate);
      _matchingValues = matchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return _matchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      return _matchingValues.contains(value);
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
      return visitor.visitLong(_matchingValues.toLongArray());
    }
  }

  private static final class FloatRawValueBasedInPredicateEvaluator extends InRawPredicateEvaluator {
    final FloatSet _matchingValues;

    FloatRawValueBasedInPredicateEvaluator(InPredicate inPredicate, FloatSet matchingValues) {
      super(inPredicate);
      _matchingValues = matchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return _matchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      return _matchingValues.contains(value);
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
      return visitor.visitFloat(_matchingValues.toFloatArray());
    }
  }

  private static final class DoubleRawValueBasedInPredicateEvaluator extends InRawPredicateEvaluator {
    final DoubleSet _matchingValues;

    DoubleRawValueBasedInPredicateEvaluator(InPredicate inPredicate, DoubleSet matchingValues) {
      super(inPredicate);
      _matchingValues = matchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return _matchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      return _matchingValues.contains(value);
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
      return visitor.visitDouble(_matchingValues.toDoubleArray());
    }
  }

  private static final class BigDecimalRawValueBasedInPredicateEvaluator extends InRawPredicateEvaluator {
    // Note: BigDecimal's compareTo is not consistent with equals (e.g. compareTo(3.0, 3) returns zero when
    //   equals(3.0, 3) returns false).
    // - HashSet implementation consider both hashCode() and equals() for the key.
    // - TreeSet implementation on the other hand decides equality based on compareTo() method, and leaves it to
    //    the end user to ensure that maintained ordering is consistent with equals if it is to correctly implement
    //    the Set interface.
    final TreeSet<BigDecimal> _matchingValues;

    BigDecimalRawValueBasedInPredicateEvaluator(InPredicate inPredicate, TreeSet<BigDecimal> matchingValues) {
      super(inPredicate);
      _matchingValues = matchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return _matchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.BIG_DECIMAL;
    }

    @Override
    public boolean applySV(BigDecimal value) {
      return _matchingValues.contains(value);
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return visitor.visitBigDecimal(_matchingValues.toArray(new BigDecimal[0]));
    }
  }

  private static final class StringRawValueBasedInPredicateEvaluator extends InRawPredicateEvaluator {
    final Set<String> _matchingValues;

    StringRawValueBasedInPredicateEvaluator(InPredicate inPredicate, Set<String> matchingValues) {
      super(inPredicate);
      _matchingValues = matchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return _matchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return _matchingValues.contains(value);
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      return visitor.visitString(_matchingValues.toArray(new String[0]));
    }
  }

  private static final class BytesRawValueBasedInPredicateEvaluator extends InRawPredicateEvaluator {
    final Set<ByteArray> _matchingValues;

    BytesRawValueBasedInPredicateEvaluator(InPredicate inPredicate, Set<ByteArray> matchingValues) {
      super(inPredicate);
      _matchingValues = matchingValues;
    }

    @Override
    public int getNumMatchingItems() {
      return _matchingValues.size();
    }

    @Override
    public DataType getDataType() {
      return DataType.BYTES;
    }

    @Override
    public boolean applySV(byte[] value) {
      return _matchingValues.contains(new ByteArray(value));
    }

    @Override
    public <R> R accept(MultiValueVisitor<R> visitor) {
      byte[][] bytes = _matchingValues.stream()
          .map(ByteArray::getBytes)
          .toArray(byte[][]::new);
      return visitor.visitBytes(bytes);
    }
  }
}
