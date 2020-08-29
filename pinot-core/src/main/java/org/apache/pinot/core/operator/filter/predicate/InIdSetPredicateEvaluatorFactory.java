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

import org.apache.pinot.core.query.request.context.predicate.InIdSetPredicate;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Factory for IN_ID_SET predicate evaluators.
 */
public class InIdSetPredicateEvaluatorFactory {
  private InIdSetPredicateEvaluatorFactory() {
  }

  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(InIdSetPredicate inIdSetPredicate,
      Dictionary dictionary) {
    return new DictionaryBasedInIdSetPredicateEvaluator(inIdSetPredicate, dictionary);
  }

  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(InIdSetPredicate inIdSetPredicate,
      DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntRawValueBasedInIdSetPredicateEvaluator(inIdSetPredicate);
      case LONG:
        return new LongRawValueBasedInIdSetPredicateEvaluator(inIdSetPredicate);
      case FLOAT:
        return new FloatRawValueBasedInIdSetPredicateEvaluator(inIdSetPredicate);
      case DOUBLE:
        return new DoubleRawValueBasedInIdSetPredicateEvaluator(inIdSetPredicate);
      case STRING:
        return new StringRawValueBasedInIdSetPredicateEvaluator(inIdSetPredicate);
      case BYTES:
        return new BytesRawValueBasedInIdSetPredicateEvaluator(inIdSetPredicate);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedInIdSetPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final IdSet _idSet;
    final Dictionary _dictionary;
    final DataType _valueType;

    DictionaryBasedInIdSetPredicateEvaluator(InIdSetPredicate inIdSetPredicate, Dictionary dictionary) {
      _idSet = inIdSetPredicate.getIdSet();
      _dictionary = dictionary;
      _valueType = dictionary.getValueType();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN_ID_SET;
    }

    @Override
    public boolean applySV(int dictId) {
      switch (_valueType) {
        case INT:
          return _idSet.contains(_dictionary.getIntValue(dictId));
        case LONG:
          return _idSet.contains(_dictionary.getLongValue(dictId));
        case FLOAT:
          return _idSet.contains(_dictionary.getFloatValue(dictId));
        case DOUBLE:
          return _idSet.contains(_dictionary.getDoubleValue(dictId));
        case STRING:
          return _idSet.contains(_dictionary.getStringValue(dictId));
        case BYTES:
          return _idSet.contains(_dictionary.getBytesValue(dictId));
        default:
          throw new IllegalStateException();
      }
    }

    @Override
    public int[] getMatchingDictIds() {
      throw new UnsupportedOperationException();
    }
  }

  private static final class IntRawValueBasedInIdSetPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final IdSet _idSet;

    IntRawValueBasedInIdSetPredicateEvaluator(InIdSetPredicate inIdSetPredicate) {
      _idSet = inIdSetPredicate.getIdSet();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN_ID_SET;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return _idSet.contains(value);
    }
  }

  private static final class LongRawValueBasedInIdSetPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final IdSet _idSet;

    LongRawValueBasedInIdSetPredicateEvaluator(InIdSetPredicate inIdSetPredicate) {
      _idSet = inIdSetPredicate.getIdSet();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN_ID_SET;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(long value) {
      return _idSet.contains(value);
    }
  }

  private static final class FloatRawValueBasedInIdSetPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final IdSet _idSet;

    FloatRawValueBasedInIdSetPredicateEvaluator(InIdSetPredicate inIdSetPredicate) {
      _idSet = inIdSetPredicate.getIdSet();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN_ID_SET;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(float value) {
      return _idSet.contains(value);
    }
  }

  private static final class DoubleRawValueBasedInIdSetPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final IdSet _idSet;

    DoubleRawValueBasedInIdSetPredicateEvaluator(InIdSetPredicate inIdSetPredicate) {
      _idSet = inIdSetPredicate.getIdSet();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN_ID_SET;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(double value) {
      return _idSet.contains(value);
    }
  }

  private static final class StringRawValueBasedInIdSetPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final IdSet _idSet;

    StringRawValueBasedInIdSetPredicateEvaluator(InIdSetPredicate inIdSetPredicate) {
      _idSet = inIdSetPredicate.getIdSet();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN_ID_SET;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(String value) {
      return _idSet.contains(value);
    }
  }

  private static final class BytesRawValueBasedInIdSetPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final IdSet _idSet;

    BytesRawValueBasedInIdSetPredicateEvaluator(InIdSetPredicate inIdSetPredicate) {
      _idSet = inIdSetPredicate.getIdSet();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.IN_ID_SET;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(byte[] value) {
      return _idSet.contains(value);
    }
  }
}
