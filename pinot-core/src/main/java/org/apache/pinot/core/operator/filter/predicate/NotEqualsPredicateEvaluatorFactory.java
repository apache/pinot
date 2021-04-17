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

import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;


/**
 * Factory for NEQ predicate evaluators.
 */
public class NotEqualsPredicateEvaluatorFactory {
  private NotEqualsPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based NEQ predicate evaluator.
   *
   * @param notEqPredicate NOT_EQ predicate to evaluate
   * @param dictionary Dictionary for the column
   * @return Dictionary based NOT_EQ predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(NotEqPredicate notEqPredicate,
      Dictionary dictionary) {
    return new DictionaryBasedNeqPredicateEvaluator(notEqPredicate, dictionary);
  }

  /**
   * Create a new instance of raw value based NEQ predicate evaluator.
   *
   * @param notEqPredicate NOT_EQ predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based NOT_EQ predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(NotEqPredicate notEqPredicate,
      DataType dataType) {
    switch (dataType) {
      case INT:
        return new IntRawValueBasedNeqPredicateEvaluator(notEqPredicate);
      case LONG:
        return new LongRawValueBasedNeqPredicateEvaluator(notEqPredicate);
      case FLOAT:
        return new FloatRawValueBasedNeqPredicateEvaluator(notEqPredicate);
      case DOUBLE:
        return new DoubleRawValueBasedNeqPredicateEvaluator(notEqPredicate);
      case STRING:
        return new StringRawValueBasedNeqPredicateEvaluator(notEqPredicate);
      case BYTES:
        return new BytesRawValueBasedNeqPredicateEvaluator(notEqPredicate);
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }

  private static final class DictionaryBasedNeqPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final int _nonMatchingDictId;
    final int[] _nonMatchingDictIds;
    final Dictionary _dictionary;
    int[] _matchingDictIds;

    DictionaryBasedNeqPredicateEvaluator(NotEqPredicate nEqPredicate, Dictionary dictionary) {
      _nonMatchingDictId = dictionary.indexOf(nEqPredicate.getValue());
      if (_nonMatchingDictId >= 0) {
        _nonMatchingDictIds = new int[]{_nonMatchingDictId};
        if (dictionary.length() == 1) {
          _alwaysFalse = true;
        }
      } else {
        _nonMatchingDictIds = new int[0];
        _alwaysTrue = true;
      }
      _dictionary = dictionary;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_EQ;
    }

    @Override
    public boolean applySV(int dictId) {
      return _nonMatchingDictId != dictId;
    }

    @Override
    public int[] getMatchingDictIds() {
      if (_matchingDictIds == null) {
        int dictionarySize = _dictionary.length();
        if (_nonMatchingDictId >= 0) {
          _matchingDictIds = new int[dictionarySize - 1];
          int index = 0;
          for (int dictId = 0; dictId < dictionarySize; dictId++) {
            if (dictId != _nonMatchingDictId) {
              _matchingDictIds[index++] = dictId;
            }
          }
        } else {
          _matchingDictIds = new int[dictionarySize];
          for (int dictId = 0; dictId < dictionarySize; dictId++) {
            _matchingDictIds[dictId] = dictId;
          }
        }
      }
      return _matchingDictIds;
    }

    @Override
    public int[] getNonMatchingDictIds() {
      return _nonMatchingDictIds;
    }
  }

  private static final class IntRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final int _nonMatchingValue;

    IntRawValueBasedNeqPredicateEvaluator(NotEqPredicate nEqPredicate) {
      _nonMatchingValue = Integer.parseInt(nEqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.INT;
    }

    @Override
    public boolean applySV(int value) {
      return _nonMatchingValue != value;
    }
  }

  private static final class LongRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final long _nonMatchingValue;

    LongRawValueBasedNeqPredicateEvaluator(NotEqPredicate nEqPredicate) {
      _nonMatchingValue = Long.parseLong(nEqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.LONG;
    }

    @Override
    public boolean applySV(long value) {
      return _nonMatchingValue != value;
    }
  }

  private static final class FloatRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final float _nonMatchingValue;

    FloatRawValueBasedNeqPredicateEvaluator(NotEqPredicate nEqPredicate) {
      _nonMatchingValue = Float.parseFloat(nEqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean applySV(float value) {
      return _nonMatchingValue != value;
    }
  }

  private static final class DoubleRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final double _nonMatchingValue;

    DoubleRawValueBasedNeqPredicateEvaluator(NotEqPredicate nEqPredicate) {
      _nonMatchingValue = Double.parseDouble(nEqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean applySV(double value) {
      return _nonMatchingValue != value;
    }
  }

  private static final class StringRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final String _nonMatchingValue;

    StringRawValueBasedNeqPredicateEvaluator(NotEqPredicate nEqPredicate) {
      _nonMatchingValue = nEqPredicate.getValue();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return !_nonMatchingValue.equals(value);
    }
  }

  private static final class BytesRawValueBasedNeqPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final byte[] _nonMatchingValue;

    BytesRawValueBasedNeqPredicateEvaluator(NotEqPredicate nEqPredicate) {
      _nonMatchingValue = BytesUtils.toBytes(nEqPredicate.getValue());
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.NOT_EQ;
    }

    @Override
    public DataType getDataType() {
      return DataType.BYTES;
    }

    @Override
    public boolean applySV(byte[] value) {
      return ByteArray.compare(_nonMatchingValue, value) != 0;
    }
  }
}
