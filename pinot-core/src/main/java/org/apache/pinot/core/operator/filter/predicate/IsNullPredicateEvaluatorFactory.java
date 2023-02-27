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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;


public class IsNullPredicateEvaluatorFactory {
  private IsNullPredicateEvaluatorFactory() {
  }
  /**
   * Create a new instance of dictionary based REGEXP_LIKE predicate evaluator.
   *
   * @param dictionary Dictionary for the column
   * @param dataType Data type for the column
   * @return Dictionary based REGEXP_LIKE predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(
      Predicate predicate, Dictionary dictionary, FieldSpec.DataType dataType) {
    return new IsNullPredicateEvaluatorFactory.DictionaryBasedIsNullPredicateEvaluator(predicate, dictionary);
  }

  /**
   * Create a new instance of raw value based IsNull predicate evaluator.
   *
   * @param dataType Data type for the column
   * @return Raw value based REGEXP_LIKE predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(Predicate predicate,
      FieldSpec.DataType dataType) {
    Preconditions.checkArgument(dataType == FieldSpec.DataType.STRING, "Unsupported data type: " + dataType);
    return new IsNullPredicateEvaluatorFactory.RawValueBasedIsNullPredicateEvaluator(predicate);
  }

  private static final class DictionaryBasedIsNullPredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    // Reuse matcher to avoid excessive allocation. This is safe to do because the evaluator is always used
    // within the scope of a single thread.
    final Dictionary _dictionary;
    int[] _matchingDictIds;

    public DictionaryBasedIsNullPredicateEvaluator(Predicate predicate, Dictionary dictionary) {
      super(predicate);
      _dictionary = dictionary;
    }

    @Override
    public boolean applySV(int dictId) {
      return _dictionary.getInternal(dictId) == null;
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
        IntList matchingDictIds = new IntArrayList();
        int dictionarySize = _dictionary.length();
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          if (applySV(dictId)) {
            matchingDictIds.add(dictId);
          }
        }
        _matchingDictIds = matchingDictIds.toIntArray();
      }
      return _matchingDictIds;
    }
  }

  private static final class RawValueBasedIsNullPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    public RawValueBasedIsNullPredicateEvaluator(Predicate predicate) {
      super(predicate);
    }

    @Override
    public FieldSpec.DataType getDataType() {
      return FieldSpec.DataType.INT;
    }

    @Override
    public boolean applySV(String value) {
      return value == null;
    }
  }
}
