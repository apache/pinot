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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.RoaringBitmapWriter;


/**
 * Factory for REGEXP_LIKE predicate evaluators.
 */
public class RegexpLikePredicateEvaluatorFactory {
  private RegexpLikePredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of dictionary based REGEXP_LIKE predicate evaluator.
   *
   * @param regexpLikePredicate REGEXP_LIKE predicate to evaluate
   * @param dictionary Dictionary for the column
   * @param dataType Data type for the column
   * @return Dictionary based REGEXP_LIKE predicate evaluator
   */
  public static BaseDictionaryBasedPredicateEvaluator newDictionaryBasedEvaluator(
      RegexpLikePredicate regexpLikePredicate, Dictionary dictionary, DataType dataType) {
    Preconditions.checkArgument(dataType == DataType.STRING, "Unsupported data type: " + dataType);
    return new DictionaryBasedRegexpLikePredicateEvaluator(regexpLikePredicate, dictionary);
  }

  /**
   * Create a new instance of raw value based REGEXP_LIKE predicate evaluator.
   *
   * @param regexpLikePredicate REGEXP_LIKE predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based REGEXP_LIKE predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(RegexpLikePredicate regexpLikePredicate,
      DataType dataType) {
    Preconditions.checkArgument(dataType == DataType.STRING, "Unsupported data type: " + dataType);
    return new RawValueBasedRegexpLikePredicateEvaluator(regexpLikePredicate);
  }

  private static final class DictionaryBasedRegexpLikePredicateEvaluator extends BaseDictionaryBasedPredicateEvaluator {
    final Pattern _pattern;
    final Dictionary _dictionary;
    RoaringBitmap _matchingDictIdsBitmap;
    int[] _matchingDictIds;

    public DictionaryBasedRegexpLikePredicateEvaluator(RegexpLikePredicate regexpLikePredicate, Dictionary dictionary) {
      super(regexpLikePredicate);
      _dictionary = dictionary;
      _pattern = regexpLikePredicate.getPattern();
    }

    @Override
    public boolean applySV(int dictId) {
      // delay scanning the dictionary until planning is complete
      ensureDictionaryScanned();
      return _matchingDictIdsBitmap.contains(dictId);
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
        ensureDictionaryScanned();
        _matchingDictIds = _matchingDictIdsBitmap.toArray();
      }
      return _matchingDictIds;
    }

    private void ensureDictionaryScanned() {
      if (_matchingDictIdsBitmap == null) {
        RoaringBitmapWriter<RoaringBitmap> writer = RoaringBitmapWriter.writer().runCompress(false).get();
        Matcher matcher = _pattern.matcher("");
        for (int dictId = 0; dictId < _dictionary.length(); dictId++) {
          String value = _dictionary.getStringValue(dictId);
          if (matcher.reset(value).find()) {
            writer.add(dictId);
          }
        }
        _matchingDictIdsBitmap = writer.get();
      }
    }
  }

  private static final class RawValueBasedRegexpLikePredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    // Reuse matcher to avoid excessive allocation. This is safe to do because the evaluator is always used
    // within the scope of a single thread.
    final Matcher _matcher;

    public RawValueBasedRegexpLikePredicateEvaluator(RegexpLikePredicate regexpLikePredicate) {
      super(regexpLikePredicate);
      _matcher = regexpLikePredicate.getPattern().matcher("");
    }

    @Override
    public DataType getDataType() {
      return DataType.STRING;
    }

    @Override
    public boolean applySV(String value) {
      return _matcher.reset(value).find();
    }
  }
}
