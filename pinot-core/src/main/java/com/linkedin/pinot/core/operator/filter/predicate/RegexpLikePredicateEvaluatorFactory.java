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

import java.util.regex.Pattern;

import com.linkedin.pinot.core.common.predicate.RegexpLikePredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

public class RegexpLikePredicateEvaluatorFactory {

  private RegexpLikePredicateEvaluatorFactory() {

  }

  public static PredicateEvaluator newDictionaryBasedEvaluator(RegexpLikePredicate predicate, Dictionary dictionary) {
    return new DictionaryBasedRegexPredicateEvaluator(predicate, dictionary);
  }

  public static PredicateEvaluator newNoDictionaryBasedEvaluator(RegexpLikePredicate predicate) {
    return new NoDictionaryBasedRegexPredicateEvaluator(predicate);
  }

  private static class DictionaryBasedRegexPredicateEvaluator extends BasePredicateEvaluator {
    private Dictionary dictionary;
    private Pattern pattern;

    public DictionaryBasedRegexPredicateEvaluator(RegexpLikePredicate predicate, Dictionary dictionary) {
      this.dictionary = dictionary;
      int flags = Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE;
      pattern = Pattern.compile(predicate.getRegex(), flags);
    }

    @Override
    public boolean apply(int dictionaryId) {
      String value = dictionary.getStringValue(dictionaryId);
      return pattern.matcher(value).find();
    }

    @Override
    public boolean apply(int[] dictionaryIds) {

      for (int dictId : dictionaryIds) {
        String value = dictionary.getStringValue(dictId);
        if (pattern.matcher(value).find()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean apply(int[] dictionaryIds, int length) {
      for (int i = 0; i < length; i++) {
        int dictId = dictionaryIds[i];
        String value = dictionary.getStringValue(dictId);
        if (pattern.matcher(value).find()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean alwaysFalse() {
      return false;
    }

  }

  private static class NoDictionaryBasedRegexPredicateEvaluator extends BasePredicateEvaluator {

    private Pattern pattern;

    public NoDictionaryBasedRegexPredicateEvaluator(RegexpLikePredicate predicate) {
      int flags = Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE;
      pattern = Pattern.compile(predicate.getRegex(), flags);
    }

    @Override
    public boolean apply(String value) {
      return pattern.matcher(value).find();
    }

    @Override
    public boolean apply(String[] values) {
      return apply(values, values.length);
    }

    @Override
    public boolean apply(String[] values, int length) {
      for (int i = 0; i < length; i++) {
        String value = values[i];
        if (pattern.matcher(value).find()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean apply(long value) {
      return apply(String.valueOf(value));
    }

    @Override
    public boolean apply(long[] values) {
      return apply(values, values.length);
    }

    @Override
    public boolean apply(long[] values, int length) {
      for (int i = 0; i < length; i++) {
        long value = values[i];
        if (apply(value)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean apply(float value) {
      return apply(String.valueOf(value));
    }

    @Override
    public boolean apply(float[] values) {
      return apply(values, values.length);
    }

    @Override
    public boolean apply(float[] values, int length) {
      for (int i = 0; i < length; i++) {
        float value = values[i];
        if (apply(value)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean apply(double value) {
      return apply(String.valueOf(value));
    }

    @Override
    public boolean apply(double[] values) {
      return apply(values, values.length);
    }

    @Override
    public boolean apply(double[] values, int length) {
      for (int i = 0; i < length; i++) {
        double value = values[i];
        if (apply(value)) {
          return true;
        }
      }
      return false;
    }
  }
}
