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

import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.RegexPredicate;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;

public class RegexPredicateEvaluator implements PredicateEvaluator {

  private RegexPredicate predicate;
  private Dictionary dictionary;
  private Pattern pattern;

  public RegexPredicateEvaluator(RegexPredicate predicate, Dictionary dictionary) {
    this.predicate = predicate;
    this.dictionary = dictionary;
    // use lowercase to make it case insensitive
    pattern = Pattern.compile(predicate.getRegex().toLowerCase());
  }

  @Override
  public boolean apply(int dictionaryId) {
    String value = dictionary.getStringValue(dictionaryId);
    return pattern.matcher(value).matches();
  }

  @Override
  public boolean apply(int[] dictionaryIds) {

    for (int dictId : dictionaryIds) {
      // make it lowercase to make it case insensitive, pattern regex is already converted to lowercase in the constructor
      String value = dictionary.getStringValue(dictId).toLowerCase();
      if (pattern.matcher(value).matches()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int[] getMatchingDictionaryIds() {
    throw new UnsupportedOperationException("Returning matching values is expensive for predicateType:" + predicate.getType());
  }

  @Override
  public int[] getNonMatchingDictionaryIds() {
    throw new UnsupportedOperationException("Returning non matching values is expensive for predicateType:" + predicate.getType());
  }

  @Override
  public boolean apply(int[] dictionaryIds, int length) {
    for (int i = 0; i < length; i++) {
      int dictId = dictionaryIds[i];
      String value = dictionary.getStringValue(dictId);
      if (pattern.matcher(value).matches()) {
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
