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

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Predicate;
import com.linkedin.pinot.core.common.predicate.EqPredicate;
import com.linkedin.pinot.core.common.predicate.InPredicate;
import com.linkedin.pinot.core.common.predicate.NEqPredicate;
import com.linkedin.pinot.core.common.predicate.NotInPredicate;
import com.linkedin.pinot.core.common.predicate.RangePredicate;
import com.linkedin.pinot.core.common.predicate.RegexpLikePredicate;
import com.linkedin.pinot.core.realtime.impl.dictionary.MutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;

public class PredicateEvaluatorProvider {

  public static PredicateEvaluator getPredicateFunctionFor(final Predicate predicate, DataSource dataSource) {
    Dictionary dictionary = dataSource.getDictionary();
    if (dataSource.getDataSourceMetadata().hasDictionary()) {
      switch (predicate.getType()) {
      case EQ:
        return EqualsPredicateEvaluator.newDictionaryBasedEvaluator((EqPredicate) predicate, dictionary);
      case NEQ:
        return new NotEqualsPredicateEvaluator((NEqPredicate) predicate, dictionary);
      case IN:
        return new InPredicateEvaluator((InPredicate) predicate, dictionary);
      case NOT_IN:
        return new NotInPredicateEvaluator((NotInPredicate) predicate, dictionary);
      case RANGE:
        if (dictionary instanceof ImmutableDictionaryReader) {
          return new RangeOfflineDictionaryPredicateEvaluator((RangePredicate) predicate, (ImmutableDictionaryReader) dictionary);
        } else {
          return new RangeRealtimeDictionaryPredicateEvaluator((RangePredicate) predicate, (MutableDictionaryReader) dictionary);
        }
      case REGEX:
        throw new UnsupportedOperationException("regex is not supported");
      default:
        throw new UnsupportedOperationException("UnKnown predicate type");
      }
    } else {
      DataType dataType = dataSource.getDataSourceMetadata().getDataType();

      switch (predicate.getType()) {
      case EQ:
        return EqualsPredicateEvaluator.newNoDictionaryBasedEvaluator((EqPredicate) predicate, dataType);
      case NEQ:
        return new NotEqualsPredicateEvaluator((NEqPredicate) predicate, dictionary);
      case IN:
        return new InPredicateEvaluator((InPredicate) predicate, dictionary);
      case NOT_IN:
        return new NotInPredicateEvaluator((NotInPredicate) predicate, dictionary);
      case RANGE:
        if (dictionary instanceof ImmutableDictionaryReader) {
          return new RangeOfflineDictionaryPredicateEvaluator((RangePredicate) predicate, (ImmutableDictionaryReader) dictionary);
        } else {
          return new RangeRealtimeDictionaryPredicateEvaluator((RangePredicate) predicate, (MutableDictionaryReader) dictionary);
        }
      case REGEXP_LIKE:
        return new RegexPredicateEvaluator((RegexpLikePredicate) predicate, dictionary);
      default:
        throw new UnsupportedOperationException("UnKnown predicate type");
      }
    }
  }
}
