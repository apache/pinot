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
import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;


public class PredicateEvaluatorProvider {
  private PredicateEvaluatorProvider() {
  }

  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, DataSource dataSource) {
    try {
      if (dataSource.getDataSourceMetadata().hasDictionary()) {
        Dictionary dictionary = dataSource.getDictionary();
        switch (predicate.getType()) {
          case EQ:
            return EqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator((EqPredicate) predicate, dictionary);
          case NEQ:
            return NotEqualsPredicateEvaluatorFactory.newDictionaryBasedEvaluator((NEqPredicate) predicate, dictionary);
          case IN:
            return InPredicateEvaluatorFactory.newDictionaryBasedEvaluator((InPredicate) predicate, dictionary);
          case NOT_IN:
            return NotInPredicateEvaluatorFactory.newDictionaryBasedEvaluator((NotInPredicate) predicate, dictionary);
          case RANGE:
            return RangePredicateEvaluatorFactory.newDictionaryBasedEvaluator((RangePredicate) predicate, dictionary);
          case REGEXP_LIKE:
            return RegexpLikePredicateEvaluatorFactory.newDictionaryBasedEvaluator((RegexpLikePredicate) predicate,
                dictionary);
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      } else {
        DataType dataType = dataSource.getDataSourceMetadata().getDataType();
        switch (predicate.getType()) {
          case EQ:
            return EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((EqPredicate) predicate, dataType);
          case NEQ:
            return NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((NEqPredicate) predicate, dataType);
          case IN:
            return InPredicateEvaluatorFactory.newRawValueBasedEvaluator((InPredicate) predicate, dataType);
          case NOT_IN:
            return NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator((NotInPredicate) predicate, dataType);
          case RANGE:
            return RangePredicateEvaluatorFactory.newRawValueBasedEvaluator((RangePredicate) predicate, dataType);
          case REGEXP_LIKE:
            return RegexpLikePredicateEvaluatorFactory.newRawValueBasedEvaluator((RegexpLikePredicate) predicate,
                dataType);
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      }
    } catch (NumberFormatException e) {
      // This NumberFormatException is caused by passing in a non-numeric string as numeric number in query
      throw new BadQueryRequestException(e);
    }
  }
}
