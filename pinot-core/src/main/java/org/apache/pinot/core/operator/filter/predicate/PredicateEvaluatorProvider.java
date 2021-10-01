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

import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.common.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;


public class PredicateEvaluatorProvider {
  private PredicateEvaluatorProvider() {
  }

  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, @Nullable Dictionary dictionary,
      DataType dataType) {
    PredicateEvaluator predicateEvaluator;
    try {
      if (dictionary != null) {
        // dictionary based predicate evaluators
        switch (predicate.getType()) {
          case EQ:
            predicateEvaluator = EqualsPredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((EqPredicate) predicate, dictionary, dataType);
            break;
          case NOT_EQ:
            predicateEvaluator = NotEqualsPredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((NotEqPredicate) predicate, dictionary, dataType);
            break;
          case IN:
            predicateEvaluator = InPredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((InPredicate) predicate, dictionary, dataType);
            break;
          case NOT_IN:
            predicateEvaluator = NotInPredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((NotInPredicate) predicate, dictionary, dataType);
            break;
          case RANGE:
            predicateEvaluator = RangePredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((RangePredicate) predicate, dictionary, dataType);
            break;
          case REGEXP_LIKE:
            predicateEvaluator = RegexpLikePredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((RegexpLikePredicate) predicate, dictionary, dataType);
            break;
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      } else {
        // raw value based predicate evaluators
        switch (predicate.getType()) {
          case EQ:
            predicateEvaluator =
                EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((EqPredicate) predicate, dataType);
            break;
          case NOT_EQ:
            predicateEvaluator =
                NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((NotEqPredicate) predicate, dataType);
            break;
          case IN:
            predicateEvaluator =
                InPredicateEvaluatorFactory.newRawValueBasedEvaluator((InPredicate) predicate, dataType);
            break;
          case NOT_IN:
            predicateEvaluator =
                NotInPredicateEvaluatorFactory.newRawValueBasedEvaluator((NotInPredicate) predicate, dataType);
            break;
          case RANGE:
            predicateEvaluator =
                RangePredicateEvaluatorFactory.newRawValueBasedEvaluator((RangePredicate) predicate, dataType);
            break;
          case REGEXP_LIKE:
            predicateEvaluator = RegexpLikePredicateEvaluatorFactory
                .newRawValueBasedEvaluator((RegexpLikePredicate) predicate, dataType);
            break;
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      }
    } catch (Exception e) {
      // Exception here is caused by mismatch between the column data type and the predicate value in the query
      throw new BadQueryRequestException(e);
    }
    predicateEvaluator.setPredicate(predicate);
    return predicateEvaluator;
  }
}
