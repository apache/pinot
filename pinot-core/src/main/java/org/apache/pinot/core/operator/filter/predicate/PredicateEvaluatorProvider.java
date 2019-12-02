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

import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.common.predicate.EqPredicate;
import org.apache.pinot.core.common.predicate.InPredicate;
import org.apache.pinot.core.common.predicate.NEqPredicate;
import org.apache.pinot.core.common.predicate.NotInPredicate;
import org.apache.pinot.core.common.predicate.RangePredicate;
import org.apache.pinot.core.common.predicate.RegexpLikePredicate;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.segment.index.readers.Dictionary;


public class PredicateEvaluatorProvider {
  private PredicateEvaluatorProvider() {
  }

  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, Dictionary dictionary, DataType dataType) {
    try {
      if (dictionary != null) {
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
            return RegexpLikePredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((RegexpLikePredicate) predicate, dictionary);
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      } else {
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
            return RegexpLikePredicateEvaluatorFactory
                .newRawValueBasedEvaluator((RegexpLikePredicate) predicate, dataType);
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
