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
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;


public class PredicateEvaluatorProvider {
  private PredicateEvaluatorProvider() {
  }

  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, @Nullable Dictionary dictionary,
      DataType dataType) {
    return getPredicateEvaluator(predicate, dictionary, dataType, null);
  }

  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, @Nullable Dictionary dictionary,
      DataType dataType, @Nullable QueryContext queryContext) {
    try {
      if (dictionary != null) {
        // dictionary based predicate evaluators
        switch (predicate.getType()) {
          case EQ:
            return EqualsPredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((EqPredicate) predicate, dictionary, dataType);
          case NOT_EQ:
            return NotEqualsPredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((NotEqPredicate) predicate, dictionary, dataType);
          case IN:
            return InPredicateEvaluatorFactory.newDictionaryBasedEvaluator((InPredicate) predicate, dictionary,
                dataType, queryContext);
          case NOT_IN:
            return NotInPredicateEvaluatorFactory.newDictionaryBasedEvaluator((NotInPredicate) predicate, dictionary,
                dataType, queryContext);
          case RANGE:
            return RangePredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((RangePredicate) predicate, dictionary, dataType);
          case REGEXP_LIKE:
            return RegexpLikePredicateEvaluatorFactory
                .newDictionaryBasedEvaluator((RegexpLikePredicate) predicate, dictionary, dataType);
          default:
            throw new UnsupportedOperationException("Unsupported predicate type: " + predicate.getType());
        }
      } else {
        // raw value based predicate evaluators
        switch (predicate.getType()) {
          case EQ:
            return EqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((EqPredicate) predicate, dataType);
          case NOT_EQ:
            return NotEqualsPredicateEvaluatorFactory.newRawValueBasedEvaluator((NotEqPredicate) predicate, dataType);
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
    } catch (Exception e) {
      // Exception here is caused by mismatch between the column data type and the predicate value in the query
      throw new BadQueryRequestException(e);
    }
  }

  public static PredicateEvaluator getPredicateEvaluator(Predicate predicate, DataSource dataSource,
      QueryContext queryContext) {
    return getPredicateEvaluator(predicate, dataSource.getDictionary(),
        dataSource.getDataSourceMetadata().getDataType(), queryContext);
  }
}
