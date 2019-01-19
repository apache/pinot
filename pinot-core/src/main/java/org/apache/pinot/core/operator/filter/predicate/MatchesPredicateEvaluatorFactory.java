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

import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.common.predicate.MatchesPredicate;

public class MatchesPredicateEvaluatorFactory {
  private MatchesPredicateEvaluatorFactory() {
  }

  /**
   * Create a new instance of raw value based REGEXP_LIKE predicate evaluator.
   * @param textMatchPredicate REGEXP_LIKE predicate to evaluate
   * @param dataType Data type for the column
   * @return Raw value based REGEXP_LIKE predicate evaluator
   */
  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(
      MatchesPredicate textMatchPredicate, FieldSpec.DataType dataType) {
    return new RawValueBasedTextMatchPredicateEvaluator(textMatchPredicate);
  }

  public static final class RawValueBasedTextMatchPredicateEvaluator
      extends BaseRawValueBasedPredicateEvaluator {
    String _query;
    String _options;

    public RawValueBasedTextMatchPredicateEvaluator(MatchesPredicate textMatchPredicate) {
      _query = textMatchPredicate.getQuery();
      _options = textMatchPredicate.getQueryOptions();
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.MATCHES;
    }

    @Override
    public boolean applySV(String value) {
      throw new UnsupportedOperationException(
          "Text Match is not supported via scanning, its supported only via inverted index");
    }

    public String getQueryString() {
      return _query;
    }

    public String getQueryOptions() {
      return _options;
    }
  }
}
