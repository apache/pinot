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
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.core.common.Predicate;
import org.apache.pinot.core.common.predicate.TextMatchPredicate;


public class TextMatchPredicateEvaluatorFactory {
  private TextMatchPredicateEvaluatorFactory() {
  }

  public static BaseRawValueBasedPredicateEvaluator newRawValueBasedEvaluator(TextMatchPredicate textMatchPredicate,
      FieldSpec.DataType dataType) {
    Preconditions.checkArgument(dataType == FieldSpec.DataType.STRING, "Unsupported data type: " + dataType);
    return new RawValueBasedTextMatchPredicateEvaluator(textMatchPredicate);
  }

  public static final class RawValueBasedTextMatchPredicateEvaluator extends BaseRawValueBasedPredicateEvaluator {
    final String _searchQuery;

    public RawValueBasedTextMatchPredicateEvaluator(TextMatchPredicate textMatchPredicate) {
      _searchQuery = textMatchPredicate.getSearchQuery();
    }

    public String getSearchQuery() {
      return _searchQuery;
    }

    @Override
    public Predicate.Type getPredicateType() {
      return Predicate.Type.TEXT_MATCH;
    }

    @Override
    public boolean applySV(String value) {
      // predicate evaluator for TEXT_MATCH is essentially a NO_OP
      // as of now since the search/predicate evaluation will be
      // done by Lucene.
      // the only reason why we have this is to plug-in the
      // code for text search as another filter operator
      // this function will never be called
      throw new UnsupportedOperationException("Operation not supported on TEXT_MATCH predicate evaluator");
    }
  }
}
