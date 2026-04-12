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
package org.apache.pinot.plugin.filter.example;

import java.util.regex.Pattern;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.custom.CustomFilterOperatorFactory;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * Factory that creates filter operators for LIKE_ANY predicates.
 *
 * <p>Implementation strategy: converts LIKE patterns to a combined regex {@code (pat1|pat2|...)}
 * and evaluates using a scan-based approach. For each document, the column value is tested
 * against the compiled pattern.
 */
public class LikeAnyFilterOperatorFactory implements CustomFilterOperatorFactory {

  @Override
  public String predicateName() {
    return "LIKE_ANY";
  }

  @Override
  public BaseFilterOperator createFilterOperator(IndexSegment indexSegment, QueryContext queryContext,
      Predicate predicate, DataSource dataSource, int numDocs) {
    LikeAnyPredicate likeAnyPredicate = (LikeAnyPredicate) predicate;

    // Convert LIKE patterns to regex and combine with OR
    StringBuilder regexBuilder = new StringBuilder();
    for (int i = 0; i < likeAnyPredicate.getPatterns().size(); i++) {
      if (i > 0) {
        regexBuilder.append('|');
      }
      String regexPattern = RegexpPatternConverterUtils.likeToRegexpLike(likeAnyPredicate.getPatterns().get(i));
      regexBuilder.append('(').append(regexPattern).append(')');
    }
    Pattern compiledPattern = Pattern.compile(regexBuilder.toString());

    // Create a scan-based evaluator using the combined regex
    Dictionary dictionary = dataSource.getDictionary();
    DataType dataType = dataSource.getDataSourceMetadata().getDataType();
    PredicateEvaluator evaluator;
    if (dictionary != null) {
      evaluator = new LikeAnyDictionaryEvaluator(likeAnyPredicate, compiledPattern, dictionary);
    } else {
      evaluator = new LikeAnyRawEvaluator(likeAnyPredicate, compiledPattern);
    }

    return FilterOperatorUtils.getLeafFilterOperator(queryContext, evaluator, dataSource, numDocs);
  }
}
