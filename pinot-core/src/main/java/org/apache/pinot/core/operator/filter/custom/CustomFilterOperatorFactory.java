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
package org.apache.pinot.core.operator.filter.custom;

import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;


/**
 * SPI interface for creating filter operators for custom predicates at query execution time.
 *
 * <p>Plugin authors implement this interface to define how their custom predicate is executed
 * against a segment. The factory is looked up by predicate name via
 * {@link CustomFilterOperatorRegistry}.
 *
 * <p>Implementations are discovered via {@link java.util.ServiceLoader} or registered
 * programmatically through {@link CustomFilterOperatorRegistry#register(CustomFilterOperatorFactory)}.
 *
 * <p>Thread-safety: implementations must be stateless and thread-safe.
 */
public interface CustomFilterOperatorFactory {

  /**
   * Returns the predicate name this factory handles (e.g., "SEMANTIC_MATCH").
   * Must match the name returned by the corresponding
   * {@link org.apache.pinot.common.request.context.predicate.CustomPredicate#getCustomTypeName()}.
   */
  String predicateName();

  /**
   * Creates a filter operator for the given custom predicate against a specific segment.
   *
   * @param indexSegment the segment to filter
   * @param queryContext the query context
   * @param predicate the custom predicate (can be cast to the plugin's specific CustomPredicate subclass)
   * @param dataSource the data source for the predicate's column, or {@code null} if the predicate
   *                   LHS is a function expression rather than a column identifier. Implementations
   *                   must handle null if they wish to support function-based predicates.
   * @param numDocs the number of documents in the segment
   * @return a filter operator that evaluates this predicate
   */
  BaseFilterOperator createFilterOperator(IndexSegment indexSegment, QueryContext queryContext,
      Predicate predicate, @Nullable DataSource dataSource, int numDocs);

  /**
   * Creates a filter operator with metadata filter awareness. Called when this custom predicate
   * appears inside an AND with other non-custom filters (metadata filters).
   *
   * <p>Some predicates (e.g., VECTOR_SIMILARITY) adjust their behavior when combined with
   * metadata filters — for example, over-fetching ANN candidates to compensate for post-filter
   * reduction. Override this method to support that pattern.
   *
   * <p>The default implementation ignores the {@code hasMetadataFilter} flag and delegates to
   * {@link #createFilterOperator(IndexSegment, QueryContext, Predicate, DataSource, int)}.
   *
   * @param indexSegment the segment to filter
   * @param queryContext the query context
   * @param predicate the custom predicate
   * @param dataSource the data source for the predicate's column (null if LHS is a function)
   * @param numDocs the number of documents in the segment
   * @param hasMetadataFilter true if this predicate is combined with metadata filters in an AND
   * @return a filter operator that evaluates this predicate
   */
  default BaseFilterOperator createFilterOperator(IndexSegment indexSegment, QueryContext queryContext,
      Predicate predicate, @Nullable DataSource dataSource, int numDocs, boolean hasMetadataFilter) {
    return createFilterOperator(indexSegment, queryContext, predicate, dataSource, numDocs);
  }
}
