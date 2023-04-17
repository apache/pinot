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
package org.apache.pinot.core.query.prefetch;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;


public class DefaultFetchPlanner implements FetchPlanner {
  /**
   * Get BloomFilter for columns present in EQ and IN predicates.
   */
  @Override
  public FetchContext planFetchForPruning(IndexSegment indexSegment, QueryContext queryContext) {
    // Extract columns in EQ/IN predicates.
    Set<String> eqInColumns = new HashSet<>();
    extractEqInColumns(Objects.requireNonNull(queryContext.getFilter()), eqInColumns);
    Map<String, List<IndexType<?, ?, ?>>> columnToIndexList = new HashMap<>();
    for (String column : eqInColumns) {
      DataSource dataSource = indexSegment.getDataSource(column);
      if (dataSource.getBloomFilter() != null) {
        columnToIndexList.put(column, Collections.singletonList(StandardIndexes.bloomFilter()));
      }
    }
    return new FetchContext(UUID.randomUUID(), indexSegment.getSegmentName(), columnToIndexList);
  }

  protected static void extractEqInColumns(FilterContext filter, Set<String> eqInColumns) {
    switch (filter.getType()) {
      case AND:
      case OR:
        for (FilterContext child : filter.getChildren()) {
          extractEqInColumns(child, eqInColumns);
        }
        break;
      case NOT:
        // Do not track the predicates under NOT filter
        break;
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        ExpressionContext lhs = predicate.getLhs();
        if (lhs.getType() != ExpressionContext.Type.IDENTIFIER) {
          // Only prune columns
          break;
        }
        String column = lhs.getIdentifier();
        Predicate.Type predicateType = predicate.getType();
        if (predicateType == Predicate.Type.EQ || predicateType == Predicate.Type.IN) {
          eqInColumns.add(column);
        }
        break;
      default:
        throw new IllegalStateException("Unknown filter type: " + filter.getType());
    }
  }

  /**
   * Fetch all kinds of index for columns accessed by the query.
   */
  @Override
  public FetchContext planFetchForProcessing(IndexSegment indexSegment, QueryContext queryContext) {
    return new FetchContext(UUID.randomUUID(), indexSegment.getSegmentName(), getColumns(indexSegment, queryContext));
  }

  private Set<String> getColumns(IndexSegment indexSegment, QueryContext queryContext) {
    List<ExpressionContext> selectExpressions = queryContext.getSelectExpressions();
    if (selectExpressions.size() == 1 && "*".equals(selectExpressions.get(0).getIdentifier())) {
      return indexSegment.getPhysicalColumnNames();
    } else {
      return queryContext.getColumns();
    }
  }
}
