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
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.store.ColumnIndexType;


public class DefaultFetchPlanner implements FetchPlanner {
  /**
   * Get BloomFilter for columns present in EQ and IN predicates.
   */
  @Nullable
  @Override
  public FetchContext planFetchForPruning(IndexSegment indexSegment, QueryContext queryContext,
      @Nullable Map<Predicate.Type, Set<String>> requestedColumns) {
    if (requestedColumns == null) {
      return null;
    }
    Set<String> columns = new HashSet<>();
    Set<String> requested = requestedColumns.get(Predicate.Type.EQ);
    if (CollectionUtils.isNotEmpty(requested)) {
      columns.addAll(requested);
    }
    requested = requestedColumns.get(Predicate.Type.IN);
    if (CollectionUtils.isNotEmpty(requested)) {
      columns.addAll(requested);
    }
    if (columns.isEmpty()) {
      return null;
    }
    Map<String, List<ColumnIndexType>> columnToIndexList = new HashMap<>();
    for (String column : columns) {
      DataSource dataSource = indexSegment.getDataSource(column);
      if (dataSource.getBloomFilter() != null) {
        columnToIndexList.put(column, Collections.singletonList(ColumnIndexType.BLOOM_FILTER));
      }
    }
    if (columnToIndexList.isEmpty()) {
      return null;
    }
    return new FetchContext(UUID.randomUUID(), indexSegment.getSegmentName(), columnToIndexList);
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
