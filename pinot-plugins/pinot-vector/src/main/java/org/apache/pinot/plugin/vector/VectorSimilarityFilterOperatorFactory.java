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
package org.apache.pinot.plugin.vector;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.custom.CustomFilterOperatorFactory;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;


/**
 * Factory that creates vector similarity filter operators for custom VECTOR_SIMILARITY predicates.
 *
 * <p>This factory is registered via ServiceLoader and looked up by predicate name at query
 * execution time. It ports the logic from {@code FilterPlanNode.constructVectorSimilarityOperator()}
 * into the plugin module.</p>
 */
public class VectorSimilarityFilterOperatorFactory implements CustomFilterOperatorFactory {

  @Override
  public String predicateName() {
    return "VECTOR_SIMILARITY";
  }

  @Override
  public BaseFilterOperator createFilterOperator(IndexSegment indexSegment, QueryContext queryContext,
      Predicate predicate, DataSource dataSource, int numDocs) {
    return createOperator(indexSegment, queryContext, predicate, dataSource, numDocs, false);
  }

  private BaseFilterOperator createOperator(IndexSegment indexSegment, QueryContext queryContext,
      Predicate predicate, @Nullable DataSource dataSource, int numDocs, boolean hasMetadataFilter) {
    VectorSimilarityPredicate vectorPredicate = (VectorSimilarityPredicate) predicate;
    String column = predicate.getLhs().getIdentifier();

    // If dataSource is null (function-based LHS), resolve from segment
    if (dataSource == null) {
      dataSource = indexSegment.getDataSource(column, queryContext.getSchema());
    }

    VectorIndexReader vectorIndex = dataSource.getVectorIndex();
    VectorIndexConfig vectorIndexConfig = dataSource.getVectorIndexConfig();
    VectorSearchParams searchParams = VectorSearchParams.fromQueryOptions(queryContext.getQueryOptions());

    if (vectorIndex != null) {
      ForwardIndexReader<?> forwardIndexReader = null;
      VectorBackendType backendType = VectorDistanceUtils.resolveBackendType(vectorIndexConfig);
      if (searchParams.isExactRerank(backendType) || searchParams.hasDistanceThreshold()) {
        forwardIndexReader = dataSource.getForwardIndex();
        Preconditions.checkState(!searchParams.hasDistanceThreshold() || forwardIndexReader != null,
            "Cannot apply vectorDistanceThreshold on column: %s -- forward index required", column);
      }
      return new VectorSimilarityFilterOperator(vectorIndex, vectorPredicate, numDocs, searchParams,
          forwardIndexReader, vectorIndexConfig, hasMetadataFilter);
    }

    // Exact scan fallback
    ForwardIndexReader<?> forwardIndexReader = dataSource.getForwardIndex();
    Preconditions.checkState(forwardIndexReader != null,
        "Cannot apply VECTOR_SIMILARITY on column: %s -- no vector index and no forward index", column);
    return new ExactVectorScanFilterOperator(forwardIndexReader, vectorPredicate, column, numDocs,
        vectorIndexConfig, getVectorFallbackReason(vectorIndexConfig), searchParams);
  }

  private static String getVectorFallbackReason(@Nullable VectorIndexConfig vectorIndexConfig) {
    if (vectorIndexConfig == null || vectorIndexConfig.isDisabled()) {
      return "vector_index_missing";
    }
    return vectorIndexConfig.resolveBackendType().supportsMutableSegments()
        ? "vector_index_missing"
        : vectorIndexConfig.resolveBackendType().name().toLowerCase() + "_index_unavailable";
  }
}
