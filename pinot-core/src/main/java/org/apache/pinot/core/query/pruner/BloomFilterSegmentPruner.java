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
package org.apache.pinot.core.query.pruner;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.query.prefetch.FetchPlanner;
import org.apache.pinot.core.query.prefetch.FetchPlannerRegistry;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.spi.env.PinotConfiguration;


/// Prunes segments using bloom filters for EQ and IN predicates, with optional prefetch.
public class BloomFilterSegmentPruner extends ValueBasedSegmentPruner {
  private FetchPlanner _fetchPlanner;

  @Override
  public void init(PinotConfiguration config) {
    super.init(config);
    _fetchPlanner = FetchPlannerRegistry.getPlanner();
  }

  @Override
  protected boolean isApplicableToPredicate(Predicate predicate, Map<String, String> queryOptions) {
    // Only prune columns
    if (predicate.getLhs().getType() != ExpressionContext.Type.IDENTIFIER) {
      return false;
    }
    Predicate.Type predicateType = predicate.getType();
    if (predicateType == Predicate.Type.EQ) {
      return true;
    }
    if (predicateType == Predicate.Type.IN) {
      List<String> values = ((InPredicate) predicate).getValues();
      //noinspection RedundantIfStatement
      if (shouldPruneInPredicate(values.size(), queryOptions)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    return prune(segments, query, null);
  }

  @Override
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query,
      @Nullable ExecutorService executorService) {
    if (segments.isEmpty() || !query.isEnablePrefetch()) {
      return super.prune(segments, query, executorService);
    }
    return prefetch(segments, query, fetchContexts -> super.prune(segments, query, executorService, fetchContexts));
  }

  private List<IndexSegment> prefetch(List<IndexSegment> segments, QueryContext query,
      Function<FetchContext[], List<IndexSegment>> pruneFunc) {
    int numSegments = segments.size();
    FetchContext[] fetchContexts = new FetchContext[numSegments];
    try {
      // Prefetch bloom filter for columns within the EQ/IN predicate if exists
      for (int i = 0; i < numSegments; i++) {
        IndexSegment segment = segments.get(i);
        FetchContext fetchContext = _fetchPlanner.planFetchForPruning(segment, query);
        if (!fetchContext.isEmpty()) {
          segment.prefetch(fetchContext);
          fetchContexts[i] = fetchContext;
        }
      }
      return pruneFunc.apply(fetchContexts);
    } finally {
      // Release the prefetched bloom filters
      for (int i = 0; i < numSegments; i++) {
        FetchContext fetchContext = fetchContexts[i];
        if (fetchContext != null) {
          segments.get(i).release(fetchContext);
        }
      }
    }
  }

  @Override
  boolean pruneSegmentWithPredicate(IndexSegment segment, Predicate predicate, Map<String, DataSource> dataSourceCache,
      ValueCache cachedValues, QueryContext query) {
    Predicate.Type predicateType = predicate.getType();
    if (predicateType == Predicate.Type.EQ) {
      return pruneEqPredicate(segment, (EqPredicate) predicate, dataSourceCache, cachedValues);
    } else if (predicateType == Predicate.Type.IN) {
      return pruneInPredicate(segment, (InPredicate) predicate, dataSourceCache, cachedValues, query);
    } else {
      return false;
    }
  }

  /// For EQ predicate, prune the segments based on column bloom filter.
  private boolean pruneEqPredicate(IndexSegment segment, EqPredicate eqPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache) {
    String column = eqPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSourceNullable(column)
        : dataSourceCache.computeIfAbsent(column, segment::getDataSourceNullable);
    if (dataSource == null) {
      // Column does not exist, cannot prune
      return false;
    }
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    ValueCache.CachedValue cachedValue = valueCache.get(eqPredicate, dataSourceMetadata.getDataType());
    // Check bloom filter
    BloomFilterReader bloomFilter = dataSource.getBloomFilter();
    return bloomFilter != null && !cachedValue.mightBeContained(bloomFilter);
  }

  /// For IN predicate, prune the segments based on column bloom filter.
  private boolean pruneInPredicate(IndexSegment segment, InPredicate inPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache, QueryContext query) {
    List<String> values = inPredicate.getValues();
    if (!shouldPruneInPredicate(values.size(), query.getQueryOptions())) {
      return false;
    }
    String column = inPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSourceNullable(column)
        : dataSourceCache.computeIfAbsent(column, segment::getDataSourceNullable);
    if (dataSource == null) {
      // Column does not exist, cannot prune
      return false;
    }
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    List<ValueCache.CachedValue> cachedValues = valueCache.get(inPredicate, dataSourceMetadata.getDataType());
    // Check bloom filter
    BloomFilterReader bloomFilter = dataSource.getBloomFilter();
    if (bloomFilter == null) {
      return false;
    }
    for (ValueCache.CachedValue value : cachedValues) {
      if (value.mightBeContained(bloomFilter)) {
        return false;
      }
    }
    return true;
  }
}
