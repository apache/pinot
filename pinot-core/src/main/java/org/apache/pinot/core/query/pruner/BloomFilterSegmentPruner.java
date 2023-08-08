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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.core.query.prefetch.FetchPlanner;
import org.apache.pinot.core.query.prefetch.FetchPlannerRegistry;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryCancelledException;


/**
 * The {@code BloomFilterSegmentPruner} prunes segments based on bloom filter for EQUALITY filter. Because the access
 * to bloom filter data is required, segment pruning is done in parallel when the number of segments is large.
 */
@SuppressWarnings({"rawtypes", "unchecked", "RedundantIfStatement"})
public class BloomFilterSegmentPruner extends ValueBasedSegmentPruner {
  // Try to schedule 10 segments for each thread, or evenly distribute them to all MAX_NUM_THREADS_PER_QUERY threads.
  // TODO: make this threshold configurable? threshold 10 is also used in CombinePlanNode, which accesses the
  //       dictionary data to do query planning and if segments are more than 10, planning is done in parallel.
  private static final int TARGET_NUM_SEGMENTS_PER_THREAD = 10;

  private FetchPlanner _fetchPlanner;

  @Override
  public void init(PinotConfiguration config) {
    super.init(config);
    _fetchPlanner = FetchPlannerRegistry.getPlanner();
  }

  @Override
  protected boolean isApplicableToPredicate(Predicate predicate) {
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
      // Skip pruning when there are too many values in the IN predicate
      if (values.size() <= _inPredicateThreshold) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query) {
    if (segments.isEmpty()) {
      return segments;
    }
    if (!query.isEnablePrefetch()) {
      return super.prune(segments, query);
    }
    return prefetch(segments, query, fetchContexts -> {
      int numSegments = segments.size();
      FilterContext filter = Objects.requireNonNull(query.getFilter());
      ValueCache cachedValues = new ValueCache();
      Map<String, DataSource> dataSourceCache = new HashMap<>();
      List<IndexSegment> selectedSegments = new ArrayList<>(numSegments);
      for (int i = 0; i < numSegments; i++) {
        dataSourceCache.clear();
        IndexSegment segment = segments.get(i);
        if (!pruneSegmentWithFetchContext(segment, fetchContexts[i], filter, dataSourceCache, cachedValues)) {
          selectedSegments.add(segment);
        }
      }
      return selectedSegments;
    });
  }

  @Override
  public List<IndexSegment> prune(List<IndexSegment> segments, QueryContext query,
      @Nullable ExecutorService executorService) {
    if (segments.isEmpty()) {
      return segments;
    }
    if (executorService == null || segments.size() <= TARGET_NUM_SEGMENTS_PER_THREAD) {
      // If executor is not provided, or the number of segments is small, prune them sequentially
      return prune(segments, query);
    }
    // With executor service and large number of segments, prune them in parallel.
    // NOTE: Even if numTasks=1 i.e. we get a single executor thread, still run it using a separate thread so that
    //       the timeout can be honored. For example, this may happen when there is only one processor.
    int numTasks = QueryMultiThreadingUtils.getNumTasks(segments.size(), TARGET_NUM_SEGMENTS_PER_THREAD,
        query.getMaxExecutionThreads());
    if (!query.isEnablePrefetch()) {
      return pruneInParallel(numTasks, segments, query, executorService, null);
    }
    return prefetch(segments, query,
        fetchContexts -> pruneInParallel(numTasks, segments, query, executorService, fetchContexts));
  }

  private List<IndexSegment> pruneInParallel(int numTasks, List<IndexSegment> segments, QueryContext queryContext,
      ExecutorService executorService, FetchContext[] fetchContexts) {
    int numSegments = segments.size();
    List<IndexSegment> allSelectedSegments = new ArrayList<>();
    QueryMultiThreadingUtils.runTasksWithDeadline(numTasks, index -> {
      FilterContext filter = Objects.requireNonNull(queryContext.getFilter());
      ValueCache cachedValues = new ValueCache();
      Map<String, DataSource> dataSourceCache = new HashMap<>();
      List<IndexSegment> selectedSegments = new ArrayList<>();
      for (int i = index; i < numSegments; i += numTasks) {
        dataSourceCache.clear();
        IndexSegment segment = segments.get(i);
        FetchContext fetchContext = fetchContexts == null ? null : fetchContexts[i];
        if (!pruneSegmentWithFetchContext(segment, fetchContext, filter, dataSourceCache, cachedValues)) {
          selectedSegments.add(segment);
        }
      }
      return selectedSegments;
    }, taskRes -> {
      if (taskRes != null) {
        allSelectedSegments.addAll(taskRes);
      }
    }, e -> {
      if (e instanceof InterruptedException) {
        throw new QueryCancelledException("Cancelled while running BloomFilterSegmentPruner", e);
      }
      throw new RuntimeException("Caught exception while running BloomFilterSegmentPruner", e);
    }, executorService, queryContext.getEndTimeMs());
    return allSelectedSegments;
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

  private boolean pruneSegmentWithFetchContext(IndexSegment segment, FetchContext fetchContext, FilterContext filter,
      Map<String, DataSource> dataSourceCache, ValueCache cachedValues) {
    if (fetchContext == null) {
      return pruneSegment(segment, filter, dataSourceCache, cachedValues);
    }
    try {
      segment.acquire(fetchContext);
      return pruneSegment(segment, filter, dataSourceCache, cachedValues);
    } finally {
      segment.release(fetchContext);
    }
  }

  @Override
  boolean pruneSegmentWithPredicate(IndexSegment segment, Predicate predicate, Map<String, DataSource> dataSourceCache,
      ValueCache cachedValues) {
    Predicate.Type predicateType = predicate.getType();
    if (predicateType == Predicate.Type.EQ) {
      return pruneEqPredicate(segment, (EqPredicate) predicate, dataSourceCache, cachedValues);
    } else if (predicateType == Predicate.Type.IN) {
      return pruneInPredicate(segment, (InPredicate) predicate, dataSourceCache, cachedValues);
    } else {
      return false;
    }
  }

  /**
   * For EQ predicate, prune the segments based on column bloom filter.
   */
  private boolean pruneEqPredicate(IndexSegment segment, EqPredicate eqPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache) {
    String column = eqPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSource(column)
        : dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
    DataSourceMetadata dataSourceMetadata = dataSource.getDataSourceMetadata();
    ValueCache.CachedValue cachedValue = valueCache.get(eqPredicate, dataSourceMetadata.getDataType());
    // Check bloom filter
    BloomFilterReader bloomFilter = dataSource.getBloomFilter();
    return bloomFilter != null && !cachedValue.mightBeContained(bloomFilter);
  }

  /**
   * For IN predicate, prune the segments based on column bloom filter.
   * NOTE: segments will not be pruned if the number of values is greater than the threshold.
   */
  private boolean pruneInPredicate(IndexSegment segment, InPredicate inPredicate,
      Map<String, DataSource> dataSourceCache, ValueCache valueCache) {
    List<String> values = inPredicate.getValues();
    // Skip pruning when there are too many values in the IN predicate
    if (values.size() > _inPredicateThreshold) {
      return false;
    }
    String column = inPredicate.getLhs().getIdentifier();
    DataSource dataSource = segment instanceof ImmutableSegment ? segment.getDataSource(column)
        : dataSourceCache.computeIfAbsent(column, segment::getDataSource);
    // NOTE: Column must exist after DataSchemaSegmentPruner
    assert dataSource != null;
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
