package com.linkedin.thirdeye.query;

import com.google.common.base.Joiner;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import com.linkedin.thirdeye.impl.storage.IndexMetadata;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ThirdEyeQueryExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeQueryExecutor.class);
  private static final Joiner OR_JOINER = Joiner.on(" OR ");

  private static final ThirdEyeFunction TO_MILLIS = new ThirdEyeUnitConversionFunction(1, TimeUnit.MILLISECONDS);

  private final ExecutorService executorService;
  private final StarTreeManager starTreeManager;
  private static Map<String, Integer> timeGranularitySortOrder = new HashMap<String, Integer>();
  static {
    timeGranularitySortOrder.put("MONTHLY", 0);
    timeGranularitySortOrder.put("WEEKLY", 1);
    timeGranularitySortOrder.put("DAILY", 2);
    timeGranularitySortOrder.put("HOURLY", 3);
  }
  public ThirdEyeQueryExecutor(ExecutorService executorService, StarTreeManager starTreeManager) {
    this.executorService = executorService;
    this.starTreeManager = starTreeManager;
  }

  public ThirdEyeQueryResult executeQuery(String sql) throws Exception {
    return executeQuery(new ThirdEyeQueryParser(sql).getQuery());
  }

  public ThirdEyeQueryResult executeQuery(final ThirdEyeQuery query) throws Exception {
    LOGGER.info("START Execution for query_id: {} query{}", query.hashCode(), query);
    ThirdEyeQueryResult result = new ThirdEyeQueryResult();

    final StarTreeConfig config = starTreeManager.getConfig(query.getCollection());
    if (config == null) {
      throw new IllegalArgumentException("No collection " + query.getCollection());
    }
    final List<String> dimensionNames = new ArrayList<>(config.getDimensions().size());
    for (DimensionSpec dimensionSpec : config.getDimensions()) {
      dimensionNames.add(dimensionSpec.getName());
    }
    result.setDimensions(dimensionNames);

    // Offset for moving average
    long startOffset = 0;
    long collectionWindowMillis = 0;
    for (ThirdEyeFunction function : query.getFunctions()) {
      if (function instanceof ThirdEyeMovingAverageFunction) {
        ThirdEyeMovingAverageFunction movingAverageFunction = (ThirdEyeMovingAverageFunction) function;
        TimeGranularity window = movingAverageFunction.getWindow();
        long windowMillis = TimeUnit.MILLISECONDS.convert(window.getSize(), window.getUnit());
        if (windowMillis > startOffset) {
          startOffset = windowMillis;
        }
      } else if (function instanceof ThirdEyeAggregateFunction) {
        ThirdEyeAggregateFunction aggregateFunction = (ThirdEyeAggregateFunction) function;
        TimeGranularity window = aggregateFunction.getWindow();
        collectionWindowMillis = TimeUnit.MILLISECONDS.convert(window.getSize(), window.getUnit());
      }
    }

    // Time
    DateTime queryStartInMillis = new DateTime(query.getStart().getMillis() - startOffset);
    long queryStartTime = dateTimeToCollectionTime(config, queryStartInMillis);
    long queryEndTime = dateTimeToCollectionTime(config, query.getEnd());


    final TimeRange inputQueryTimeRange = new TimeRange(queryStartInMillis.getMillis(), query.getEnd().getMillis());

    // select the trees that need to be queried based on the
    Map<UUID, IndexMetadata> immutableMetadataMap = new HashMap<>();
    Map<UUID, IndexMetadata> realTimeMetadataMap = new HashMap<>();
    for (StarTree starTree : starTreeManager.getStarTrees(config.getCollection()).values()) {
      UUID treeId = starTree.getRoot().getId();
      IndexMetadata indexMetadata = starTreeManager.getIndexMetadata(treeId);
      if ("KAFKA".equals(indexMetadata.getTimeGranularity())) {
        realTimeMetadataMap.put(treeId, indexMetadata);
      } else {
        immutableMetadataMap.put(treeId, indexMetadata);
      }
    }
    LOGGER.info("Selecting trees to query for queryTimeRange:{}", inputQueryTimeRange);
    List<UUID> treeIdsToQuery = selectTreesToQuery(immutableMetadataMap, inputQueryTimeRange);

    // Align to aggregation boundary
    if (collectionWindowMillis > 0) {
      long collectionWindow = dateTimeToCollectionTime(config, new DateTime(collectionWindowMillis));
      queryStartTime = (queryStartTime / collectionWindow) * collectionWindow;
      queryEndTime = (queryEndTime / collectionWindow + 1) * collectionWindow; // include everything in that window
    }
    final TimeRange queryTimeRange = new TimeRange(queryStartTime, queryEndTime);

    // Time ranges that should be queried for each tree
    final Map<UUID, TimeRange> timeRangesToQuery = new HashMap<>();
    for (UUID treeId : treeIdsToQuery) {
      // Whole query time for each of the immutable trees
      timeRangesToQuery.put(treeId, queryTimeRange);
    }

    // Determine max data time from the most recent tree that's being queried from immutable segments
    Long maxImmutableTimeMillis = null;
    for (UUID treeId : treeIdsToQuery) {
      IndexMetadata indexMetadata = immutableMetadataMap.get(treeId);
      if (maxImmutableTimeMillis == null || maxImmutableTimeMillis < indexMetadata.getMaxDataTimeMillis()) {
        maxImmutableTimeMillis = indexMetadata.getMaxDataTimeMillis();
      }
    }

    // Get the starting collection time we should use for real-time segments
    Long realTimeStartTime = null;
    Long realTimeStartTimeMillis = null;
    if (maxImmutableTimeMillis == null) {
      realTimeStartTime = queryTimeRange.getStart();
      realTimeStartTimeMillis = queryStartInMillis.getMillis();
    } else if (maxImmutableTimeMillis < query.getEnd().getMillis()) {
      realTimeStartTime = dateTimeToCollectionTime(config, new DateTime(maxImmutableTimeMillis));
      realTimeStartTimeMillis = maxImmutableTimeMillis;
      long collectionWindow = dateTimeToCollectionTime(config, new DateTime(collectionWindowMillis));
      if (collectionWindow > 0) {
        realTimeStartTime = (realTimeStartTime / collectionWindow) * collectionWindow;
      }
    }

    // Get the real time trees we need to query
    List<UUID> realTimeTreeIdsToQuery = new ArrayList<>();
    if (realTimeStartTime != null) {
      TimeRange mutableTimeRange = new TimeRange(realTimeStartTime, queryTimeRange.getEnd());
      TimeRange mutableTimeRangeMillis = new TimeRange(realTimeStartTimeMillis, query.getEnd().getMillis());
      realTimeTreeIdsToQuery.addAll(selectTreesToQuery(realTimeMetadataMap, mutableTimeRangeMillis));

      // Also add in-memory tree (though it may not match)
      StarTree mutableTree = starTreeManager.getMutableStarTree(config.getCollection());
      realTimeTreeIdsToQuery.add(mutableTree.getRoot().getId());

      // Only query the back-end of the time range for these trees
      for (UUID treeId : realTimeTreeIdsToQuery) {
        timeRangesToQuery.put(treeId, mutableTimeRange);
      }
    }

    // For all group by dimensions add those as fixed
    if (!query.getGroupByColumns().isEmpty()) {
      for (final String groupByColumn : query.getGroupByColumns()) {
        if (query.getDimensionValues().containsKey(groupByColumn)) {
          throw new IllegalArgumentException("Cannot fix dimension value in group by: " + groupByColumn);
        }

        final Set<Future<Set<String>>> dimensionSetFutures = new HashSet<>();
        final List<StarTree> starTrees = new ArrayList<>(starTreeManager.getStarTrees(config.getCollection()).values());
        StarTree mutableTree = starTreeManager.getMutableStarTree(config.getCollection());
        if (mutableTree != null) {
          starTrees.add(mutableTree);
        }
        for (final StarTree starTree : starTrees) {
          UUID treeId = starTree.getRoot().getId();
          if (!treeIdsToQuery.contains(treeId) && !realTimeTreeIdsToQuery.contains(treeId)) {
            continue;
          }

          dimensionSetFutures.add(executorService.submit(new Callable<Set<String>>() {
            @Override
            public Set<String> call() throws Exception {
              // TODO: Support multiple values per dimension
              Multimap<String, String> values = query.getDimensionValues();
              Map<String, String> singleValues = new HashMap<>(values.size());
              for (Map.Entry<String, String> entry : query.getDimensionValues().entries()) {
                if (singleValues.containsKey(entry.getKey())) {
                  throw new IllegalArgumentException("Multiple values currently not supported: " + values);
                }
                singleValues.put(entry.getKey(), entry.getValue());
              }
              return starTree.getDimensionValues(groupByColumn, singleValues);
            }
          }));
        }

        Set<String> dimensionSet = new HashSet<>();
        for (Future<Set<String>> future : dimensionSetFutures) {
          dimensionSet.addAll(future.get());
        }
        dimensionSet.remove(StarTreeConstants.STAR);  // never represent this one

        for (String dimensionValue : dimensionSet) {
          query.addDimensionValue(groupByColumn, dimensionValue);
        }
      }
    }

    // Dimensions
    List<DimensionKey> dimensionKeys = new ArrayList<>();
    for (String[] combination : query.getDimensionCombinations(config.getDimensions())) {
      dimensionKeys.add(new DimensionKey(combination));
    }

    // Metrics
    Map<StarTree, Multimap<DimensionKey, Future<MetricTimeSeries>>> timeSeriesFutures = new HashMap<>();
    final List<StarTree> starTrees = new ArrayList<>(starTreeManager.getStarTrees(config.getCollection()).values());
    StarTree mutableTree = starTreeManager.getMutableStarTree(config.getCollection());
    if (mutableTree != null) {
      starTrees.add(mutableTree);
    }
    for (final StarTree starTree : starTrees) {
      final UUID treeId = starTree.getRoot().getId();
      if (!treeIdsToQuery.contains(treeId) && !realTimeTreeIdsToQuery.contains(treeId)) {
        continue;
      }

      Multimap<DimensionKey, Future<MetricTimeSeries>> singleKeyResultMap = LinkedListMultimap.create();
      timeSeriesFutures.put(starTree, singleKeyResultMap);
      for (final DimensionKey dimensionKey : dimensionKeys) {
        DimensionKey flattenedKey = flattenDisjunctions(config, query, dimensionKey);
        timeSeriesFutures.get(starTree).put(flattenedKey, executorService.submit(new Callable<MetricTimeSeries>() {
          @Override
          public MetricTimeSeries call() throws Exception {
            TimeRange timeRange = timeRangesToQuery.get(treeId);
            return starTree.getTimeSeries(new StarTreeQueryImpl(config, dimensionKey, timeRange));
          }
        }));
      }
    }

    // Merge results
    Map<DimensionKey, MetricTimeSeries> mergedResults = new HashMap<>();
    for (Multimap<DimensionKey, Future<MetricTimeSeries>> resultMap : timeSeriesFutures.values()) {
      for (Map.Entry<DimensionKey, Collection<Future<MetricTimeSeries>>> entry : resultMap.asMap().entrySet()) {
        for (Future<MetricTimeSeries> seriesFuture : entry.getValue()) {
          MetricTimeSeries additionalSeries = seriesFuture.get();
          MetricTimeSeries currentSeries = mergedResults.get(entry.getKey());
          if (currentSeries == null) {
            currentSeries = new MetricTimeSeries(additionalSeries.getSchema());
            mergedResults.put(entry.getKey(), currentSeries);
          }
          currentSeries.aggregate(additionalSeries);
        }
      }
    }

    // Aggregate across all trees and apply functions
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : mergedResults.entrySet()) {
      MetricTimeSeries timeSeries = entry.getValue();
      // Compute aggregate functions
      for (ThirdEyeFunction function : query.getFunctions()) {
        timeSeries = function.apply(config, query, timeSeries);
      }
      // Add derived metrics
      for (ThirdEyeFunction function : query.getDerivedMetrics()) {
        timeSeries = function.apply(config, query, timeSeries);
      }
      // Convert to milliseconds
      timeSeries = TO_MILLIS.apply(config, query, timeSeries);
      result.addData(entry.getKey(), timeSeries);
      result.setMetrics(timeSeries.getSchema().getNames()); // multiple calls should be idempotent
    }
    LOGGER.info("END Execution for query_id: {} ", query.hashCode());

    return result;
  }

  /**
   * Selects the trees whose data times are non-disjoint with the query time range.
   *
   * <p>
   *   This uses a greedy selection algorithm, in which we prefer trees with a
   *   more coarse granularity (e.g. MONTHLY over HOURLY)
   * </p>
   */
  public List<UUID> selectTreesToQuery(final Map<UUID, IndexMetadata> treeMetadataMap,
                                       final TimeRange queryTimeRange) {
    List<UUID> treeIds = new ArrayList<>();
    // Determine which trees we need to query
    for (UUID treeId : treeMetadataMap.keySet()) {
      IndexMetadata indexMetadata = treeMetadataMap.get(treeId);
      TimeRange treeTimeRange =
          new TimeRange(indexMetadata.getMinDataTimeMillis(), indexMetadata.getMaxDataTimeMillis());
      if (!queryTimeRange.isDisjoint(treeTimeRange)) {
        treeIds.add(treeId);
      }
    }
    Comparator<? super UUID> comparator = new Comparator<UUID>() {
      @Override
      public int compare(UUID treeId1, UUID treeId2) {
        IndexMetadata indexMetadata1 = treeMetadataMap.get(treeId1);
        IndexMetadata indexMetadata2 = treeMetadataMap.get(treeId2);
        Long startTime1 = indexMetadata1.getStartTimeMillis();
        Long startTime2 = indexMetadata2.getStartTimeMillis();
        int ret = startTime1.compareTo(startTime2);
        if (ret == 0) {
          Integer timeGranularity1 =
              timeGranularitySortOrder.get(indexMetadata1.getTimeGranularity().toUpperCase());
          Integer timeGranularity2 =
              timeGranularitySortOrder.get(indexMetadata2.getTimeGranularity().toUpperCase());
          ret = timeGranularity1.compareTo(timeGranularity2);
        }
        return ret;
      }
    };

    // We will have segments at multiple granularities hourly, daily, weekly, monthly.
    // Find the minimum number of trees to query
    // We use a greedy algorithm that sorts the tree with startTime, Granularity (monthly appears
    // first followed by weekly daily and hourly)
    Collections.sort(treeIds, comparator);

    List<UUID> treeIdsToQuery = new ArrayList<>();

    // Select the disjointed trees after filtering
    TimeRange lastSelectedRange = null;
    for (UUID treeId : treeIds) {
      IndexMetadata indexMetadata = treeMetadataMap.get(treeId);
      TimeRange treeRange = new TimeRange(indexMetadata.getStartTimeMillis(), indexMetadata.getEndTimeMillis());
      if (lastSelectedRange == null || lastSelectedRange.getEnd() <= treeRange.getStart()) { // range end is exclusive for tree
        lastSelectedRange = treeRange;
        treeIdsToQuery.add(treeId);
        LOGGER.info("Selecting treeId:{} with TimeRange:{}", treeId, treeRange);
      }
    }

    return treeIdsToQuery;
  }

  private static long dateTimeToCollectionTime(StarTreeConfig config, DateTime dateTime) {
    TimeGranularity bucket = config.getTime().getBucket();
    return bucket.getUnit().convert(dateTime.getMillis(), TimeUnit.MILLISECONDS) / bucket.getSize();
  }

  /** Replaces dimensions in all queries involving disjunctions such that they can be grouped */
  private static DimensionKey flattenDisjunctions(StarTreeConfig config,
                                                  ThirdEyeQuery query,
                                                  DimensionKey dimensionKey) {
    String[] flattenedValues = new String[config.getDimensions().size()];

    for (int i = 0; i < config.getDimensions().size(); i++) {
      String dimensionName = config.getDimensions().get(i).getName();
      String dimensionValue = dimensionKey.getDimensionValues()[i];

      Collection queryValues = query.getDimensionValues().get(dimensionName);
      if (!query.getGroupByColumns().contains(dimensionName) && queryValues.size() > 1) {
        dimensionValue = OR_JOINER.join(queryValues);
      }

      flattenedValues[i] = dimensionValue;
    }

    return new DimensionKey(flattenedValues);
  }
}
