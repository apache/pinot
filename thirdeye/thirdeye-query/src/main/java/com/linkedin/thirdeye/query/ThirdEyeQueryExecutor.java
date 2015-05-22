package com.linkedin.thirdeye.query;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import com.linkedin.thirdeye.impl.storage.IndexMetadata;

import org.joda.time.DateTime;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ThirdEyeQueryExecutor {
  private static final ThirdEyeFunction TO_MILLIS = new ThirdEyeUnitConversionFunction(1, TimeUnit.MILLISECONDS);

  private final ExecutorService executorService;
  private final StarTreeManager starTreeManager;
  private static Map<String, Integer> timeGranularitySortOrder = new HashMap<String, Integer>();
  static {
    timeGranularitySortOrder.put("monthly", 0);
    timeGranularitySortOrder.put("weekly", 1);
    timeGranularitySortOrder.put("daily", 2);
    timeGranularitySortOrder.put("hourly", 3);
  }
  public ThirdEyeQueryExecutor(ExecutorService executorService, StarTreeManager starTreeManager) {
    this.executorService = executorService;
    this.starTreeManager = starTreeManager;
  }

  public ThirdEyeQueryResult executeQuery(String sql) throws Exception {
    return executeQuery(new ThirdEyeQueryParser(sql).getQuery());
  }

  public ThirdEyeQueryResult executeQuery(final ThirdEyeQuery query) throws Exception {
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
    long queryStartTime = dateTimeToCollectionTime(config, new DateTime(query.getStart().getMillis() - startOffset));
    long queryEndTime = dateTimeToCollectionTime(config, query.getEnd());

    // Align to aggregation boundary
    if (collectionWindowMillis > 0) {
      long collectionWindow = dateTimeToCollectionTime(config, new DateTime(collectionWindowMillis));
      queryStartTime = (queryStartTime / collectionWindow) * collectionWindow;
      queryEndTime = (queryEndTime / collectionWindow + 1) * collectionWindow; // include everything in that window
    }
    
    final TimeRange queryTimeRange = new TimeRange(queryStartTime, queryEndTime);
    List<UUID> treeIdsToQuery = selectTreesToQuery(config, queryTimeRange);

    // For all group by dimensions add those as fixed
    if (!query.getGroupByColumns().isEmpty()) {
      for (final String groupByColumn : query.getGroupByColumns()) {
        if (query.getDimensionValues().containsKey(groupByColumn)) {
          throw new IllegalArgumentException("Cannot fix dimension value in group by: " + groupByColumn);
        }

        final Set<Future<Set<String>>> dimensionSetFutures = new HashSet<>();
        for (final StarTree starTree : starTreeManager.getStarTrees(config.getCollection()).values()) {
          if (!treeIdsToQuery.contains(starTree.getRoot().getId())) {
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
    Map<StarTree, Map<DimensionKey, Future<MetricTimeSeries>>> timeSeriesFutures = new HashMap<>();
    for (final StarTree starTree : starTreeManager.getStarTrees(config.getCollection()).values()) {
      if (!treeIdsToQuery.contains(starTree.getRoot().getId())) {
        continue;
      }

      timeSeriesFutures.put(starTree, new HashMap<DimensionKey, Future<MetricTimeSeries>>());
      for (final DimensionKey dimensionKey : dimensionKeys) {
        timeSeriesFutures.get(starTree).put(dimensionKey, executorService.submit(new Callable<MetricTimeSeries>() {
          @Override
          public MetricTimeSeries call() throws Exception {
            return starTree.getTimeSeries(new StarTreeQueryImpl(config, dimensionKey, queryTimeRange));
          }
        }));
      }
    }

    // Merge results
    Map<DimensionKey, MetricTimeSeries> mergedResults = new HashMap<>();
    for (Map<DimensionKey, Future<MetricTimeSeries>> resultMap : timeSeriesFutures.values()) {
      for (Map.Entry<DimensionKey, Future<MetricTimeSeries>> entry : resultMap.entrySet()) {
        MetricTimeSeries additionalSeries = entry.getValue().get();
        MetricTimeSeries currentSeries = mergedResults.get(entry.getKey());
        if (currentSeries == null) {
          currentSeries = new MetricTimeSeries(additionalSeries.getSchema());
          mergedResults.put(entry.getKey(), currentSeries);
        }
        currentSeries.aggregate(additionalSeries);
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

    return result;
  }

  private List<UUID> selectTreesToQuery(StarTreeConfig config, final TimeRange queryTimeRange) {
    long queryStartTime = queryTimeRange.getStart();
    long queryEndTime = queryTimeRange.getEnd();

    List<UUID> treeIds = new ArrayList<>();
    // Determine which trees we need to query
    for (StarTree starTree : starTreeManager.getStarTrees(config.getCollection()).values()) {
      IndexMetadata indexMetadata = starTreeManager.getIndexMetadata(starTree.getRoot().getId());
      TimeRange treeTimeRange =
          new TimeRange(indexMetadata.getMinDataTime(), indexMetadata.getMaxDataTime());
      if (!queryTimeRange.isDisjoint(treeTimeRange)) {
        treeIds.add(starTree.getRoot().getId());
      }
    }

    Comparator<? super UUID> comparator = new Comparator<UUID>() {
      @Override
      public int compare(UUID treeId1, UUID treeId2) {
        IndexMetadata indexMetadata1 = starTreeManager.getIndexMetadata(treeId1);
        IndexMetadata indexMetadata2 = starTreeManager.getIndexMetadata(treeId1);
        Long startTime1 = indexMetadata1.getStartTime();
        Long startTime2 = indexMetadata2.getStartTime();
        int ret = startTime1.compareTo(startTime2);
        if (ret == 0) {
          Integer timeGranularity1 =
              timeGranularitySortOrder.get(indexMetadata1.getTimeGranularity());
          Integer timeGranularity2 =
              timeGranularitySortOrder.get(indexMetadata2.getTimeGranularity());
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
    TimeRange remainingTimeRange = new TimeRange(queryStartTime, queryEndTime);
    List<UUID> treeIdsToQuery = new ArrayList<>();

    for (UUID treeId : treeIds) {
      IndexMetadata indexMetadata = starTreeManager.getIndexMetadata(treeId);
      long startTime = indexMetadata.getStartTime();
      long endTime = indexMetadata.getStartTime();
      TimeRange treeTimeRange = new TimeRange(startTime, endTime);
      if (treeTimeRange.isWithIn(remainingTimeRange)) {
        treeIdsToQuery.add(treeId);
        // update the remaining Time Range
        remainingTimeRange = new TimeRange(endTime, queryEndTime);
        // if we have reached the queryEndTime, break out of the loop
        if (endTime == queryEndTime) {
          break;
        }
      }
    }
    return treeIdsToQuery;
  }

  private static long dateTimeToCollectionTime(StarTreeConfig config, DateTime dateTime) {
    TimeGranularity bucket = config.getTime().getBucket();
    return bucket.getUnit().convert(dateTime.getMillis(), TimeUnit.MILLISECONDS) / bucket.getSize();
  }

  private static TimeRange fromStats(StarTreeStats starTreeStats) {
    return new TimeRange(starTreeStats.getMinTime(), starTreeStats.getMaxTime());
  }
}
