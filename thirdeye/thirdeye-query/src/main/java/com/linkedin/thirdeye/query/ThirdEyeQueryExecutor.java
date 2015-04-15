package com.linkedin.thirdeye.query;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.*;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
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

  public ThirdEyeQueryExecutor(ExecutorService executorService, StarTreeManager starTreeManager) {
    this.executorService = executorService;
    this.starTreeManager = starTreeManager;
  }

  public ThirdEyeQueryResult executeQuery(String sql) throws Exception {
    return executeQuery(new ThirdEyeQueryParser(sql).getQuery());
  }

  public ThirdEyeQueryResult executeQuery(ThirdEyeQuery query) throws Exception {
    ThirdEyeQueryResult result = new ThirdEyeQueryResult();

    final StarTreeConfig config = starTreeManager.getConfig(query.getCollection());
    if (config == null) {
      throw new IllegalArgumentException("No collection " + query.getCollection());
    }
    result.setMetrics(query.getMetricNames());
    List<String> dimensionNames = new ArrayList<>(config.getDimensions().size());
    for (DimensionSpec dimensionSpec : config.getDimensions()) {
      dimensionNames.add(dimensionSpec.getName());
    }
    result.setDimensions(dimensionNames);

    // Time
    final TimeRange timeRange = new TimeRange(
        dateTimeToCollectionTime(config, query.getStart()),
        dateTimeToCollectionTime(config, query.getEnd()) - 1); // time from query is exclusive

    // For all group by dimensions add those as fixed
    if (!query.getGroupByColumns().isEmpty()) {
      for (final String groupByColumn : query.getGroupByColumns()) {
        if (query.getDimensionValues().containsKey(groupByColumn)) {
          throw new IllegalArgumentException("Cannot fix dimension value in group by: " + groupByColumn);
        }

        final Set<Future<Set<String>>> dimensionSetFutures = new HashSet<>();
        for (final StarTree starTree : starTreeManager.getStarTrees(config.getCollection()).values()) {
          dimensionSetFutures.add(executorService.submit(new Callable<Set<String>>() {
            @Override
            public Set<String> call() throws Exception {
              Set<String> collector = new HashSet<>();
              getGroupByValues(starTree.getRoot(), groupByColumn, null, collector);
              return collector;
            }
          }));
        }

        Set<String> dimensionSet = new HashSet<>();
        for (Future<Set<String>> future : dimensionSetFutures) {
          dimensionSet.addAll(future.get());
        }

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
      if (timeRange.isDisjoint(fromStats(starTree.getStats()))) {
        continue;
      }

      timeSeriesFutures.put(starTree, new HashMap<DimensionKey, Future<MetricTimeSeries>>());
      for (final DimensionKey dimensionKey : dimensionKeys) {
        timeSeriesFutures.get(starTree).put(dimensionKey, executorService.submit(new Callable<MetricTimeSeries>() {
          @Override
          public MetricTimeSeries call() throws Exception {
            return starTree.getTimeSeries(new StarTreeQueryImpl(config, dimensionKey, timeRange));
          }
        }));
      }
    }

    // Aggregate across all trees and apply functions
    for (Map<DimensionKey, Future<MetricTimeSeries>> treeResult : timeSeriesFutures.values()) {
      for (Map.Entry<DimensionKey, Future<MetricTimeSeries>> entry : treeResult.entrySet()) {
        MetricTimeSeries timeSeries = entry.getValue().get();
        for (ThirdEyeFunction function : query.getFunctions()) {
          timeSeries = function.apply(config, timeSeries);
        }
        timeSeries = TO_MILLIS.apply(config, timeSeries);
        result.addData(entry.getKey(), timeSeries);
      }
    }

    return result;
  }

  private static void getGroupByValues(StarTreeNode node,
                                       String groupByDimension,
                                       Multimap<String, String> dimensionValues,
                                       Set<String> collector) {
    if (node.isLeaf()) {
      // Just find all at this leaf
      collector.addAll(node.getRecordStore().getDimensionValues(groupByDimension));
    } else if (node.getChildDimensionName().equals(groupByDimension)) {
      // Get all the children (we are done here, since these are all that's fixed)
      for (StarTreeNode child : node.getChildren()) {
        collector.add(child.getDimensionValue());
      }
      collector.add(StarTreeConstants.OTHER);
    } else if (dimensionValues == null || dimensionValues.get(node.getChildDimensionName()) == null) {
      // Traverse to star node (not fixed)
      getGroupByValues(node.getStarNode(), groupByDimension, dimensionValues, collector);
    } else {
      // Traverse
      boolean includeOther = false;
      Collection<String> values = dimensionValues.get(node.getChildDimensionName());
      for (String value : values) {
        StarTreeNode child = node.getChild(value);
        if (child == null) {
          includeOther = true;
        } else {
          getGroupByValues(child, groupByDimension, dimensionValues, collector);
        }
      }
      if (includeOther) {
        getGroupByValues(node.getOtherNode(), groupByDimension, dimensionValues, collector);
      }
    }
  }

  private static long dateTimeToCollectionTime(StarTreeConfig config, DateTime dateTime) {
    TimeGranularity bucket = config.getTime().getBucket();
    return bucket.getUnit().convert(dateTime.getMillis(), TimeUnit.MILLISECONDS) / bucket.getSize();
  }

  private static TimeRange fromStats(StarTreeStats starTreeStats) {
    return new TimeRange(starTreeStats.getMinTime(), starTreeStats.getMaxTime());
  }
}
