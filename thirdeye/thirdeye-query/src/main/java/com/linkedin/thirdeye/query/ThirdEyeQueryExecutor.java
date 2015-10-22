package com.linkedin.thirdeye.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeQueryImpl;
import com.linkedin.thirdeye.impl.storage.IndexMetadata;

public class ThirdEyeQueryExecutor {
  private static final String UNKNOWN_DIMENSION_VALUE = "";
  private static final String OTHER_DIMENSION_VALUE = "?";
  private static final Logger LOGGER = LoggerFactory.getLogger(ThirdEyeQueryExecutor.class);
  private static final Joiner OR_JOINER = Joiner.on(" OR ");

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
        ThirdEyeMovingAverageFunction movingAverageFunction =
            (ThirdEyeMovingAverageFunction) function;
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

    TreeSelections treeSelections = calculateTreeSelections(config, query.getStart(),
        query.getEnd(), collectionWindowMillis, startOffset);
    final Map<UUID, TimeRange> timeRangesToQuery = treeSelections.getTimeRangesToQuery();
    final List<StarTree> starTrees = getStarTrees(config, treeSelections.getAllTreeIdsToQuery());

    // For all group by dimensions add those as fixed
    if (!query.getGroupByColumns().isEmpty()) {
      for (final String groupByColumn : query.getGroupByColumns()) {
        if (query.getDimensionValues().containsKey(groupByColumn)) {
          throw new IllegalArgumentException(
              "Cannot fix dimension value in group by: " + groupByColumn);
        }

        final Set<Future<Set<String>>> dimensionSetFutures = new HashSet<>();

        for (final StarTree starTree : starTrees) {

          dimensionSetFutures.add(executorService.submit(new Callable<Set<String>>() {
            @Override
            public Set<String> call() throws Exception {
              return starTree.getDimensionValues(groupByColumn, query.getDimensionValues().asMap());
            }
          }));
        }

        Set<String> dimensionSet = new HashSet<>();
        for (Future<Set<String>> future : dimensionSetFutures) {
          dimensionSet.addAll(future.get());
        }
        dimensionSet.remove(StarTreeConstants.STAR); // never represent this one

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
    Map<StarTree, Multimap<DimensionKey, Future<MetricTimeSeries>>> timeSeriesFutures =
        new HashMap<>();

    for (final StarTree starTree : starTrees) {
      final UUID treeId = starTree.getRoot().getId();

      Multimap<DimensionKey, Future<MetricTimeSeries>> singleKeyResultMap =
          LinkedListMultimap.create();
      timeSeriesFutures.put(starTree, singleKeyResultMap);
      for (final DimensionKey dimensionKey : dimensionKeys) {
        DimensionKey flattenedKey = flattenDisjunctions(config, query, dimensionKey);
        timeSeriesFutures.get(starTree).put(flattenedKey,
            executorService.submit(new Callable<MetricTimeSeries>() {

              @Override
              public MetricTimeSeries call() throws Exception {
                TimeRange timeRange = timeRangesToQuery.get(treeId);
                return starTree
                    .getTimeSeries(new StarTreeQueryImpl(config, dimensionKey, timeRange));
              }
            }));
      }
    }

    // Merge results
    Map<DimensionKey, MetricTimeSeries> mergedResults = new HashMap<>();
    for (Multimap<DimensionKey, Future<MetricTimeSeries>> resultMap : timeSeriesFutures.values())

    {
      for (Map.Entry<DimensionKey, Collection<Future<MetricTimeSeries>>> entry : resultMap.asMap()
          .entrySet()) {
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
      // Retain only the specified metrics
      List<String> resultMetrics = new ArrayList<>();
      resultMetrics.addAll(query.getMetricNames());
      for (ThirdEyeFunction function : query.getDerivedMetrics()) {
        resultMetrics.add(function.toString()); // e.g. RATIO(A,B)
      }

      // Convert to milliseconds
      ThirdEyeFunction toMillis =
          new ThirdEyeUnitConversionFunction(1, TimeUnit.MILLISECONDS, resultMetrics);
      timeSeries = toMillis.apply(config, query, timeSeries);
      result.addData(entry.getKey(), timeSeries);
      result.setMetrics(timeSeries.getSchema().getNames()); // multiple calls should be idempotent
    }
    LOGGER.info("END Execution for query_id: {} ", query.hashCode());

    return result;

  }

  /**
   * Returns sorted list of all observed dimension values for each unfixed dimension in the provided
   * query range, using the provided fixed dimension values. UNKNOWN("") and OTHER("?") dimension
   * values
   * will appear at the end of the collection if present.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public Map<String, Collection<String>> getAllDimensionValues(final String collection,
      final DateTime queryStart, final DateTime queryEnd,
      final Map<String, Collection<String>> fixedDimensions)
          throws InterruptedException, ExecutionException {
    final StarTreeConfig config = starTreeManager.getConfig(collection);

    if (config == null) {
      throw new IllegalArgumentException("No collection " + collection);
    }

    final TreeSelections treeSelections = calculateTreeSelections(config, queryStart, queryEnd);
    final List<StarTree> starTrees = getStarTrees(config, treeSelections.getAllTreeIdsToQuery());

    Map<String, Collection<String>> dimensionValues = new HashMap<>(config.getDimensions().size());
    for (DimensionSpec dimensionSpec : config.getDimensions()) {
      final String dimensionName = dimensionSpec.getName();
      if (fixedDimensions.containsKey(dimensionName)) {
        // dimension already fixed, no need to explore further.
        continue;
      }
      // TODO trying to submit the getDimensionValues calls via executorService/Futures results in
      // hanging?
      dimensionValues.put(dimensionName,
          getDimensionValues(dimensionName, fixedDimensions, starTrees));
    }

    return dimensionValues;
  }

  /**
   * Returns sorted list of all observed dimension values for the given dimension
   * from the provided StarTrees, using the provided fixed dimension values.
   * UNKNOWN("") and OTHER("?") dimension values will appear at the end of the
   * collection if present.
   * @throws ExecutionException
   * @throws InterruptedException
   */
  private List<String> getDimensionValues(final String dimension,
      final Map<String, Collection<String>> fixedDimensions, List<StarTree> starTrees)
          throws InterruptedException, ExecutionException {

    List<Future<Set<String>>> dimensionValuesFutures = new LinkedList<>();

    for (final StarTree starTree : starTrees) {
      Future<Set<String>> future = executorService.submit(new Callable<Set<String>>() {

        @Override
        public Set<String> call() throws Exception {
          return starTree.getDimensionValues(dimension, fixedDimensions);
        }
      });
      dimensionValuesFutures.add(future);
    }

    Set<String> dimensionValues = new HashSet<>();
    for (Future<Set<String>> future : dimensionValuesFutures) {
      Set<String> values = future.get();
      dimensionValues.addAll(values);
    }

    boolean hasOther = dimensionValues.remove(OTHER_DIMENSION_VALUE);
    boolean hasUnknown = dimensionValues.remove(UNKNOWN_DIMENSION_VALUE);
    List<String> sortedValues = new ArrayList<>(dimensionValues);
    Collections.sort(sortedValues);
    if (hasOther) {
      sortedValues.add(OTHER_DIMENSION_VALUE);
    }
    if (hasUnknown) {
      dimensionValues.add(UNKNOWN_DIMENSION_VALUE);
    }
    return sortedValues;
  }

  /**
   * Selects the trees whose data times are non-disjoint with the query time range.
   * <p>
   * This uses a greedy selection algorithm, in which we prefer trees with a
   * more coarse granularity (e.g. MONTHLY over HOURLY)
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
      TimeRange wallClockRange =
          new TimeRange(indexMetadata.getStartTimeMillis(), indexMetadata.getEndTimeMillis());
      if ((!queryTimeRange.isDisjoint(treeTimeRange) || !queryTimeRange.isDisjoint(wallClockRange))
          && !treeIds.contains(treeId)) {
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
      TimeRange treeRange =
          new TimeRange(indexMetadata.getStartTimeMillis(), indexMetadata.getEndTimeMillis());
      if (lastSelectedRange == null || lastSelectedRange.getEnd() <= treeRange.getStart()) { // range
                                                                                             // end
                                                                                             // is
                                                                                             // exclusive
                                                                                             // for
                                                                                             // tree
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
  private static DimensionKey flattenDisjunctions(StarTreeConfig config, ThirdEyeQuery query,
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

  private TreeSelections calculateTreeSelections(StarTreeConfig config, DateTime queryStart,
      DateTime queryEnd) {
    return calculateTreeSelections(config, queryStart, queryEnd, 0, 0);
  }

  private TreeSelections calculateTreeSelections(StarTreeConfig config, DateTime queryStart,
      DateTime queryEnd, long collectionWindowMillis, long startOffset) {

    // Time
    final DateTime queryStartInMillis = new DateTime(queryStart.getMillis() - startOffset);

    final TimeRange inputQueryTimeRange =
        new TimeRange(queryStartInMillis.getMillis(), queryEnd.getMillis());

    long queryStartTime = dateTimeToCollectionTime(config, queryStartInMillis);
    long queryEndTime = dateTimeToCollectionTime(config, queryEnd);

    final Pair<Map<UUID, IndexMetadata>, Map<UUID, IndexMetadata>> metadataMaps =
        getMetadataMaps(config);
    final Map<UUID, IndexMetadata> immutableMetadataMap = metadataMaps.getFirst();
    final Map<UUID, IndexMetadata> realTimeMetadataMap = metadataMaps.getSecond();
    LOGGER.info("Selecting trees to query for queryTimeRange:{}", inputQueryTimeRange);
    List<UUID> immutableTreeIdsToQuery =
        selectTreesToQuery(immutableMetadataMap, inputQueryTimeRange);

    // Align to aggregation boundary
    if (collectionWindowMillis > 0) {
      long collectionWindow =
          dateTimeToCollectionTime(config, new DateTime(collectionWindowMillis));
      queryStartTime = (queryStartTime / collectionWindow) * collectionWindow;
      queryEndTime = (queryEndTime / collectionWindow + 1) * collectionWindow; // include everything
                                                                               // in that window
    }
    final TimeRange queryTimeRange = new TimeRange(queryStartTime, queryEndTime);

    // Time ranges that should be queried for each tree
    final Map<UUID, TimeRange> timeRangesToQuery = new HashMap<>();
    for (UUID treeId : immutableTreeIdsToQuery) {
      // Whole query time for each of the immutable trees
      timeRangesToQuery.put(treeId, queryTimeRange);
    }

    // Determine max data time from the most recent tree that's being queried from immutable
    // segments
    Long maxImmutableTimeMillis = null;
    for (UUID treeId : immutableTreeIdsToQuery) {
      IndexMetadata indexMetadata = immutableMetadataMap.get(treeId);
      if (maxImmutableTimeMillis == null
          || maxImmutableTimeMillis < indexMetadata.getMaxDataTimeMillis()) {
        maxImmutableTimeMillis = indexMetadata.getMaxDataTimeMillis();
      }
    }

    // Get the starting collection time we should use for real-time segments
    Long realTimeStartTime = null;
    Long realTimeStartTimeMillis = null;
    if (maxImmutableTimeMillis == null) {
      realTimeStartTime = queryTimeRange.getStart();
      realTimeStartTimeMillis = queryStartInMillis.getMillis();
    } else if (maxImmutableTimeMillis < queryEnd.getMillis()) {
      realTimeStartTime = dateTimeToCollectionTime(config, new DateTime(maxImmutableTimeMillis));
      realTimeStartTimeMillis = maxImmutableTimeMillis;
      long collectionWindow =
          dateTimeToCollectionTime(config, new DateTime(collectionWindowMillis));
      if (collectionWindow > 0) {
        realTimeStartTime = (realTimeStartTime / collectionWindow) * collectionWindow;
      }
    }

    // Get the real time trees we need to query
    final List<UUID> realTimeTreeIdsToQuery = new ArrayList<>();
    if (realTimeStartTime != null) {
      TimeRange mutableTimeRange = new TimeRange(realTimeStartTime, queryTimeRange.getEnd());
      TimeRange mutableTimeRangeMillis =
          new TimeRange(realTimeStartTimeMillis, queryEnd.getMillis());
      realTimeTreeIdsToQuery
          .addAll(selectTreesToQuery(realTimeMetadataMap, mutableTimeRangeMillis));

      // Also add in-memory tree (though it may not match)
      StarTree mutableTree = starTreeManager.getMutableStarTree(config.getCollection());
      realTimeTreeIdsToQuery.add(mutableTree.getRoot().getId());

      // Only query the back-end of the time range for these trees
      for (UUID treeId : realTimeTreeIdsToQuery) {
        timeRangesToQuery.put(treeId, mutableTimeRange);
      }
    }
    return new TreeSelections(immutableTreeIdsToQuery, realTimeTreeIdsToQuery, timeRangesToQuery);
  }

  /**
   * Populates the input maps with the appropriate treeId -> indexMetadata mappings for all
   * StarTrees associated with the collection specified in the config.
   * @param config
   * @param immutableMetadataMap
   * @param realTimeMetadataMap
   */
  private Pair<Map<UUID, IndexMetadata>, Map<UUID, IndexMetadata>> getMetadataMaps(
      StarTreeConfig config) {
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
    return new Pair<Map<UUID, IndexMetadata>, Map<UUID, IndexMetadata>>(immutableMetadataMap,
        realTimeMetadataMap);
  }

  private List<StarTree> getStarTrees(StarTreeConfig config, List<UUID> ids) {
    final List<StarTree> starTrees =
        new ArrayList<>(starTreeManager.getStarTrees(config.getCollection()).values());
    final StarTree mutableTree = starTreeManager.getMutableStarTree(config.getCollection());
    if (mutableTree != null) {
      starTrees.add(mutableTree);
    }
    List<StarTree> results = new LinkedList<>();
    for (final StarTree starTree : starTrees) {
      UUID treeId = starTree.getRoot().getId();
      if (ids.contains(treeId)) {
        results.add(starTree);
      }
    }
    return results;
  }

  private class TreeSelections {
    private final List<UUID> immutableTreeIdsToQuery;
    private final List<UUID> realTimeTreeIdsToQuery;
    private final Map<UUID, TimeRange> timeRangesToQuery;

    public TreeSelections(List<UUID> immutableTreeIdsToQuery, List<UUID> realTimeTreeIdsToQuery,
        Map<UUID, TimeRange> timeRangesToQuery) {
      this.immutableTreeIdsToQuery = immutableTreeIdsToQuery;
      this.realTimeTreeIdsToQuery = realTimeTreeIdsToQuery;
      this.timeRangesToQuery = timeRangesToQuery;
    }

    public List<UUID> getImmutableTreeIdsToQuery() {
      return immutableTreeIdsToQuery;
    }

    public List<UUID> getRealTimeTreeIdsToQuery() {
      return realTimeTreeIdsToQuery;
    }

    public List<UUID> getAllTreeIdsToQuery() {
      final List<UUID> allTreeIdsToQuery = new ArrayList<>(immutableTreeIdsToQuery);
      allTreeIdsToQuery.addAll(realTimeTreeIdsToQuery);
      return allTreeIdsToQuery;
    }

    public Map<UUID, TimeRange> getTimeRangesToQuery() {
      return timeRangesToQuery;
    }
  }
}
