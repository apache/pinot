package com.linkedin.thirdeye.datasource.pinot;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponse;
import com.linkedin.thirdeye.datasource.TimeRangeUtils;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeDataFrameResultSet;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetMetaData;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotThirdEyeDataSource implements ThirdEyeDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(PinotThirdEyeDataSource.class);
  private static final ThirdEyeCacheRegistry CACHE_REGISTRY_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  public static final String DATA_SOURCE_NAME = PinotThirdEyeDataSource.class.getSimpleName();

  public static final String CACHE_LOADER_CLASS_NAME_STRING = "cacheLoaderClassName";
  // TODO: make default cache size configurable
  private static final int DEFAULT_HEAP_PERCENTAGE_FOR_RESULTSETGROUP_CACHE = 50;
  private static final int DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB = 100;
  private static final int DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB = 8192;
  protected LoadingCache<PinotQuery, ThirdEyeResultSetGroup> pinotResponseCache;

  protected PinotDataSourceMaxTime pinotDataSourceMaxTime;
  protected PinotDataSourceDimensionFilters pinotDataSourceDimensionFilters;

  /**
   * Construct a Pinot data source, which connects to a Pinot controller, using {@link PinotThirdEyeDataSourceConfig}.
   *
   * @param pinotThirdEyeDataSourceConfig the configuration that provides the information of the Pinot controller.
   *
   * @throws Exception when failed to connect to the controller.
   */
  public PinotThirdEyeDataSource(PinotThirdEyeDataSourceConfig pinotThirdEyeDataSourceConfig) throws Exception {
    PinotResponseCacheLoader pinotResponseCacheLoader = new PinotControllerResponseCacheLoader(pinotThirdEyeDataSourceConfig);
    pinotResponseCache = buildResponseCache(pinotResponseCacheLoader);

    pinotDataSourceMaxTime = new PinotDataSourceMaxTime(this);
    pinotDataSourceDimensionFilters = new PinotDataSourceDimensionFilters(this);
  }


  /**
   * This constructor is invoked by Java Reflection for initialize a ThirdEyeDataSource.
   *
   * @param properties the property to initialize this data source.
   */
  public PinotThirdEyeDataSource(Map<String, String> properties) throws Exception {
    Preconditions.checkNotNull(properties, "Data source property cannot be empty.");

    PinotResponseCacheLoader pinotResponseCacheLoader = getCacheLoaderInstance(properties);
    pinotResponseCacheLoader.init(properties);
    pinotResponseCache = buildResponseCache(pinotResponseCacheLoader);

    pinotDataSourceMaxTime = new PinotDataSourceMaxTime(this);
    pinotDataSourceDimensionFilters = new PinotDataSourceDimensionFilters(this);
  }

  /**
   * Constructs a PinotResponseCacheLoader from the given property map and initialize the loader with that map.
   *
   * @param properties the property map of the cache loader, which contains the class path of the cache loader.
   *
   * @return a constructed PinotResponseCacheLoader.
   *
   * @throws Exception when an error occurs connecting to the Pinot controller.
   */
  static PinotResponseCacheLoader getCacheLoaderInstance(Map<String, String> properties)
      throws Exception {
    final String cacheLoaderClassName;
    if (properties.containsKey(CACHE_LOADER_CLASS_NAME_STRING)) {
      cacheLoaderClassName = properties.get(CACHE_LOADER_CLASS_NAME_STRING);
    } else {
      cacheLoaderClassName = PinotControllerResponseCacheLoader.class.getName();
    }
    LOG.info("Constructing cache loader: {}", cacheLoaderClassName);
    Class<?> aClass = null;
    try {
      aClass = Class.forName(cacheLoaderClassName);
    } catch (Throwable throwable) {
      LOG.error("Failed to initiate cache loader: {}; reason:", cacheLoaderClassName, throwable);
      aClass = PinotControllerResponseCacheLoader.class;
    }
    LOG.info("Initiating cache loader: {}", aClass.getName());
    Constructor<?> constructor = aClass.getConstructor();
    PinotResponseCacheLoader pinotResponseCacheLoader = (PinotResponseCacheLoader) constructor.newInstance();
    return pinotResponseCacheLoader;
  }

  @Override
  public String getName() {
    return DATA_SOURCE_NAME;
  }

  @Override
  public ThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    Preconditions
        .checkNotNull(this.pinotResponseCache, "{} doesn't connect to Pinot or cache is not initialized.", getName());

    long tStart = System.nanoTime();
    try {
      LinkedHashMap<MetricFunction, List<ThirdEyeResultSet>> metricFunctionToResultSetList = new LinkedHashMap<>();

      // TODO: Change this to consider multiple data time spec from different datasets.
      // We assume that every dataset has the same data time spec for now.
      TimeSpec dataTimeSpec = null;
      for (MetricFunction metricFunction : request.getMetricFunctions()) {
        String dataset = metricFunction.getDataset();
        DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
        if (dataTimeSpec == null) {
          dataTimeSpec = ThirdEyeUtils.getTimeSpecFromDatasetConfig(datasetConfig);
        }

        Multimap<String, String> decoratedFilterSet = request.getFilterSet();
        // Decorate filter set for pre-computed (non-additive) dataset
        // NOTE: We do not decorate the filter if the metric name is '*', which is used by count(*) query, because
        // the results are usually meta-data and should be shown regardless the filter setting.
        if (!datasetConfig.isAdditive() && !"*".equals(metricFunction.getMetricName())) {
          decoratedFilterSet =
              generateFilterSetWithPreAggregatedDimensionValue(request.getFilterSet(), request.getGroupBy(),
                  datasetConfig.getDimensions(), datasetConfig.getDimensionsHaveNoPreAggregation(),
                  datasetConfig.getPreAggregatedKeyword());
        }

        // By default, query only offline, unless dataset has been marked as realtime
        TimeSpec timestampTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
        MetricConfigDTO metricConfig = metricFunction.getMetricConfig();
        String pql = null;
        if (metricConfig != null && metricConfig.isDimensionAsMetric()) {
          pql = PqlUtils
              .getDimensionAsMetricPql(request, metricFunction, decoratedFilterSet, timestampTimeSpec, datasetConfig);
        } else {
          pql = PqlUtils.getPql(request, metricFunction, decoratedFilterSet, timestampTimeSpec);
        }
        String tableName = ThirdEyeUtils.computeTableName(dataset);
        ThirdEyeResultSetGroup resultSetGroup = this.executePQL(new PinotQuery(pql, tableName));
        // Makes a copy for each DataFrameResultSet to prevent any change of the data frame refexes to the loading cache.
        resultSetGroup = copyDataFrameResultSets(resultSetGroup);
        // Create timestamp column to ms
        resultSetGroup = convertTimestampToMS(resultSetGroup, timestampTimeSpec);
        metricFunctionToResultSetList.put(metricFunction, resultSetGroup.getResultSets());
      }
      // TODO: Roll up the data from different datasets to the same time granularity before join them
      // Join the data from different result sets and datasets
      ThirdEyeDataFrameResultSet joinedResultSet = joinResultSets(metricFunctionToResultSetList);
      // TODO: Evaluate expression before return the response
      TimeSpec convertedTimeSpec =
          new TimeSpec(dataTimeSpec.getColumnName(), dataTimeSpec.getDataGranularity(), TimeSpec.SINCE_EPOCH_FORMAT,
              dataTimeSpec.getTimezone());
      return new PinotDataFrameThirdEyeResponse(request, convertedTimeSpec, joinedResultSet.getMetaData(),
          joinedResultSet.getDataFrame());
    } finally {
      ThirdeyeMetricsUtil.pinotCallCounter.inc();
      ThirdeyeMetricsUtil.pinotDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  /**
   * Ensures the data frame of {@link ThirdEyeDataFrameResultSet} is copied to prevent any future changes to its data
   * frame also changes the value in the loading cache.
   *
   * @param resultSetGroup the result set group from the loading cache.
   *
   * @return the result set group whose data frame result sets are a copy of that in the loading cache.
   */
  private ThirdEyeResultSetGroup copyDataFrameResultSets(ThirdEyeResultSetGroup resultSetGroup) {
    List<ThirdEyeResultSet> resultSets = resultSetGroup.getResultSets();
    List<ThirdEyeResultSet> copiedResultSets = new ArrayList<>();
    for (ThirdEyeResultSet resultSet : resultSets) {
      if (resultSet instanceof ThirdEyeDataFrameResultSet) {
        ThirdEyeDataFrameResultSet dataFrameResultSet = (ThirdEyeDataFrameResultSet) resultSet;
        DataFrame copiedDataFrame = new DataFrame(dataFrameResultSet.getDataFrame());
        ThirdEyeResultSet copiedResultSet =
            new ThirdEyeDataFrameResultSet(dataFrameResultSet.getMetaData(), copiedDataFrame);
        copiedResultSets.add(copiedResultSet);
      } else {
        copiedResultSets.add(resultSet);
      }
    }
    return new ThirdEyeResultSetGroup(copiedResultSets);
  }

  /**
   * Converts the timestamp that is stored in Pinot to ThirdEye's uniform format, i.e., milliseconds.
   * This method takes care these timestamp formats in Pinot:
   * 1. ISO format in yyyyMMdd.
   * 2. EPOCH format in different granularity (e.g., Epoch in MS, Epoch in 5-MINS, Epoch in 1-HOURS, etc.)
   *
   * @param originalResultSetGroup the result set group that contains the result sets whose timestamp column should be
   *                               converted to MS.
   * @param timestampTimeSpec the timestamp spec. of the result set. Typically, the spec is related to the dataset from which
   *                          the data of the result set group come.
   *
   * @return a result set group whose result sets' time column is converted to MS.
   */
  private ThirdEyeResultSetGroup convertTimestampToMS(ThirdEyeResultSetGroup originalResultSetGroup,
      final TimeSpec timestampTimeSpec) {

    if (timestampTimeSpec != null) {
      final DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd");
      boolean isEPOCH =
          timestampTimeSpec.getFormat() == null || TimeSpec.SINCE_EPOCH_FORMAT.equals(timestampTimeSpec.getFormat());

      List<ThirdEyeResultSet> timestampConvertedResultSets = new ArrayList<>();
      for (ThirdEyeResultSet thirdEyeResultSet : originalResultSetGroup.getResultSets()) {
        // TODO: Should we enforce the usage of ThirdEyeDataFrameResultSet?
        ThirdEyeDataFrameResultSet dataFrameResultSet = (ThirdEyeDataFrameResultSet) thirdEyeResultSet;
        DataFrame dataFrame = new DataFrame(dataFrameResultSet.getDataFrame());
        if (dataFrame.contains(timestampTimeSpec.getColumnName())) {
          if (isEPOCH) {
            final TimeGranularity dataGranularity = timestampTimeSpec.getDataGranularity();
            dataFrame.mapInPlace(new Series.StringFunction() {
              @Override
              public String apply(String... values) {
                return Long.toString(dataGranularity.toMillis(Long.parseLong(values[0])));
              }
            }, timestampTimeSpec.getColumnName());
          } else {
            dataFrame.mapInPlace(new Series.StringFunction() {
              @Override
              public String apply(String... values) {
                DateTime dateTime = fmt.withZone(timestampTimeSpec.getTimezone()).parseDateTime(values[0]);
                return Long.toString(dateTime.getMillis());
              }
            }, timestampTimeSpec.getColumnName());
          }
          // Add the timestamp-converted result set
          ThirdEyeDataFrameResultSet newDataFrameResultSet =
              new ThirdEyeDataFrameResultSet(dataFrameResultSet.getMetaData(), dataFrame);
          timestampConvertedResultSets.add(newDataFrameResultSet);
        } else {
          timestampConvertedResultSets.add(dataFrameResultSet);
        }
      }

      return new ThirdEyeResultSetGroup(timestampConvertedResultSets);
    } else {
      return originalResultSetGroup;
    }
  }

  /**
   * Returns a result sets that joins multiple (GroupBy) result sets. When we send a group-by query as the following:
   *   SELECT SUM(traffic_count),SUM(page_view) FROM dataset GROUP BY time,
   * two results will be returned, which contains the following columns, perspectively:
   * 1. time, sum_traffic_count
   * 2. time, page_view
   *
   * This method joins these two result sets and returns a result set that contain these columns in one table:
   *   time, sum_traffic_count, page_view.
   *
   * @param metricFunctionToResultSetList the result sets returned from Pinot.
   *
   * @return a joined result set.
   */
  private ThirdEyeDataFrameResultSet joinResultSets(
      Map<MetricFunction, List<ThirdEyeResultSet>> metricFunctionToResultSetList) {

    Set<String> metricNames = new LinkedHashSet<>();
    DataFrame joinedDf = null;
    List<String> groupKeyColumnNames = null;
    for (Entry<MetricFunction, List<ThirdEyeResultSet>> metricFunctionResultSets : metricFunctionToResultSetList
        .entrySet()) {
      List<ThirdEyeResultSet> resultSets = metricFunctionResultSets.getValue();
      for (ThirdEyeResultSet resultSet : resultSets) {
        ThirdEyeDataFrameResultSet dataFrameResultSet = (ThirdEyeDataFrameResultSet) resultSet;
        // 2x2 join on all result sets
        if (joinedDf != null) {
          DataFrame dataFrameRight = dataFrameResultSet.getDataFrame();
          if (Objects.equals(joinedDf.getIndexNames(), dataFrameRight.getIndexNames())) {
            if (CollectionUtils.isNotEmpty(joinedDf.getIndexNames())) {
              // Group-By Results
              joinedDf = joinedDf.joinLeft(dataFrameRight);
            } else {
              // Aggregation Results or Select Results
              for (Entry<String, Series> seriesEntry : dataFrameRight.getSeries().entrySet()) {
                joinedDf.addSeries(seriesEntry.getKey(), seriesEntry.getValue());
              }
            }
          } else {
            // Mix of different types of results
            throw new IllegalArgumentException("Unsupported Join operation on data frames with different indexes; "
                + "Make sure the query results are the same types (e.g., all aggregation results, all group by results,"
                + " etc.)");
          }
        } else {
          // The very first result set
          joinedDf = new DataFrame(dataFrameResultSet.getDataFrame());
          groupKeyColumnNames = dataFrameResultSet.getMetaData().getGroupKeyColumnNames();
        }
        // Build matadata regarding metric column names
        for (int i = 0; i < dataFrameResultSet.getColumnCount(); i++) {
          String metricName = dataFrameResultSet.getColumnName(i);
          if (metricNames.contains(metricName)) {
            throw new IllegalArgumentException(
                String.format("Unable to join duplicated metric column: %s", metricName));
          } else {
            metricNames.add(metricName);
          }
        }
      }
    }

    ThirdEyeResultSetMetaData metaData =
        new ThirdEyeResultSetMetaData(groupKeyColumnNames, new ArrayList<String>(metricNames));

    ThirdEyeDataFrameResultSet resultSet = new ThirdEyeDataFrameResultSet(metaData, joinedDf);
    return resultSet;
  }

  /**
   * Definition of Pre-Aggregated Data: the data that has been pre-aggregated or pre-calculated and should not be
   * applied with any aggregation function during grouping by. Usually, this kind of data exists in non-additive
   * dataset. For such data, we assume that there exists a dimension value named "all", which could be overridden
   * in dataset configuration, that stores the pre-aggregated value.
   *
   * By default, when a query does not specify any value on pre-aggregated dimension, Pinot aggregates all values
   * at that dimension, which is an undesirable behavior for non-additive data. Therefore, this method modifies the
   * request's dimension filters such that the filter could pick out the "all" value for that dimension. Example:
   * Suppose that we have a dataset with 3 pre-aggregated dimensions: country, pageName, and osName, and the pre-
   * aggregated keyword is 'all'. Further assume that the original request's filter = {'country'='US, IN'} and
   * GroupBy dimension = pageName, then the decorated request has the new filter =
   * {'country'='US, IN', 'osName' = 'all'}. Note that 'pageName' = 'all' is not in the filter set because it is
   * a GroupBy dimension, which will not be aggregated.
   *
   * @param filterSet the original filterSet, which will NOT be modified.
   *
   * @return a decorated filter set for the queries to the pre-aggregated dataset.
   */
  public static Multimap<String, String> generateFilterSetWithPreAggregatedDimensionValue(
      Multimap<String, String> filterSet, List<String> groupByDimensions, List<String> allDimensions,
      List<String> dimensionsHaveNoPreAggregation, String preAggregatedKeyword) {

    Set<String> preAggregatedDimensionNames = new HashSet<>(allDimensions);
    // Remove dimension names that do not have the pre-aggregated value
    if (CollectionUtils.isNotEmpty(dimensionsHaveNoPreAggregation)) {
      preAggregatedDimensionNames.removeAll(dimensionsHaveNoPreAggregation);
    }
    // Remove dimension names that have been included in the original filter set because we should not override
    // users' explicit filter setting
    if (filterSet != null) {
      preAggregatedDimensionNames.removeAll(filterSet.asMap().keySet());
    }
    // Remove dimension names that are going to be grouped by because GroupBy dimensions will not be aggregated anyway
    if (CollectionUtils.isNotEmpty(groupByDimensions)) {
      preAggregatedDimensionNames.removeAll(groupByDimensions);
    }
    // Add pre-aggregated dimension value to the remaining dimension names
    Multimap<String, String> decoratedFilterSet;
    if (filterSet != null) {
      decoratedFilterSet = HashMultimap.create(filterSet);
    } else {
      decoratedFilterSet = HashMultimap.create();
    }
    if (preAggregatedDimensionNames.size() != 0) {
      for (String preComputedDimensionName : preAggregatedDimensionNames) {
        decoratedFilterSet.put(preComputedDimensionName, preAggregatedKeyword);
      }
    }

    return decoratedFilterSet;
  }

  /**
   * Returns the cached ResultSetGroup corresponding to the given Pinot query.
   *
   * @param pinotQuery the query that is specifically constructed for Pinot.
   * @return the corresponding ResultSetGroup to the given Pinot query.
   *
   * @throws ExecutionException is thrown if failed to connect to Pinot or gets results from Pinot.
   */
  public ThirdEyeResultSetGroup executePQL(PinotQuery pinotQuery) throws ExecutionException {
    Preconditions
        .checkNotNull(this.pinotResponseCache, "{} doesn't connect to Pinot or cache is not initialized.", getName());

    return this.pinotResponseCache.get(pinotQuery);
  }

  /**
   * Refreshes and returns the cached ResultSetGroup corresponding to the given Pinot query.
   *
   * @param pinotQuery the query that is specifically constructed for Pinot.
   * @return the corresponding ResultSetGroup to the given Pinot query.
   *
   * @throws ExecutionException is thrown if failed to connect to Pinot or gets results from Pinot.
   */
  public ThirdEyeResultSetGroup refreshPQL(PinotQuery pinotQuery) throws ExecutionException {
    Preconditions
        .checkNotNull(this.pinotResponseCache, "{} doesn't connect to Pinot or cache is not initialized.", getName());

    pinotResponseCache.refresh(pinotQuery);

    return executePQL(pinotQuery);
  }

  @Override
  public List<String> getDatasets() throws Exception {
    return CACHE_REGISTRY_INSTANCE.getDatasetsCache().getDatasets();
  }

  @Override
  public long getMaxDataTime(String dataset) throws Exception {
    return pinotDataSourceMaxTime.getMaxDateTime(dataset);
  }

  @Override
  public Map<String, List<String>> getDimensionFilters(String dataset) throws Exception {
    return pinotDataSourceDimensionFilters.getDimensionFilters(dataset);
  }

  @Override
  public void clear() throws Exception {
  }

  @Override
  public void close() throws Exception {
    controllerClient.close();
  }

  /**
   * Initialzes the cache and cache loader for the response of this data source.
   *
   * @param pinotResponseCacheLoader the cache loader that directly gets query results from data source if the results
   *                                 are not in its cache.
   *
   * @throws Exception is thrown when Pinot brokers are unable to be reached.
   */
  private static LoadingCache<PinotQuery, ThirdEyeResultSetGroup> buildResponseCache(
      PinotResponseCacheLoader pinotResponseCacheLoader) throws Exception {
    Preconditions.checkNotNull(pinotResponseCacheLoader, "A loader that sends query to Pinot is required.");

    // Initializes listener that prints expired entries in debuggin mode.
    RemovalListener<PinotQuery, ThirdEyeResultSetGroup> listener;
    if (LOG.isDebugEnabled()) {
      listener = new RemovalListener<PinotQuery, ThirdEyeResultSetGroup>() {
        @Override
        public void onRemoval(RemovalNotification<PinotQuery, ThirdEyeResultSetGroup> notification) {
          LOG.debug("Expired {}", notification.getKey().getPql());
        }
      };
    } else {
      listener = new RemovalListener<PinotQuery, ThirdEyeResultSetGroup>() {
        @Override public void onRemoval(RemovalNotification<PinotQuery, ThirdEyeResultSetGroup> notification) { }
      };
    }

    // ResultSetGroup Cache. The size of this cache is limited by the total number of buckets in all ResultSetGroup.
    // We estimate that 1 bucket (including overhead) consumes 1KB and this cache is allowed to use up to 50% of max
    // heap space.
    long maxBucketNumber = getApproximateMaxBucketNumber(DEFAULT_HEAP_PERCENTAGE_FOR_RESULTSETGROUP_CACHE);
    LOG.debug("Max bucket number for {}'s cache is set to {}", DATA_SOURCE_NAME, maxBucketNumber);

    return CacheBuilder.newBuilder()
        .removalListener(listener)
        .expireAfterWrite(ThirdEyeCacheRegistry.CACHE_EXPIRATION_HOURS, TimeUnit.HOURS)
        .maximumWeight(maxBucketNumber)
        .weigher(new Weigher<PinotQuery, ThirdEyeResultSetGroup>() {
          @Override public int weigh(PinotQuery pinotQuery, ThirdEyeResultSetGroup resultSetGroup) {
            int resultSetCount = resultSetGroup.size();
            int weight = 0;
            for (int idx = 0; idx < resultSetCount; ++idx) {
              ThirdEyeResultSet resultSet = resultSetGroup.get(idx);
              weight += ((resultSet.getColumnCount() + resultSet.getGroupKeyLength()) * resultSet.getRowCount());
            }
            return weight;
          }
        })
        .build(pinotResponseCacheLoader);
  }

  /**
   * Returns the suggested max weight for LoadingCache according to the given percentage of max heap space.
   *
   * The approximate weight is calculated by following rules:
   * 1. We estimate that a bucket, including its overhead, occupies 1 KB.
   * 2. Cache size (in bytes) = System's maxMemory * percentage
   * 3. We also bound the cache size between DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB and
   *    DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB if max heap size is unavailable.
   * 4. Weight (number of buckets) = cache size / 1KB.
   *
   * @param percentage the percentage of JVM max heap space
   * @return the suggested max weight for LoadingCache
   */
  private static long getApproximateMaxBucketNumber(int percentage) {
    long jvmMaxMemoryInBytes = Runtime.getRuntime().maxMemory();
    if (jvmMaxMemoryInBytes == Long.MAX_VALUE) { // Check upper bound
      jvmMaxMemoryInBytes = DEFAULT_UPPER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB * 1048576L; // MB to Bytes
    } else { // Check lower bound
      long lowerBoundInBytes = DEFAULT_LOWER_BOUND_OF_RESULTSETGROUP_CACHE_SIZE_IN_MB * 1048576L; // MB to Bytes
      if (jvmMaxMemoryInBytes < lowerBoundInBytes) {
        jvmMaxMemoryInBytes = lowerBoundInBytes;
      }
    }
    return (jvmMaxMemoryInBytes / 102400) * percentage;
  }


  /** TESTING ONLY - WE SHOULD NOT BE USING THIS. */
  @Deprecated
  private HttpHost controllerHost;
  @Deprecated
  private CloseableHttpClient controllerClient;

  @Deprecated
  protected PinotThirdEyeDataSource(String host, int port) {
    this.controllerHost = new HttpHost(host, port);
    this.controllerClient = HttpClients.createDefault();
    this.pinotDataSourceMaxTime = new PinotDataSourceMaxTime(this);
    this.pinotDataSourceDimensionFilters = new PinotDataSourceDimensionFilters(this);
    LOG.info("Created PinotThirdEyeDataSource with controller {}", controllerHost);
  }

  @Deprecated
  public static PinotThirdEyeDataSource fromZookeeper(String controllerHost, int controllerPort, String zkUrl) {
    ZkClient zkClient = new ZkClient(zkUrl);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.waitUntilConnected();
    PinotThirdEyeDataSource pinotThirdEyeDataSource = new PinotThirdEyeDataSource(controllerHost, controllerPort);
    LOG.info("Created PinotThirdEyeDataSource to zookeeper: {} controller: {}:{}", zkUrl, controllerHost, controllerPort);
    return pinotThirdEyeDataSource;
  }

  @Deprecated
  public static PinotThirdEyeDataSource getDefaultTestDataSource() {
    // TODO REPLACE WITH CONFIGS
    String controllerHost = "localhost";
    int controllerPort = 11984;
    String zkUrl = "localhost:12913/pinot-cluster";
    return fromZookeeper(controllerHost, controllerPort, zkUrl);
  }
}
