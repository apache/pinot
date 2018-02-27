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
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.datasource.ThirdEyeDataSource;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.TimeRangeUtils;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import com.linkedin.thirdeye.util.ThirdEyeUtils;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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
  public PinotThirdEyeResponse execute(ThirdEyeRequest request) throws Exception {
    Preconditions.checkNotNull(this.pinotResponseCache, "{} doesn't connect to Pinot or cache is not initialized.",
        getName());

    long tStart = System.nanoTime();
    try {
      LinkedHashMap<MetricFunction, List<ThirdEyeResultSet>> metricFunctionToResultSetList = new LinkedHashMap<>();

      TimeSpec timeSpec = null;
      for (MetricFunction metricFunction : request.getMetricFunctions()) {
        String dataset = metricFunction.getDataset();
        DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
        TimeSpec dataTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);
        if (timeSpec == null) {
          timeSpec = dataTimeSpec;
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
        String tableName = ThirdEyeUtils.computeTableName(dataset);
        String pql = null;
        MetricConfigDTO metricConfig = metricFunction.getMetricConfig();
        if (metricConfig != null && metricConfig.isDimensionAsMetric()) {
          pql = PqlUtils.getDimensionAsMetricPql(request, metricFunction, decoratedFilterSet, dataTimeSpec, datasetConfig);
        } else {
          pql = PqlUtils.getPql(request, metricFunction, decoratedFilterSet, dataTimeSpec);
        }
        ThirdEyeResultSetGroup resultSetGroup = this.executePQL(new PinotQuery(pql, tableName));
        metricFunctionToResultSetList.put(metricFunction, resultSetGroup.getResultSets());
      }

      List<String[]> resultRows = parseResultSets(request, metricFunctionToResultSetList);
      PinotThirdEyeResponse resp = new PinotThirdEyeResponse(request, resultRows, timeSpec);
      return resp;
    } finally {
      ThirdeyeMetricsUtil.pinotCallCounter.inc();
      ThirdeyeMetricsUtil.pinotDurationCounter.inc(System.nanoTime() - tStart);
    }
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

    try {
      return this.pinotResponseCache.get(pinotQuery);
    } catch (ExecutionException e) {
      LOG.error("Failed to execute PQL: {}", pinotQuery.getPql());
      throw e;
    }
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

    try {
      pinotResponseCache.refresh(pinotQuery);
      return pinotResponseCache.get(pinotQuery);
    } catch (ExecutionException e) {
      LOG.error("Failed to refresh PQL: {}", pinotQuery.getPql());
      throw e;
    }
  }

  private List<String[]> parseResultSets(ThirdEyeRequest request,
      Map<MetricFunction, List<ThirdEyeResultSet>> metricFunctionToResultSetList) throws ExecutionException {

    int numGroupByKeys = 0;
    boolean hasGroupBy = false;
    if (request.getGroupByTimeGranularity() != null) {
      numGroupByKeys += 1;
    }
    if (request.getGroupBy() != null) {
      numGroupByKeys += request.getGroupBy().size();
    }
    if (numGroupByKeys > 0) {
      hasGroupBy = true;
    }
    int numMetrics = request.getMetricFunctions().size();
    int numCols = numGroupByKeys + numMetrics;
    boolean hasGroupByTime = false;
    if (request.getGroupByTimeGranularity() != null) {
      hasGroupByTime = true;
    }

    int position = 0;
    Map<String, String[]> dataMap = new HashMap<>();
    Map<String, Integer> countMap = new HashMap<>();
    for (Entry<MetricFunction, List<ThirdEyeResultSet>> entry : metricFunctionToResultSetList.entrySet()) {

      MetricFunction metricFunction = entry.getKey();

      String dataset = metricFunction.getDataset();
      DatasetConfigDTO datasetConfig = ThirdEyeUtils.getDatasetConfigFromName(dataset);
      TimeSpec dataTimeSpec = ThirdEyeUtils.getTimestampTimeSpecFromDatasetConfig(datasetConfig);

      TimeGranularity dataGranularity = null;
      long startTime = request.getStartTimeInclusive().getMillis();
      DateTimeZone dateTimeZone = Utils.getDataTimeZone(dataset);
      DateTime startDateTime = new DateTime(startTime, dateTimeZone);
      dataGranularity = dataTimeSpec.getDataGranularity();
      boolean isISOFormat = false;
      DateTimeFormatter inputDataDateTimeFormatter = null;
      String timeFormat = dataTimeSpec.getFormat();
      if (timeFormat != null && !timeFormat.equals(TimeSpec.SINCE_EPOCH_FORMAT)) {
        isISOFormat = true;
        inputDataDateTimeFormatter = DateTimeFormat.forPattern(timeFormat).withZone(dateTimeZone);
      }

      List<ThirdEyeResultSet> resultSets = entry.getValue();
      for (int i = 0; i < resultSets.size(); i++) {
        ThirdEyeResultSet resultSet = resultSets.get(i);
        int numRows = resultSet.getRowCount();
        for (int r = 0; r < numRows; r++) {
          boolean skipRowDueToError = false;
          String[] groupKeys;
          if (hasGroupBy) {
            groupKeys = new String[resultSet.getGroupKeyLength()];
            for (int grpKeyIdx = 0; grpKeyIdx < resultSet.getGroupKeyLength(); grpKeyIdx++) {
              String groupKeyVal = "";
              try {
                groupKeyVal = resultSet.getGroupKeyColumnValue(r, grpKeyIdx);
              } catch (Exception e) {
                // IGNORE FOR NOW, workaround for Pinot Bug
              }
              if (hasGroupByTime && grpKeyIdx == 0) {
                int timeBucket;
                long millis;
                if (!isISOFormat) {
                  millis = dataGranularity.toMillis(Double.valueOf(groupKeyVal).longValue());
                } else {
                  millis = DateTime.parse(groupKeyVal, inputDataDateTimeFormatter).getMillis();
                }
                if (millis < startTime) {
                  LOG.error("Data point earlier than requested start time {}: {}", new Date(startTime), new Date(millis));
                  skipRowDueToError = true;
                  break;
                }
                timeBucket = TimeRangeUtils
                    .computeBucketIndex(request.getGroupByTimeGranularity(), startDateTime,
                        new DateTime(millis, dateTimeZone));
                groupKeyVal = String.valueOf(timeBucket);
              }
              groupKeys[grpKeyIdx] = groupKeyVal;
            }
            if (skipRowDueToError) {
              continue;
            }
          } else {
            groupKeys = new String[] {};
          }
          String compositeGroupKey = StringUtils.join(groupKeys, "|");

          String[] rowValues = dataMap.get(compositeGroupKey);
          if (rowValues == null) {
            rowValues = new String[numCols];
            Arrays.fill(rowValues, "0");
            System.arraycopy(groupKeys, 0, rowValues, 0, groupKeys.length);
            dataMap.put(compositeGroupKey, rowValues);
          }

          String countKey = compositeGroupKey + "|" + position;
          if (!countMap.containsKey(countKey)) {
            countMap.put(countKey, 0);
          }
          final int aggCount = countMap.get(countKey);
          countMap.put(countKey, aggCount + 1);

          // aggregation of multiple values
          rowValues[groupKeys.length + position + i] = String.valueOf(
              reduce(
                  Double.parseDouble(rowValues[groupKeys.length + position + i]),
                  Double.parseDouble(resultSet.getString(r, 0)),
                  aggCount,
                  metricFunction.getFunctionName()
              ));

        }
      }
      position ++;
    }
    List<String[]> rows = new ArrayList<>();
    rows.addAll(dataMap.values());
    return rows;

  }

  static double reduce(double aggregate, double value, int prevCount, MetricAggFunction aggFunction) {
    switch(aggFunction) {
      case SUM:
        return aggregate + value;
      case AVG:
        return (aggregate * prevCount + value) / (prevCount + 1);
      case MAX:
        return Math.max(aggregate, value);
      case COUNT:
        return aggregate + 1;
      default:
        throw new IllegalArgumentException(String.format("Unknown aggregation function '%s'", aggFunction));
    }
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
