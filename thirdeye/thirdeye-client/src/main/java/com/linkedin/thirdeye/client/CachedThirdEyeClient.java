package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.SegmentDescriptor;
import com.linkedin.thirdeye.api.StarTreeConfig;

/**
 * Wrapper client to provide a configurable caching layer on top of existing {@link ThirdEyeClient}
 * implementations.
 * @author jteoh
 */
public class CachedThirdEyeClient implements ThirdEyeClient {
  private static final int DEFAULT_CACHE_DURATION = 5;

  private static final Logger LOG = LoggerFactory.getLogger(CachedThirdEyeClient.class);
  private final ThirdEyeClient client;
  private final CachedThirdEyeClientConfig config;

  private final LoadingCache<ThirdEyeRequest, Map<DimensionKey, MetricTimeSeries>> resultCache;
  private final LoadingCache<String, StarTreeConfig> starTreeConfigCache;
  private final LoadingCache<ThirdEyeRequest, ThirdEyeRawResponse> rawResultCache;
  private final LoadingCache<String, List<SegmentDescriptor>> segmentDescriptorCache;
  private final LoadingCache<ThirdEyeRequest, Long> expectedTimeBucketCache;
  private final LoadingCache<ThirdEyeRequest, List<String>> expectedTimestampsCache;
  private Supplier<List<String>> collectionsSupplier;
  private final Supplier<List<String>> _baseCollectionsSupplier = new CollectionSupplier();

  /**
   * Creates a cached wrapper for the provided client, using cache configurations provided in the
   * config. <tt>config</tt> determines whether an optimization is enabled to leverage
   * internal caches for the {@link ThirdEyeClient#execute(ThirdEyeRequest)} method via the
   * {@link CachedThirdEyeClientConfig#useCacheForExecuteMethod()} method.
   * @param client
   * @param config
   * @param useClientExecute If <tt>true</tt>, the provided client's
   *          {@link #execute(ThirdEyeRequest)} method will be used instead of the internal cache
   *          optimization.
   * @return
   */
  @SuppressWarnings({
      "unchecked", "rawtypes"
  })
  public CachedThirdEyeClient(ThirdEyeClient client, CachedThirdEyeClientConfig config) {

    LOG.info("Initializing cache client for {} with config {}, using client", client, config);
    this.client = client;

    if (config != null) {
      config = new CachedThirdEyeClientConfig();
    }
    this.config = config;

    CacheBuilder builder = CacheBuilder.newBuilder();
    if (config.isExpireAfterAccess()) {
      builder.expireAfterAccess(config.getExpirationTime(), config.getExpirationUnit());
    } else {
      builder.expireAfterWrite(config.getExpirationTime(), config.getExpirationUnit());
    }
    this.resultCache = builder.build(new ResultCacheLoader());
    this.rawResultCache = builder.build(new RawResultCacheLoader());

    // TODO make these expiration times individually configurable
    this.starTreeConfigCache =
        CacheBuilder.newBuilder().expireAfterWrite(DEFAULT_CACHE_DURATION, TimeUnit.SECONDS)
            .build(new StarTreeConfigCacheLoader());
    // longer because request involves file system operations (migrated from DataCache)
    CacheBuilder<Object, Object> readCacheBuilder =
        CacheBuilder.newBuilder().expireAfterAccess(DEFAULT_CACHE_DURATION, TimeUnit.MINUTES);
    this.segmentDescriptorCache = readCacheBuilder.build(new SegmentDescriptorCacheLoader());

    this.expectedTimeBucketCache = readCacheBuilder.build(new ExpectedTimeBucketsCacheLoader());
    this.expectedTimestampsCache = readCacheBuilder.build(new ExpectedTimestampsCacheLoader());

    this.collectionsSupplier = buildCollectionsSupplier();
  }

  @Override
  public Map<DimensionKey, MetricTimeSeries> execute(ThirdEyeRequest request) throws Exception {
    LOG.info("execute: {}", request);
    return resultCache.get(request);
  }

  @Override
  public ThirdEyeRawResponse getRawResponse(ThirdEyeRequest request) throws Exception {
    LOG.info("getRawResponse: {}", request);
    return rawResultCache.get(request);
  }

  @Override
  public StarTreeConfig getStarTreeConfig(String collection) throws Exception {
    LOG.info("getStarTreeConfig: {}", collection);
    return starTreeConfigCache.get(collection);
  }

  @Override
  public List<String> getCollections() throws Exception {
    LOG.info("getCollections");
    return collectionsSupplier.get();
  }

  @Override
  public List<SegmentDescriptor> getSegmentDescriptors(String collection) throws Exception {
    LOG.info("getSegmentDescriptors: {}", collection);
    return segmentDescriptorCache.get(collection);
  }

  @Override
  public long getExpectedTimeBuckets(ThirdEyeRequest request) throws Exception {
    return expectedTimeBucketCache.get(request);
  }

  @Override
  public List<String> getExpectedTimestamps(ThirdEyeRequest request) throws Exception {
    return expectedTimestampsCache.get(request);
  }

  @Override
  public void clear() throws Exception {
    resultCache.invalidateAll();
    starTreeConfigCache.invalidateAll();
    rawResultCache.invalidateAll();
    this.collectionsSupplier = buildCollectionsSupplier();
    segmentDescriptorCache.invalidateAll();
    client.clear();
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  private Supplier<List<String>> buildCollectionsSupplier() {
    return Suppliers.memoizeWithExpiration(_baseCollectionsSupplier, config.getExpirationTime(),
        config.getExpirationUnit());
  }

  /**
   * Unless specifically specified by config.userClientExecute(), default behavior will leverage
   * internal caches to retrieve results.
   */
  private class ResultCacheLoader
      extends CacheLoader<ThirdEyeRequest, Map<DimensionKey, MetricTimeSeries>> {
    @Override
    public Map<DimensionKey, MetricTimeSeries> load(ThirdEyeRequest request) throws Exception {
      if (!config.useCacheForExecuteMethod()) {
        return client.execute(request);
      }
      ThirdEyeRawResponse thirdEyeRawResponse = rawResultCache.get(request);
      StarTreeConfig starTreeConfig = starTreeConfigCache.get(request.getCollection());
      Map<String, MetricType> metricTypes = new HashMap<>();
      for (MetricSpec metricSpec : starTreeConfig.getMetrics()) {
        String metricName = metricSpec.getName();
        MetricType metricType = metricSpec.getType();
        metricTypes.put(metricName, metricType);
      }
      List<MetricType> projectionTypes = new ArrayList<>();
      for (String metricName : thirdEyeRawResponse.getMetrics()) {
        MetricType metricType = metricTypes.get(metricName);
        projectionTypes.add(metricType);
      }
      return thirdEyeRawResponse.convert(projectionTypes);
    }
  }

  private class RawResultCacheLoader extends CacheLoader<ThirdEyeRequest, ThirdEyeRawResponse> {

    @Override
    public ThirdEyeRawResponse load(ThirdEyeRequest request) throws Exception {
      return client.getRawResponse(request);
    }
  }

  private class StarTreeConfigCacheLoader extends CacheLoader<String, StarTreeConfig> {

    @Override
    public StarTreeConfig load(String collection) throws Exception {
      return client.getStarTreeConfig(collection);
    }
  }

  private class CollectionSupplier implements Supplier<List<String>> {
    @Override
    public List<String> get() {
      try {
        return client.getCollections();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  };

  private class SegmentDescriptorCacheLoader extends CacheLoader<String, List<SegmentDescriptor>> {
    @Override
    public List<SegmentDescriptor> load(String collection) throws Exception {
      return client.getSegmentDescriptors(collection);
    }
  }

  private class ExpectedTimeBucketsCacheLoader extends CacheLoader<ThirdEyeRequest, Long> {
    @Override
    public Long load(ThirdEyeRequest request) throws Exception {
      return client.getExpectedTimeBuckets(request);
    }
  }

  private class ExpectedTimestampsCacheLoader extends CacheLoader<ThirdEyeRequest, List<String>> {
    @Override
    public List<String> load(ThirdEyeRequest request) throws Exception {
      return client.getExpectedTimestamps(request);
    }
  }

  @Override
  public String toString() {
    return String.format("Cached client(%s): %s", client, config);
  }
}
