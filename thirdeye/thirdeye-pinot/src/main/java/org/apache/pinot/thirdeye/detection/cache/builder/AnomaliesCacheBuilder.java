package org.apache.pinot.thirdeye.detection.cache.builder;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.cache.CacheConfig;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *  Essentially a fetcher for fetching anomalies from cache/datasource.
 *  The cache holds anomalies information per Anomaly Slices
 */
public class AnomaliesCacheBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(AnomaliesCacheBuilder.class);

  private static final String PROP_DETECTION_CONFIG_ID = "detectionConfigId";
  private static final long TIMEOUT = 60000;
  private static LoadingCache<AnomalySlice, Collection<MergedAnomalyResultDTO>> CACHE;
  private final ExecutorService executor = Executors.newCachedThreadPool();

  private MergedAnomalyResultManager anomalyDAO;

  private boolean cacheEnabled;

  private AnomaliesCacheBuilder(boolean enabled) {
    this.cacheEnabled = enabled;
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
  }

  public static LoadingCache<AnomalySlice, Collection<MergedAnomalyResultDTO>> getInstance(boolean cacheEnabled) {
    if (CACHE == null) {
      AnomaliesCacheBuilder anomaliesCache = new AnomaliesCacheBuilder(cacheEnabled);
      anomaliesCache.init();
    }

    return CACHE;
  }

  public static LoadingCache<AnomalySlice, Collection<MergedAnomalyResultDTO>> getInstance() {
    if (CacheConfig.getInstance().useInMemoryCache()) {
      return getInstance(true);
    }

    return getInstance(false);
  }


  private void init() {
    LOG.info("Initializing anomalies cache");
    CACHE = CacheBuilder.newBuilder()
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .maximumSize(10000)
        .build(new CacheLoader<AnomalySlice, Collection<MergedAnomalyResultDTO>>() {
          @Override
          public Collection<MergedAnomalyResultDTO> load(AnomalySlice slice) {
            return loadAnomalies(Collections.singleton(slice)).get(slice);
          }

          @Override
          public Map<AnomalySlice, Collection<MergedAnomalyResultDTO>> loadAll(Iterable<? extends AnomalySlice> slices) {
            return loadAnomalies(Lists.newArrayList(slices));
          }
        });
  }

  private Map<AnomalySlice, Collection<MergedAnomalyResultDTO>> loadAnomalies(Collection<AnomalySlice> slices) {
    Map<AnomalySlice, Collection<MergedAnomalyResultDTO>> output = new HashMap<>();
    try {
      long ts = System.currentTimeMillis();

      // if the anomalies are already in cache, return directly
      if (cacheEnabled) {
        for (AnomalySlice slice : slices) {
          for (Map.Entry<AnomalySlice, Collection<MergedAnomalyResultDTO>> entry : CACHE.asMap().entrySet()) {
            // if the anomaly slice is already in cache, return directly. Otherwise fetch from data source.
            if (entry.getKey().containSlice(slice)) {
              output.computeIfAbsent(slice, k -> new ArrayList<>());
              for (MergedAnomalyResultDTO anomaly : entry.getValue()) {
                if (slice.match(anomaly)) {
                  output.get(slice).add(anomaly);
                }
              }
              break;
            }
          }
        }
      }

      // if not in cache, fetch from data source
      Map<AnomalySlice, Future<Collection<MergedAnomalyResultDTO>>> futures = new HashMap<>();
      for (AnomalySlice slice : slices) {
        if (!output.containsKey(slice)) {
          futures.put(slice, this.executor.submit(() -> {
            List<Predicate> predicates = DetectionUtils.buildPredicatesOnTime(slice.getStart(), slice.getEnd());

            if (slice.getDetectionId() >= 0) {
              predicates.add(Predicate.EQ(PROP_DETECTION_CONFIG_ID, slice.getDetectionId()));
            }

            if (predicates.isEmpty()) {
              throw new IllegalArgumentException("Must provide at least one of start, end, or " + PROP_DETECTION_CONFIG_ID);
            }

            Collection<MergedAnomalyResultDTO> anomalies = anomalyDAO.findByPredicate(DetectionUtils.AND(predicates));
            anomalies.removeIf(anomaly -> !slice.match(anomaly));

            return anomalies;
          }));
        }
      }

      for (AnomalySlice slice : slices) {
        if (futures.get(slice) != null) {
          output.put(slice, futures.get(slice).get(TIMEOUT, TimeUnit.MILLISECONDS));
        }
      }

      int anomalies = output.values().stream().mapToInt(Collection::size).sum();
      LOG.info("Fetched {} anomalies, from {} slices, took {} milliseconds, {} slices hit cache, {} slices missed cache",
          anomalies, slices.size(), System.currentTimeMillis() - ts,
          (slices.size() - futures.size()), futures.size());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return output;
  }
}
