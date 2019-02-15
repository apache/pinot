/*
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

package org.apache.pinot.thirdeye.detection;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.Weigher;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.spi.model.EventSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


public class DefaultDataProvider implements DataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultDataProvider.class);
  private static final long TIMEOUT = 60000;

  private final ExecutorService executor = Executors.newCachedThreadPool();

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final TimeSeriesLoader timeseriesLoader;
  private final AggregationLoader aggregationLoader;
  private final DetectionPipelineLoader loader;
  private final LoadingCache<MetricSlice, DataFrame> timeseriesCache;

  public DefaultDataProvider(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, EventManager eventDAO,
      MergedAnomalyResultManager anomalyDAO, TimeSeriesLoader timeseriesLoader, AggregationLoader aggregationLoader,
      DetectionPipelineLoader loader) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.eventDAO = eventDAO;
    this.anomalyDAO = anomalyDAO;
    this.timeseriesLoader = timeseriesLoader;
    this.aggregationLoader = aggregationLoader;
    this.loader = loader;
    this.timeseriesCache = CacheBuilder.newBuilder()
        // don't use more than one third of memory for detection time series
        .maximumWeight(Runtime.getRuntime().maxMemory() / 3)
        .expireAfterWrite(30, TimeUnit.MINUTES)
        .weigher((Weigher<MetricSlice, DataFrame>) (slice, dataFrame) -> dataFrame.size() * 2 * Double.BYTES)
        .build(new CacheLoader<MetricSlice, DataFrame>() {
          // load single slice
          @Override
          public DataFrame load(MetricSlice slice) {
            return loadTimeseries(Collections.singleton(slice)).get(slice);
          }

          // buck loading time series slice in parallel
          @Override
          public Map<MetricSlice, DataFrame> loadAll(Iterable<? extends MetricSlice> slices) throws Exception {
            return loadTimeseries(Lists.newArrayList(slices));
          }
        });
  }

  private Map<MetricSlice, DataFrame> loadTimeseries(Collection<MetricSlice> slices) {
    try {
      long ts = System.currentTimeMillis();
      Map<MetricSlice, DataFrame> output = new HashMap<>();

      // if the time series slice is already in cache, return directly
      for (MetricSlice slice : slices){
        for (Map.Entry<MetricSlice, DataFrame> entry : this.timeseriesCache.asMap().entrySet()) {
          if (entry.getKey().containSlice(slice)){
            DataFrame df = entry.getValue().filter(entry.getValue().getLongs(COL_TIME).between(slice.getStart(), slice.getEnd())).dropNull(COL_TIME);
            output.put(slice, df);
            break;
          }
        }
      }

      // if not in cache, fetch from data source
      Map<MetricSlice, Future<DataFrame>> futures = new HashMap<>();
      for (final MetricSlice slice : slices) {
        if (!output.containsKey(slice)){
          futures.put(slice, this.executor.submit(() -> DefaultDataProvider.this.timeseriesLoader.load(slice)));
        }
      }
      LOG.info("Fetching {} slices of timeseries, {} cache hit, {} cache miss", slices.size(), output.size(), futures.size());
      final long deadline = System.currentTimeMillis() + TIMEOUT;
      for (MetricSlice slice : slices) {
        if (!output.containsKey(slice)) {
          output.put(slice, futures.get(slice).get(makeTimeout(deadline), TimeUnit.MILLISECONDS));
        }
      }
      LOG.info("Fetching {} slices used {} milliseconds", slices.size(), System.currentTimeMillis() - ts);
      return output;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices) {
    try {
      Map<MetricSlice, DataFrame> cacheResult = this.timeseriesCache.getAll(slices);
      Map<MetricSlice, DataFrame> timeseriesResult = new HashMap<>();
      for (Map.Entry<MetricSlice, DataFrame> entry : cacheResult.entrySet()){
        // make a copy of the result so that cache won't be contaminated by client code
        timeseriesResult.put(entry.getKey(), entry.getValue().copy());
      }
      return  timeseriesResult;
    } catch (ExecutionException e) {
      throw new RuntimeException("fetch time series failed", e);
    }
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices, final List<String> dimensions) {
    try {
      Map<MetricSlice, Future<DataFrame>> futures = new HashMap<>();
      for (final MetricSlice slice : slices) {
        futures.put(slice, this.executor.submit(new Callable<DataFrame>() {
          @Override
          public DataFrame call() throws Exception {
            return DefaultDataProvider.this.aggregationLoader.loadAggregate(slice, dimensions, -1);
          }
        }));
      }

      final long deadline = System.currentTimeMillis() + TIMEOUT;
      Map<MetricSlice, DataFrame> output = new HashMap<>();
      for (MetricSlice slice : slices) {
        output.put(slice, futures.get(slice).get(makeTimeout(deadline), TimeUnit.MILLISECONDS));
      }
      return output;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices,
      long configId, boolean isLegacy) {
    String functionIdKey = "detectionConfigId";
    if (isLegacy) {
      functionIdKey = "functionId";
    }

    Multimap<AnomalySlice, MergedAnomalyResultDTO> output = ArrayListMultimap.create();
    for (AnomalySlice slice : slices) {
      List<Predicate> predicates = new ArrayList<>();
      if (slice.getEnd() >= 0) {
        predicates.add(Predicate.LT("startTime", slice.getEnd()));
      }
      if (slice.getStart() >= 0) {
        predicates.add(Predicate.GT("endTime", slice.getStart()));
      }
      if (configId >= 0) {
        predicates.add(Predicate.EQ(functionIdKey, configId));
      }

      if (predicates.isEmpty()) throw new IllegalArgumentException("Must provide at least one of start, end, or " + functionIdKey);

      Collection<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findByPredicate(AND(predicates));
      anomalies.removeIf(anomaly -> !slice.match(anomaly));

      if (isLegacy) {
        anomalies.removeIf(anomaly ->
            (configId >= 0) && (anomaly.getFunctionId() == null || anomaly.getFunctionId() != configId)
        );
      } else {
        anomalies.removeIf(anomaly ->
            (configId >= 0) && (anomaly.getDetectionConfigId() == null || anomaly.getDetectionConfigId() != configId)
        );
      }
      // filter all child anomalies. those are kept in the parent anomaly children set.
      anomalies = Collections2.filter(anomalies, mergedAnomaly -> mergedAnomaly != null && !mergedAnomaly.isChild());

      LOG.info("Fetched {} anomalies between (startTime = {}, endTime = {}) with confid Id = {}", anomalies.size(), slice.getStart(), slice.getEnd(), configId);
      output.putAll(slice, anomalies);
    }
    return output;
  }

  @Override
  public Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchLegacyAnomalies(Collection<AnomalySlice> slices, long configId) {
    return fetchAnomalies(slices, configId, true);
  }

  @Override
  public Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices, long configId) {
    return fetchAnomalies(slices, configId, false);
  }

  @Override
  public Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices) {
    Multimap<EventSlice, EventDTO> output = ArrayListMultimap.create();
    for (EventSlice slice : slices) {
      List<Predicate> predicates = new ArrayList<>();
      if (slice.getEnd() >= 0)
        predicates.add(Predicate.LT("startTime", slice.getEnd()));
      if (slice.getStart() >= 0)
        predicates.add(Predicate.GT("endTime", slice.getStart()));

      if (predicates.isEmpty())
        throw new IllegalArgumentException("Must provide at least one of start, or end");
      List<EventDTO> events = this.eventDAO.findByPredicate(AND(predicates));
      Iterator<EventDTO> itEvent = events.iterator();
      while (itEvent.hasNext()) {
        if (!slice.match(itEvent.next())) {
          itEvent.remove();
        }
      }

      output.putAll(slice, events);
    }
    return output;
  }

  @Override
  public Map<Long, MetricConfigDTO> fetchMetrics(Collection<Long> ids) {
    List<MetricConfigDTO> metrics = this.metricDAO.findByPredicate(Predicate.IN("baseId", ids.toArray()));

    Map<Long, MetricConfigDTO> output = new HashMap<>();
    for (MetricConfigDTO metric : metrics) {
      if (metric != null) {
        output.put(metric.getId(), metric);
      }
    }
    return output;
  }

  @Override
  public Map<String, DatasetConfigDTO> fetchDatasets(Collection<String> datasetNames) {
    List<DatasetConfigDTO> datasets = this.datasetDAO.findByPredicate(Predicate.IN("dataset", datasetNames.toArray()));

    Map<String, DatasetConfigDTO> output = new HashMap<>();
    for (DatasetConfigDTO dataset : datasets) {
      if (dataset != null) {
        output.put(dataset.getDataset(), dataset);
      }
    }
    return output;
  }

  @Override
  public DetectionPipeline loadPipeline(DetectionConfigDTO config, long start, long end) throws Exception {
    return this.loader.from(this, config, start, end);
  }

  @Override
  public MetricConfigDTO fetchMetric(String metricName, String datasetName) {
    return this.metricDAO.findByMetricAndDataset(metricName, datasetName);
  }

  private static Predicate AND(Collection<Predicate> predicates) {
    return Predicate.AND(predicates.toArray(new Predicate[predicates.size()]));
  }

  private static long makeTimeout(long deadline) {
    long diff = deadline - System.currentTimeMillis();
    return diff > 0 ? diff : 0;
  }
}
