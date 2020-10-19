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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.EvaluationManager;
import org.apache.pinot.thirdeye.datalayer.bao.EventManager;
import org.apache.pinot.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EventDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.util.Predicate;
import org.apache.pinot.thirdeye.datasource.loader.AggregationLoader;
import org.apache.pinot.thirdeye.datasource.loader.TimeSeriesLoader;
import org.apache.pinot.thirdeye.detection.cache.builder.AnomaliesCacheBuilder;
import org.apache.pinot.thirdeye.detection.cache.builder.TimeSeriesCacheBuilder;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.detection.spi.model.EvaluationSlice;
import org.apache.pinot.thirdeye.detection.spi.model.EventSlice;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDataProvider implements DataProvider {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultDataProvider.class);
  private static final long TIMEOUT = 60000;

  private final ExecutorService executor = Executors.newCachedThreadPool();

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final EventManager eventDAO;
  private final MergedAnomalyResultManager anomalyDAO;
  private final EvaluationManager evaluationDAO;
  private final TimeSeriesLoader timeseriesLoader;
  private final AggregationLoader aggregationLoader;
  private final DetectionPipelineLoader loader;

  private final TimeSeriesCacheBuilder timeseriesCache;
  private final AnomaliesCacheBuilder anomaliesCache;

  public DefaultDataProvider(MetricConfigManager metricDAO, DatasetConfigManager datasetDAO, EventManager eventDAO,
      MergedAnomalyResultManager anomalyDAO, EvaluationManager evaluationDAO, TimeSeriesLoader timeseriesLoader,
      AggregationLoader aggregationLoader, DetectionPipelineLoader loader, TimeSeriesCacheBuilder timeseriesCache,
      AnomaliesCacheBuilder anomaliesCache) {
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.eventDAO = eventDAO;
    this.anomalyDAO = anomalyDAO;
    this.evaluationDAO = evaluationDAO;
    this.timeseriesLoader = timeseriesLoader;
    this.aggregationLoader = aggregationLoader;
    this.loader = loader;
    this.timeseriesCache = timeseriesCache;
    this.anomaliesCache = anomaliesCache;
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchTimeseries(Collection<MetricSlice> slices) {
    try {
      Map<MetricSlice, MetricSlice> alignedMetricSlicesToOriginalSlice = new HashMap<>();
      for (MetricSlice slice: slices) {
        alignedMetricSlicesToOriginalSlice.put(alignSlice(slice), slice);
      }
      Map<MetricSlice, DataFrame> cacheResult = timeseriesCache.fetchSlices(alignedMetricSlicesToOriginalSlice.keySet());
      Map<MetricSlice, DataFrame> timeseriesResult = new HashMap<>();
      for (Map.Entry<MetricSlice, DataFrame> entry : cacheResult.entrySet()) {
        // make a copy of the result so that cache won't be contaminated by client code
        timeseriesResult.put(alignedMetricSlicesToOriginalSlice.get(entry.getKey()), entry.getValue().copy());
      }
      return timeseriesResult;
    } catch (Exception e) {
      throw new RuntimeException("fetch time series failed", e);
    }
  }

  @Override
  public Map<MetricSlice, DataFrame> fetchAggregates(Collection<MetricSlice> slices, final List<String> dimensions, int limit) {
    try {
      Map<MetricSlice, Future<DataFrame>> futures = new HashMap<>();
      for (final MetricSlice slice : slices) {
        futures.put(slice, this.executor.submit(
            () -> DefaultDataProvider.this.aggregationLoader.loadAggregate(slice, dimensions, limit)));
      }

      final long deadline = System.currentTimeMillis() + TIMEOUT;
      Map<MetricSlice, DataFrame> output = new HashMap<>();
      for (MetricSlice slice : slices) {
        DataFrame result = futures.get(slice).get(DetectionUtils.makeTimeout(deadline), TimeUnit.MILLISECONDS);
        // fill in time stamps
        result.dropSeries(DataFrame.COL_TIME).addSeries(
            DataFrame.COL_TIME, LongSeries.fillValues(result.size(), slice.getStart())).setIndex(
            DataFrame.COL_TIME);
        output.put(slice, result);
      }
      return output;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetch all anomalies based on the request Anomaly Slices (overlap with slice window)
   */
  @Override
  public Multimap<AnomalySlice, MergedAnomalyResultDTO> fetchAnomalies(Collection<AnomalySlice> slices) {
    Multimap<AnomalySlice, MergedAnomalyResultDTO> output = ArrayListMultimap.create();
    try {
      for (AnomalySlice slice : slices) {
        Collection<MergedAnomalyResultDTO> cacheResult = anomaliesCache.fetchSlice(slice);

        // make a copy of the result so that cache won't be contaminated by client code
        List<MergedAnomalyResultDTO> clonedAnomalies = new ArrayList<>();
        for (MergedAnomalyResultDTO anomaly : cacheResult) {
          clonedAnomalies.add((MergedAnomalyResultDTO) SerializationUtils.clone(anomaly));
        }

        LOG.info("Fetched {} anomalies for slice {}", clonedAnomalies.size(), slice);
        output.putAll(slice, clonedAnomalies);
      }

      return output;
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch anomalies from database.", e);
    }
  }

  @Override
  public Multimap<EventSlice, EventDTO> fetchEvents(Collection<EventSlice> slices) {
    Multimap<EventSlice, EventDTO> output = ArrayListMultimap.create();
    for (EventSlice slice : slices) {
      List<Predicate> predicates = DetectionUtils.buildPredicatesOnTime(slice.getStart(), slice.getEnd());

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
  public DetectionPipeline loadPipeline(DetectionConfigDTO config, long start, long end) {
    return this.loader.from(this, config, start, end);
  }

  @Override
  public MetricConfigDTO fetchMetric(String metricName, String datasetName) {
    return this.metricDAO.findByMetricAndDataset(metricName, datasetName);
  }

  @Override
  public Multimap<EvaluationSlice, EvaluationDTO> fetchEvaluations(Collection<EvaluationSlice> slices, long configId) {
    Multimap<EvaluationSlice, EvaluationDTO> output = ArrayListMultimap.create();
    for (EvaluationSlice slice : slices) {
      List<Predicate> predicates = DetectionUtils.buildPredicatesOnTime(slice.getStart(), slice.getEnd());
      if (predicates.isEmpty())
        throw new IllegalArgumentException("Must provide at least one of start, or end");

      if (configId >= 0) {
        predicates.add(Predicate.EQ("detectionConfigId", configId));
      }
      List<EvaluationDTO> evaluations = this.evaluationDAO.findByPredicate(AND(predicates));
      output.putAll(slice, evaluations.stream().filter(slice::match).collect(Collectors.toList()));
    }
    return output;
  }

  private static Predicate AND(Collection<Predicate> predicates) {
    return Predicate.AND(predicates.toArray(new Predicate[predicates.size()]));
  }

  /**
   * Aligns a metric slice based on its granularity, or the dataset granularity.
   *
   * @param slice metric slice
   * @return aligned metric slice
   */
  private MetricSlice alignSlice(MetricSlice slice) {
    MetricConfigDTO metric = this.metricDAO.findById(slice.getMetricId());
    if (metric == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", slice.getMetricId()));
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id %d", metric.getDataset(), slice.getMetricId()));
    }

    TimeGranularity granularity = dataset.bucketTimeGranularity();
    if (!MetricSlice.NATIVE_GRANULARITY.equals(slice.getGranularity())) {
      granularity = slice.getGranularity();
    }

    // align to time buckets and request time zone
    // if granularity is more than 1 day, align to the daily boundary
    // this alignment is required by the Pinot datasource, otherwise, it may return wrong results
    long offset = DateTimeZone.forID(dataset.getTimezone()).getOffset(slice.getStart());
    long timeGranularity = Math.min(granularity.toMillis(), TimeUnit.DAYS.toMillis(1));
    long start = ((slice.getStart() + offset)/ timeGranularity) * timeGranularity - offset;
    long end = ((slice.getEnd() + offset + timeGranularity - 1) / timeGranularity) * timeGranularity - offset;

    return slice.withStart(start).withEnd(end).withGranularity(granularity);
  }

  @Override
  public  List<DatasetConfigDTO> fetchDatasetByDisplayName(String datasetDisplayName) {
    List<DatasetConfigDTO> dataset = this.datasetDAO.findByPredicate(Predicate.EQ("displayName", datasetDisplayName));
    return dataset;
  }

  public void cleanCache() {
    if (timeseriesCache != null) {
      timeseriesCache.cleanCache();
    }

    if (anomaliesCache != null) {
      anomaliesCache.cleanCache();
    }
  }
}
