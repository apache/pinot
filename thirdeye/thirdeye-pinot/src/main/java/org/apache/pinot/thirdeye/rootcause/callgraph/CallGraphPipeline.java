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

package org.apache.pinot.thirdeye.rootcause.callgraph;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.RequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.MaxScoreSet;
import org.apache.pinot.thirdeye.rootcause.Pipeline;
import org.apache.pinot.thirdeye.rootcause.PipelineContext;
import org.apache.pinot.thirdeye.rootcause.PipelineResult;
import org.apache.pinot.thirdeye.rootcause.impl.DimensionsEntity;
import org.apache.pinot.thirdeye.rootcause.impl.TimeRangeEntity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * CallGraphPipeline explores callgraph datsets based on a provided input filter. It compares
 * graph edges based on the change in average latency between anomaly and baseline time ranges,
 * delivering the top increasing edges as a result.
 */
public class CallGraphPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(CallGraphPipeline.class);

  private static final String COL_TIME = DataFrame.COL_TIME;
  private static final String COL_VALUE = DataFrame.COL_VALUE;
  private static final String COL_COUNT = "count";
  private static final String COL_LATENCY = "latency";
  private static final String COL_AVERAGE = "average";
  private static final String COL_SCORE = "score";

  private static final String COL_CURR_COUNT = "currCount";
  private static final String COL_CURR_LATENCY = "currLatency";
  private static final String COL_CURR_AVERAGE = "currAverage";
  private static final String COL_BASE_COUNT = "baseCount";
  private static final String COL_BASE_LATENCY = "baseLatency";
  private static final String COL_BASE_AVERAGE = "baseAverage";
  private static final String COL_DIFF_COUNT = "diffCount";
  private static final String COL_DIFF_LATENCY = "diffLatency";
  private static final String COL_DIFF_AVERAGE = "diffAverage";

  private static final String PROP_DATASET = "dataset";
  private static final String PROP_DATASET_DEFAULT = "call_graph_average_hourly_additive";

  private static final String PROP_METRIC_COUNT = "metricCount";
  private static final String PROP_METRIC_COUNT_DEFAULT = "count";

  private static final String PROP_METRIC_LATENCY = "metricLatency";
  private static final String PROP_METRIC_LATENCY_DEFAULT = "latency";

  private static final String PROP_INCLUDE_DIMENSIONS = "includeDimensions";

  private static final String PROP_EXCLUDE_DIMENSIONS = "excludeDimensions";

  private static final String PROP_K = "k";
  private static final int PROP_K_DEFAULT = 20;

  private static final String PROP_CUTOFF_FRACTION = "cutoffFraction";
  private static final double PROP_CUTOFF_FRACTION_DEFAULT = 0.01;

  private static final long TIMEOUT = 60000;

  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final QueryCache cache;

  private final String dataset;
  private final String metricCount;
  private final String metricLatency;

  private final Set<String> includeDimensions;
  private final Set<String> excludeDimensions;
  private final int k;

  private final double cutoffFraction;

  public CallGraphPipeline(String outputName, Set<String> inputNames, MetricConfigManager metricDAO,
      DatasetConfigManager datasetDAO, QueryCache cache, String dataset, String metricCount, String metricLatency,
      Set<String> includeDimensions, Set<String> excludeDimensions, int k, double cutoffFraction) {
    super(outputName, inputNames);
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.cache = cache;
    this.dataset = dataset;
    this.metricCount = metricCount;
    this.metricLatency = metricLatency;
    this.includeDimensions = includeDimensions;
    this.excludeDimensions = excludeDimensions;
    this.k = k;
    this.cutoffFraction = cutoffFraction;
  }

  public CallGraphPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    this.dataset = MapUtils.getString(properties, PROP_DATASET, PROP_DATASET_DEFAULT);
    this.metricCount = MapUtils.getString(properties, PROP_METRIC_COUNT, PROP_METRIC_COUNT_DEFAULT);
    this.metricLatency = MapUtils.getString(properties, PROP_METRIC_LATENCY, PROP_METRIC_LATENCY_DEFAULT);
    this.k = MapUtils.getIntValue(properties, PROP_K, PROP_K_DEFAULT);
    this.cutoffFraction = MapUtils.getDoubleValue(properties, PROP_CUTOFF_FRACTION, PROP_CUTOFF_FRACTION_DEFAULT);

    if (properties.containsKey(PROP_INCLUDE_DIMENSIONS)) {
      this.includeDimensions = new HashSet<>((Collection<String>) properties.get(PROP_INCLUDE_DIMENSIONS));
    } else {
      this.includeDimensions = new HashSet<>();
    }

    if (properties.containsKey(PROP_EXCLUDE_DIMENSIONS)) {
      this.excludeDimensions = new HashSet<>((Collection<String>) properties.get(PROP_EXCLUDE_DIMENSIONS));
    } else {
      this.excludeDimensions = new HashSet<>();
    }
  }

  @Override
  public PipelineResult run(PipelineContext pipelineContext) {
    Set<DimensionsEntity> dimensions = filterDimensions(pipelineContext.filter(DimensionsEntity.class));

    TimeRangeEntity anomaly = TimeRangeEntity.getTimeRangeAnomaly(pipelineContext);
    TimeRangeEntity baseline = TimeRangeEntity.getTimeRangeBaseline(pipelineContext);

    DatasetConfigDTO dataset = getDataset(this.dataset);
    MetricConfigDTO metricCount = getMetric(this.dataset, this.metricCount);
    MetricConfigDTO metricLatency = getMetric(this.dataset, this.metricLatency);

    if (dimensions.size() != 1) {
      throw new IllegalArgumentException("Must provide exactly one DimensionsEntity");
    }

    Multimap<String, String> filters = ArrayListMultimap.create(dimensions.iterator().next().getDimensions());
    filters.put(this.metricCount, ">0");
    filters.put(this.metricLatency, ">0");

    List<String> explore = new ArrayList<>(this.getExplore(dataset));

    LOG.info("Filtering '{}' for edges '{}'", this.dataset, filters);
    LOG.info("Exploring dimensions '{}'", explore);

    // data slices
    MetricSlice sliceCurrCount = MetricSlice.from(metricCount.getId(), anomaly.getStart(), anomaly.getEnd(), filters);
    MetricSlice sliceCurrLatency = MetricSlice.from(metricLatency.getId(), anomaly.getStart(), anomaly.getEnd(), filters);
    MetricSlice sliceBaseCount = MetricSlice.from(metricCount.getId(), baseline.getStart(), baseline.getEnd(), filters);
    MetricSlice sliceBaseLatency = MetricSlice.from(metricLatency.getId(), baseline.getStart(), baseline.getEnd(), filters);

    // fetch data
    try {
      // prepare requests
      RequestContainer rcCurrCount = DataFrameUtils
          .makeAggregateRequest(sliceCurrCount, explore, -1, "currCount", this.metricDAO, this.datasetDAO);
      RequestContainer rcCurrLatency = DataFrameUtils
          .makeAggregateRequest(sliceCurrLatency, explore, -1, "currLatency", this.metricDAO, this.datasetDAO);
      RequestContainer rcBaseCount = DataFrameUtils
          .makeAggregateRequest(sliceBaseCount, explore, -1, "baseCount", this.metricDAO, this.datasetDAO);
      RequestContainer rcBaseLatency = DataFrameUtils
          .makeAggregateRequest(sliceBaseLatency, explore, -1, "baseLatency", this.metricDAO, this.datasetDAO);

      // send requests
      Future<ThirdEyeResponse> resCurrCount = this.cache.getQueryResultAsync(rcCurrCount.getRequest());
      Future<ThirdEyeResponse> resCurrLatency = this.cache.getQueryResultAsync(rcCurrLatency.getRequest());
      Future<ThirdEyeResponse> resBaseCount = this.cache.getQueryResultAsync(rcBaseCount.getRequest());
      Future<ThirdEyeResponse> resBaseLatency = this.cache.getQueryResultAsync(rcBaseLatency.getRequest());

      // fetch responses
      final long timeoutTimestamp = System.currentTimeMillis() + TIMEOUT;
      DataFrame dfCurrCount = getResponse(resCurrCount, rcCurrCount, explore, timeoutTimestamp);
      DataFrame dfCurrLatency = getResponse(resCurrLatency, rcCurrLatency, explore, timeoutTimestamp);
      DataFrame dfBaseCount = getResponse(resBaseCount, rcBaseCount, explore, timeoutTimestamp);
      DataFrame dfBaseLatency = getResponse(resBaseLatency, rcBaseLatency, explore, timeoutTimestamp);

      // prepare data
      DataFrame dfCurr = alignResults(dfCurrCount, dfCurrLatency).sortedBy(COL_COUNT);
      DataFrame dfBase = alignResults(dfBaseCount, dfBaseLatency).sortedBy(COL_COUNT);
      LOG.info("Got {} rows for current, {} rows for baseline", dfCurr.size(), dfBase.size());

      // cutoffs
      final long currCutoffCount = (long) (dfCurr.getLongs(COL_COUNT).max().doubleValue() * this.cutoffFraction);
      final long baseCutoffCount = (long) (dfBase.getLongs(COL_COUNT).max().doubleValue() * this.cutoffFraction);
      LOG.info("Cutoff for traffic count is {} for current, {} for baseline", currCutoffCount, baseCutoffCount);

      // join and filter
      DataFrame data = dfCurr
          .renameSeries(COL_COUNT, COL_CURR_COUNT)
          .renameSeries(COL_LATENCY, COL_CURR_LATENCY)
          .renameSeries(COL_AVERAGE, COL_CURR_AVERAGE)
          .joinInner(dfBase
              .renameSeries(COL_COUNT, COL_BASE_COUNT)
              .renameSeries(COL_LATENCY, COL_BASE_LATENCY)
              .renameSeries(COL_AVERAGE, COL_BASE_AVERAGE)
          )
          .filter(new Series.DoubleConditional() { // remove rows with invalid latency
            @Override
            public boolean apply(double... doubles) {
              return doubles[0] >= 0 && doubles[1] >= 0;
            }
          }, COL_CURR_LATENCY, COL_BASE_LATENCY)
          .filter(new Series.LongConditional() { // remove rows below (current) cutoff
            @Override
            public boolean apply(long... longs) {
              return longs[0] >= currCutoffCount;
            }
          }, COL_CURR_COUNT)
          .dropNull();

      // derived data
      data.addSeries(COL_DIFF_COUNT, data.getDoubles(COL_CURR_COUNT).subtract(data.getDoubles(COL_BASE_COUNT)));
      data.addSeries(COL_DIFF_LATENCY, data.getDoubles(COL_CURR_LATENCY).subtract(data.getDoubles(COL_BASE_LATENCY)));
      data.addSeries(COL_DIFF_AVERAGE, data.getDoubles(COL_CURR_AVERAGE).subtract(data.getDoubles(COL_BASE_AVERAGE)));

      // scoring
      data.addSeries(COL_SCORE, data.getDoubles(COL_DIFF_AVERAGE).normalize());

      // top k edges
      DataFrame topk = data.sortedBy(COL_SCORE).reverse().head(this.k);

      LOG.info("topk ({} rows):\n{}", topk.size(), topk.head(10));

      List<Entity> related = new ArrayList<>();
      related.add(anomaly);
      related.add(baseline);

      Set<CallGraphEntity> output = new MaxScoreSet<>();
      for (int i = 0; i < topk.size(); i++) {
        output.add(CallGraphEntity.fromEdge(topk.getDouble(COL_SCORE, i), related, topk.slice(i, i + 1)));
      }

      return new PipelineResult(pipelineContext, output);

    } catch (Exception e) {
      throw new IllegalStateException("Could not process data", e);

    }
  }

  /**
   * Filters DimensionsEntites based on the include and exclude dimension sets configured in the pipeline config.
   *
   * @see CallGraphPipeline#filterDimensions(Set, Set, Set)
   *
   * @param dimensions set of DimensionsEntities
   * @return set of filtered DimensionsEntities
   */
  private Set<DimensionsEntity> filterDimensions(Set<DimensionsEntity> dimensions) {
    return filterDimensions(dimensions, this.includeDimensions, this.excludeDimensions);
  }

  /**
   * Returns the set of dimensoins to explore for a given dataset under the constraint of include and exclude dimensions.
   *
   * @param dataset dataset dto
   * @return set of dimensions to explore
   */
  private Set<String> getExplore(DatasetConfigDTO dataset) {
    Set<String> dimensions = new HashSet<>();

    if (this.includeDimensions.isEmpty()) {
      dimensions.addAll(dataset.getDimensions());

    } else {
      dimensions.addAll(this.includeDimensions);
    }

    dimensions.removeAll(this.excludeDimensions);

    return dimensions;
  }

  /**
   * Returns the metric for a specified metric and dataset name from the database
   *
   * @param dataset dataset name
   * @param metric metric name
   * @return metric dto
   * @throws IllegalArgumentException if the metric cannot be found
   */
  private MetricConfigDTO getMetric(String dataset, String metric) {
    MetricConfigDTO metricDTO = this.metricDAO.findByMetricAndDataset(metric, dataset);
    if (metricDTO == null) {
      throw new IllegalArgumentException(String.format("Could not resolve metric '%s::%s'", dataset, metric));
    }
    return metricDTO;
  }

  /**
   * Returns the dataset for a specified dataset name from the database.
   *
   * @param dataset dataset name
   * @return dataset dto
   * @throws IllegalArgumentException if the dataset cannot be found
   */
  private DatasetConfigDTO getDataset(String dataset) {
    DatasetConfigDTO datasetDTO = this.datasetDAO.findByDataset(dataset);
    if (datasetDTO == null) {
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s'", dataset));
    }
    return datasetDTO;
  }

  /**
   * Filters DimensionsEntites based on the include and exclude dimension sets
   *
   * @param dimensionsEntities set of DimensionsEntities
   * @param include dimensions to include (empty = all)
   * @param exclude dimensions to exclude
   * @return set of filtered DimensionsEntities
   */
  private static Set<DimensionsEntity> filterDimensions(Set<DimensionsEntity> dimensionsEntities, Set<String> include, Set<String> exclude) {
    Set<DimensionsEntity> output = new HashSet<>();
    for (DimensionsEntity de : dimensionsEntities) {
      Multimap<String, String> dimensions = ArrayListMultimap.create();

      if (include.isEmpty()) {
        dimensions.putAll(de.getDimensions());

      } else {
        for (String name : include) {
          dimensions.putAll(name, de.getDimensions().get(name));
        }
      }

      for (String name : exclude) {
        dimensions.removeAll(name);
      }

      output.add(de.withDimensions(dimensions));
    }

    return output;
  }

  /**
   * Returns the time remaining until a given timestamp (in the future).
   *
   * @param timestamp timeout timestamp
   * @return remaining time (in millis)
   */
  private static long makeTimeout(long timestamp) {
    long delta = timestamp - System.currentTimeMillis();
    return delta >= 0 ? delta : 0;
  }

  /**
   * Retrieves and processes a raw aggregation response. drops time column and sets index.
   *
   * @param response thirdeye response
   * @param container request container
   * @param dimensions dimensions to serve as index
   * @param timeoutTimestamp timestamp for ultimate timeout
   * @return response as formatted dataframe
   * @throws Exception
   */
  private static DataFrame getResponse(Future<ThirdEyeResponse> response, RequestContainer container,
      List<String> dimensions, long timeoutTimestamp) throws Exception {
    return DataFrameUtils.evaluateResponse(response.get(makeTimeout(timeoutTimestamp), TimeUnit.MILLISECONDS), container)
        .dropSeries(COL_TIME)
        .setIndex(dimensions);
  }

  /**
   * Joins count and latency data and computes an aligned average
   *
   * @param dfCount count data
   * @param dfLatency latency data
   * @return joined data with average
   */
  private static DataFrame alignResults(DataFrame dfCount, DataFrame dfLatency) {
    return dfCount.renameSeries(COL_VALUE, COL_COUNT)
        .joinInner(dfLatency.renameSeries(COL_VALUE, COL_LATENCY))
        .mapInPlace(new Series.DoubleFunction() {
          @Override
          public double apply(double... doubles) {
            return doubles[0] / doubles[1];
          }
        }, COL_AVERAGE, COL_LATENCY, COL_COUNT);
  }

}
