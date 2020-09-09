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

package org.apache.pinot.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.TimeSeriesRequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.Pipeline;
import org.apache.pinot.thirdeye.rootcause.PipelineContext;
import org.apache.pinot.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
 * The MetricAnalysisPipeline ranks metrics based on the degree of anomalous behavior during
 * the anomaly period. It scores anomalous behavior with user-configured strategies.
 *
 * <br/><b>NOTE:</b> this is the successor to {@code MetricCorrelationRankingPipeline}, and can
 * be used as a drop-in replacement that handles MetricEntities with filter URNs
 *
 * @see MetricCorrelationRankingPipeline
 */
public class MetricAnalysisPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricAnalysisPipeline.class);

  private static final long TIMEOUT = 1200000;

  private static final long TRAINING_OFFSET = TimeUnit.DAYS.toMillis(7);

  private static final String STRATEGY_THRESHOLD = "threshold";

  private static final String PROP_STRATEGY = "strategy";
  private static final String PROP_STRATEGY_DEFAULT = STRATEGY_THRESHOLD;

  private static final String PROP_GRANULARITY = "granularity";
  private static final String PROP_GRANULARITY_DEFAULT = "15_MINUTES";

  private static final String COL_TIME = DataFrame.COL_TIME;
  private static final String COL_VALUE = DataFrame.COL_VALUE;
  private static final String COL_CURRENT = "current";
  private static final String COL_BASELINE = "baseline";

  private final QueryCache cache;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final ScoringStrategyFactory strategyFactory;
  private final TimeGranularity granularity;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param strategyFactory scoring strategy for differences
   * @param granularity time series target granularity
   * @param cache query cache
   * @param metricDAO metric config DAO
   * @param datasetDAO datset config DAO
   */
  public MetricAnalysisPipeline(String outputName, Set<String> inputNames, ScoringStrategyFactory strategyFactory, TimeGranularity granularity, QueryCache cache, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) {
    super(outputName, inputNames);
    this.cache = cache;
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.strategyFactory = strategyFactory;
    this.granularity = granularity;
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_STRATEGY})
   */
  public MetricAnalysisPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();
    this.strategyFactory = parseStrategyFactory(MapUtils.getString(properties, PROP_STRATEGY, PROP_STRATEGY_DEFAULT));
    this.granularity = TimeGranularity.fromString(MapUtils.getString(properties, PROP_GRANULARITY, PROP_GRANULARITY_DEFAULT));
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metrics = context.filter(MetricEntity.class);

    TimeRangeEntity anomalyRange = TimeRangeEntity.getTimeRangeAnomaly(context);
    TimeRangeEntity baselineRange = TimeRangeEntity.getTimeRangeBaseline(context);

    // NOTE: baseline lookback only affects amount of training data, training is always WoW
    // NOTE: data window is aligned to metric time granularity
    final long trainingBaselineStart = baselineRange.getStart() - TRAINING_OFFSET;
    final long trainingBaselineEnd = anomalyRange.getStart() - TRAINING_OFFSET;
    final long trainingCurrentStart = baselineRange.getStart();
    final long trainingCurrentEnd = anomalyRange.getStart();

    final long testBaselineStart = baselineRange.getStart();
    final long testBaselineEnd = baselineRange.getEnd();
    final long testCurrentStart = anomalyRange.getStart();
    final long testCurrentEnd = anomalyRange.getEnd();

    Multimap<String, String> filters = DimensionEntity.makeFilterSet(context);

    LOG.info("Processing {} metrics", metrics.size());

    // generate requests
    List<TimeSeriesRequestContainer> requestList = new ArrayList<>();
    requestList.addAll(makeRequests(metrics, trainingBaselineStart, testCurrentEnd, filters));

    LOG.info("Requesting {} time series", requestList.size());
    List<ThirdEyeRequest> thirdeyeRequests = new ArrayList<>();
    Map<String, TimeSeriesRequestContainer> requests = new HashMap<>();
    for(TimeSeriesRequestContainer rc : requestList) {
      final ThirdEyeRequest req = rc.getRequest();
      requests.put(req.getRequestReference(), rc);
      thirdeyeRequests.add(req);
    }

    Collection<Future<ThirdEyeResponse>> futures = submitRequests(thirdeyeRequests);

    // fetch responses and calculate derived metrics
    int i = 0;
    Map<String, DataFrame> responses = new HashMap<>();
    for(Future<ThirdEyeResponse> future : futures) {
      ThirdEyeResponse response;
      try {
        response = future.get(TIMEOUT, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (Exception e) {
        LOG.warn("Error executing request '{}'. Skipping.", requestList.get(i).getRequest().getRequestReference(), e);
        continue;
      } finally {
        i++;
      }

      // parse time series
      String id = response.getRequest().getRequestReference();
      DataFrame df;
      try {
        df = DataFrameUtils.evaluateResponse(response, requests.get(id));
      } catch (Exception e) {
        LOG.warn("Could not parse response for '{}'. Skipping.", id, e);
        continue;
      }

      // store time series
      responses.put(id, df);
    }

    // score metrics
    Set<MetricEntity> output = new HashSet<>();
    for(MetricEntity me : metrics) {
      try {
        if(!responses.containsKey(me.getUrn())) {
          LOG.warn("Did not receive response for '{}'. Skipping.", me.getUrn());
          continue;
        }

        DataFrame timeseries = responses.get(me.getUrn());
        if (timeseries.isEmpty()) {
          LOG.warn("No data for '{}'. Skipping.", me.getUrn());
          continue;
        }

        LOG.info("Preparing training and test data for metric '{}'", me.getUrn());
        DataFrame trainingBaseline = extractTimeRange(timeseries, trainingBaselineStart, trainingBaselineEnd);
        DataFrame trainingCurrent = extractTimeRange(timeseries, trainingCurrentStart, trainingCurrentEnd);
        DataFrame testBaseline = extractTimeRange(timeseries, testBaselineStart, testBaselineEnd);
        DataFrame testCurrent = extractTimeRange(timeseries, testCurrentStart, testCurrentEnd);

//        LOG.info("timeseries ({} rows): {}", timeseries.size(), timeseries.head(20));
//        LOG.info("trainingBaseline ({} rows): from {} to {}", trainingBaseline.size(), trainingBaselineStart, trainingBaselineEnd);
//        LOG.info("trainingCurrent ({} rows): from {} to {}", trainingCurrent.size(), trainingCurrentStart, trainingCurrentEnd);
//        LOG.info("testBaseline ({} rows): from {} to {}", testBaseline.size(), testBaselineStart, testBaselineEnd);
//        LOG.info("testCurrent ({} rows): from {} to {}", testCurrent.size(), testCurrentStart, testCurrentEnd);

        DataFrame trainingDiff = diffTimeseries(trainingCurrent, trainingBaseline);
        DataFrame testDiff = diffTimeseries(testCurrent, testBaseline);

        double score = this.strategyFactory.fit(trainingDiff).apply(testDiff);

        List<Entity> related = new ArrayList<>();
        related.add(anomalyRange);
        related.add(baselineRange);

        output.add(me.withScore(score).withRelated(related));

      } catch (Exception e) {
        LOG.warn("Error processing '{}'. Skipping", me.getUrn(), e);
      }
    }

    LOG.info("Generated {} MetricEntities with valid scores", output.size());

    return new PipelineResult(context, output);
  }

  private DataFrame extractTimeRange(DataFrame data, final long start, final long end) {
    if (data.size() <= 1) {
      return data;
    }

    final LongSeries timestamps = data.getLongs(COL_TIME);
    final long granularity = timestamps.subtract(timestamps.shift(1)).dropNull().head(1).longValue();
    if ((start / granularity) == (end / granularity)) {
      // not crossing bucket boundary, return nearest lower timestamp
      return data.filterEquals(COL_TIME, (start / granularity) * granularity).dropNull();
    }

    return data.filter(new Series.LongConditional() {
          @Override
          public boolean apply(long... values) {
            return values[0] >= start && values[0] < end;
          }
        }, COL_TIME).dropNull();
  }

  private DataFrame diffTimeseries(DataFrame current, DataFrame baseline) {
    final long currentOffset = current.getLongs(COL_TIME).min().fillNull().longValue();

    DataFrame offsetCurrent = new DataFrame()
        .addSeries(COL_TIME, current.getLongs(COL_TIME).subtract(currentOffset))
        .addSeries(COL_CURRENT, current.getDoubles(COL_VALUE))
        .setIndex(COL_TIME);
    DataFrame offsetBaseline = new DataFrame()
        .addSeries(COL_TIME, baseline.getLongs(COL_TIME).subtract(currentOffset - TRAINING_OFFSET))
        .addSeries(COL_BASELINE, baseline.getDoubles(COL_VALUE))
        .setIndex(COL_TIME);
    DataFrame joined = offsetCurrent.joinInner(offsetBaseline);
    return joined.addSeries(COL_VALUE, joined.getDoubles(COL_CURRENT).subtract(joined.get(COL_BASELINE)))
        .dropSeries(COL_CURRENT, COL_BASELINE);
  }

  private Collection<Future<ThirdEyeResponse>> submitRequests(List<ThirdEyeRequest> thirdeyeRequests) {
    try {
      return this.cache.getQueryResultsAsync(thirdeyeRequests).values();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<TimeSeriesRequestContainer> makeRequests(Collection<MetricEntity> metrics, long start, long end, Multimap<String, String> filters) {
    List<TimeSeriesRequestContainer> requests = new ArrayList<>();
    for(MetricEntity me : metrics) {
      Multimap<String, String> jointFilters = ArrayListMultimap.create();
      jointFilters.putAll(filters);
      jointFilters.putAll(me.getFilters());

      MetricSlice slice = MetricSlice.from(me.getId(), start, end, jointFilters, this.granularity);
      try {
        requests.add(DataFrameUtils.makeTimeSeriesRequestAligned(slice, me.getUrn(), this.metricDAO, this.datasetDAO));
      } catch (Exception ex) {
        LOG.warn(String.format("Could not make request for '%s'. Skipping.", me.getUrn()), ex);
      }
    }
    return requests;
  }

  private static ScoringStrategyFactory parseStrategyFactory(String strategy) {
    switch(strategy) {
      case STRATEGY_THRESHOLD:
        return new ThresholdStrategyFactory(0.90, 0.95, 0.975, 0.99, 1.00);
      default:
        throw new IllegalArgumentException(String.format("Unknown strategy '%s'", strategy));
    }
  }

  private interface ScoringStrategyFactory {
    ScoringStrategy fit(DataFrame training);
  }

  private interface ScoringStrategy {
    double apply(DataFrame data);
  }

  private static class ThresholdStrategyFactory implements ScoringStrategyFactory {
    final double[] quantiles;

    ThresholdStrategyFactory(double... quantiles) {
      this.quantiles = quantiles;
    }

    @Override
    public ScoringStrategy fit(DataFrame training) {
      DoubleSeries train = training.getDoubles(COL_VALUE);
      double[] thresholds = new double[this.quantiles.length];
      for (int i = 0; i < thresholds.length; i++) {
        thresholds[i] = train.abs().quantile(this.quantiles[i]).fillNull(Double.POSITIVE_INFINITY).doubleValue();
      }
      return new ThresholdStrategy(thresholds);
    }
  }

  private static class ThresholdStrategy implements ScoringStrategy {
    final double[] thresholds;

    ThresholdStrategy(double... thresholds) {
      this.thresholds = thresholds;
    }

    @Override
    public double apply(DataFrame data) {
      DoubleSeries test = data.getDoubles(COL_VALUE);

      LOG.info("thresholds: {}", this.thresholds);

      long sumViolations = 0;
      for (final double t : thresholds) {
        sumViolations += test.abs().map(new Series.DoubleConditional() {
          @Override
          public boolean apply(double... values) {
            return values[0] > t;
          }
        }).sum().fillNull().longValue();
      }

      final int max = test.size() * this.thresholds.length;
      if (max <= 0) {
        return 0;
      }

      return sumViolations / (double) max;
    }
  }
}
