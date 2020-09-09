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

import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.TimeSeriesRequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.Pipeline;
import org.apache.pinot.thirdeye.rootcause.PipelineContext;
import org.apache.pinot.thirdeye.rootcause.PipelineResult;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregate;
import org.apache.pinot.thirdeye.rootcause.timeseries.BaselineAggregateType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.joda.time.PeriodType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The MetricAnalysisPipeline ranks metrics based on the degree of anomalous behavior during
 * the anomaly period. It scores anomalous behavior with user-configured strategies.
 *
 * <br/><b>NOTE:</b> this is the successor to {@code MetricAnalysisPipeline}. It supports computation
 * of complex, named training windows.
 *
 * @see MetricAnalysisPipeline
 */
public class MetricAnalysisPipeline2 extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricAnalysisPipeline2.class);

  private static final long TIMEOUT = 60000;

  private static final long TRAINING_WINDOW = TimeUnit.DAYS.toMillis(7);

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
  public MetricAnalysisPipeline2(String outputName, Set<String> inputNames, ScoringStrategyFactory strategyFactory, TimeGranularity granularity, QueryCache cache, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) {
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
  public MetricAnalysisPipeline2(String outputName, Set<String> inputNames, Map<String, Object> properties) {
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

    BaselineAggregate rangeCurrent = BaselineAggregate.fromOffsets(BaselineAggregateType.MEDIAN, Collections.singletonList(new Period(0, PeriodType.weeks())), DateTimeZone.UTC);
    BaselineAggregate rangeBaseline = BaselineAggregate.fromWeekOverWeek(BaselineAggregateType.MEDIAN, 4, 0, DateTimeZone.UTC);

    Map<MetricEntity, MetricSlice> trainingSet = new HashMap<>();
    Map<MetricEntity, MetricSlice> testSet = new HashMap<>();
    Set<MetricSlice> slicesRaw = new HashSet<>();
    for (MetricEntity me : metrics) {
      MetricSlice sliceTrain = MetricSlice.from(me.getId(), anomalyRange.getStart() - TRAINING_WINDOW, anomalyRange.getStart(), me.getFilters(), this.granularity);
      MetricSlice sliceTest = MetricSlice.from(me.getId(), anomalyRange.getStart(), anomalyRange.getEnd(), me.getFilters(), this.granularity);

      trainingSet.put(me, sliceTrain);
      testSet.put(me, sliceTest);

      // TODO make training data cacheable (e.g. align to hours, days)

      printSlices(rangeCurrent.scatter(sliceTest));
      printSlices(rangeCurrent.scatter(sliceTrain));
      printSlices(rangeBaseline.scatter(sliceTest));
      printSlices(rangeBaseline.scatter(sliceTrain));

      slicesRaw.addAll(rangeCurrent.scatter(sliceTest));
      slicesRaw.addAll(rangeCurrent.scatter(sliceTrain));
      slicesRaw.addAll(rangeBaseline.scatter(sliceTest));
      slicesRaw.addAll(rangeBaseline.scatter(sliceTrain));
    }

    printSlices(slicesRaw);

    List<TimeSeriesRequestContainer> requestList = makeRequests(slicesRaw);

//    // NOTE: baseline lookback only affects amount of training data, training is always WoW
//    // NOTE: data window is aligned to metric time granularity
//    final long trainingBaselineStart = baselineRange.getStart() - TRAINING_OFFSET;
//    final long trainingBaselineEnd = anomalyRange.getStart() - TRAINING_OFFSET;
//    final long trainingCurrentStart = baselineRange.getStart();
//    final long trainingCurrentEnd = anomalyRange.getStart();
//
//    final long testBaselineStart = baselineRange.getStart();
//    final long testBaselineEnd = baselineRange.getEnd();
//    final long testCurrentStart = anomalyRange.getStart();
//    final long testCurrentEnd = anomalyRange.getEnd();
//
//    Multimap<String, String> filters = DimensionEntity.makeFilterSet(context);
//
//    LOG.info("Processing {} metrics", metrics.size());
//
//    // generate requests
//    List<TimeSeriesRequestContainer> requestList = new ArrayList<>();
//    requestList.addAll(makeRequests(metrics, trainingBaselineStart, testCurrentEnd, filters));

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

    // Collect slices and requests
    Map<String, MetricSlice> id2slice = new HashMap<>();
    for (MetricSlice slice : slicesRaw) {
      id2slice.put(makeIdentifier(slice), slice);
    }

    Map<MetricSlice, DataFrame> data = new HashMap<>();
    for (Map.Entry<String, DataFrame> entry : responses.entrySet()) {
      MetricSlice slice = id2slice.get(entry.getKey());
      if (slice == null) {
        LOG.warn("Could not associate response id '{}' with request. Skipping.", entry.getKey());
        continue;
      }

      data.put(slice, entry.getValue());
    }

    // score metrics
    Set<MetricEntity> output = new HashSet<>();
    for(MetricEntity me : metrics) {
      try {
        MetricSlice sliceTest = testSet.get(me);
        MetricSlice sliceTrain = trainingSet.get(me);

        boolean isDailyData = this.isDailyData(sliceTest.getMetricId());

        DataFrame testCurrent = rangeCurrent.gather(sliceTest, data);
        DataFrame testBaseline = rangeBaseline.gather(sliceTest, data);
        DataFrame trainingCurrent = rangeCurrent.gather(sliceTrain, data);
        DataFrame trainingBaseline = rangeBaseline.gather(sliceTrain, data);

//        LOG.info("Preparing training and test data for metric '{}'", me.getUrn());
//        DataFrame trainingBaseline = extractTimeRange(timeseries, trainingBaselineStart, trainingBaselineEnd);
//        DataFrame trainingCurrent = extractTimeRange(timeseries, trainingCurrentStart, trainingCurrentEnd);
//        DataFrame testBaseline = extractTimeRange(timeseries, testBaselineStart, testBaselineEnd);
//        DataFrame testCurrent = extractTimeRange(timeseries, testCurrentStart, testCurrentEnd);

//        LOG.info("timeseries ({} rows): {}", timeseries.size(), timeseries.head(20));
//        LOG.info("trainingBaseline ({} rows): from {} to {}", trainingBaseline.size(), trainingBaselineStart, trainingBaselineEnd);
//        LOG.info("trainingCurrent ({} rows): from {} to {}", trainingCurrent.size(), trainingCurrentStart, trainingCurrentEnd);
//        LOG.info("testBaseline ({} rows): from {} to {}", testBaseline.size(), testBaselineStart, testBaselineEnd);
//        LOG.info("testCurrent ({} rows): from {} to {}", testCurrent.size(), testCurrentStart, testCurrentEnd);

        DataFrame trainingDiff = diffTimeseries(trainingCurrent, trainingBaseline);
        DataFrame testDiff = diffTimeseries(testCurrent, testBaseline);

        LOG.info("trainingBaseline ({} rows):\n{}", trainingBaseline.size(), trainingBaseline);
        LOG.info("trainingCurrent ({} rows):\n{}", trainingCurrent.size(), trainingCurrent);
        LOG.info("testBaseline ({} rows):\n{}", testBaseline.size(), testBaseline);
        LOG.info("testCurrent ({} rows):\n{}", testCurrent.size(), testCurrent);
        LOG.info("trainingDiff ({} rows):\n{}", trainingDiff.size(), trainingDiff);
        LOG.info("testDiff ({} rows):\n{}", testDiff.size(), testDiff);

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

  private void printSlices(Collection<MetricSlice> slicesRaw) {
    List<MetricSlice> slices = new ArrayList<>(slicesRaw);
    Collections.sort(slices, new Comparator<MetricSlice>() {
      @Override
      public int compare(MetricSlice o1, MetricSlice o2) {
        return Long.compare(o1.getStart(), o2.getStart());
      }
    });
    LOG.info("Fetching {} slices:\n{}", slices.size(), StringUtils.join(slices, "\n"));
  }

  private boolean isDailyData(long metricId) {
    MetricConfigDTO metric  = this.metricDAO.findById(metricId);
    if (metric == null) {
      return false;
    }

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if (dataset == null) {
      return false;
    }

    return TimeUnit.DAYS.equals(dataset.bucketTimeGranularity().getUnit());
  }

  private DataFrame diffTimeseries(DataFrame current, DataFrame baseline) {
    DataFrame offsetCurrent = new DataFrame(COL_TIME, current.getLongs(COL_TIME))
        .addSeries(COL_CURRENT, current.getDoubles(COL_VALUE));
    DataFrame offsetBaseline = new DataFrame(COL_TIME, baseline.getLongs(COL_TIME))
        .addSeries(COL_BASELINE, baseline.getDoubles(COL_VALUE));
    DataFrame joined = offsetCurrent.joinInner(offsetBaseline);
    joined.addSeries(COL_VALUE, joined.getDoubles(COL_CURRENT).subtract(joined.getDoubles(COL_BASELINE)));
    return joined;
  }

  private Collection<Future<ThirdEyeResponse>> submitRequests(List<ThirdEyeRequest> thirdeyeRequests) {
    try {
      return this.cache.getQueryResultsAsync(thirdeyeRequests).values();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<TimeSeriesRequestContainer> makeRequests(Collection<MetricSlice> slices) {
    List<TimeSeriesRequestContainer> requests = new ArrayList<>();
    for(MetricSlice slice : slices) {
      try {
        requests.add(DataFrameUtils.makeTimeSeriesRequestAligned(slice, makeIdentifier(slice), this.metricDAO, this.datasetDAO));
      } catch (Exception ex) {
        LOG.warn(String.format("Could not make request. Skipping."), ex);
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

  private static String makeIdentifier(MetricSlice slice) {
    return String.valueOf(slice.hashCode());
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
