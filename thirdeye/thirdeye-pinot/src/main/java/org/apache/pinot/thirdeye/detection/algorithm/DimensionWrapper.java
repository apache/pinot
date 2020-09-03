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

package org.apache.pinot.thirdeye.detection.algorithm;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.thirdeye.common.utils.MetricUtils;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.DetectionConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.EvaluationDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datalayer.pojo.MetricConfigBean;
import org.apache.pinot.thirdeye.detection.ConfigUtils;
import org.apache.pinot.thirdeye.detection.DataProvider;
import org.apache.pinot.thirdeye.detection.DetectionPipeline;
import org.apache.pinot.thirdeye.detection.DetectionPipelineException;
import org.apache.pinot.thirdeye.detection.DetectionPipelineResult;
import org.apache.pinot.thirdeye.detection.DetectionUtils;
import org.apache.pinot.thirdeye.detection.PredictionResult;
import org.apache.pinot.thirdeye.detection.cache.CacheConfig;
import org.apache.pinot.thirdeye.detection.spi.exception.DetectorDataInsufficientException;
import org.apache.pinot.thirdeye.detection.spi.model.AnomalySlice;
import org.apache.pinot.thirdeye.rootcause.impl.MetricEntity;
import org.apache.pinot.thirdeye.util.ThirdEyeUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Detection pipeline for dimension exploration with a configurable nested detection pipeline.
 * Loads and prunes a metric's dimensions and sequentially retrieves data to run detection on
 * each filtered time series.
 */
public class DimensionWrapper extends DetectionPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(DimensionWrapper.class);

  // prototyping
  private static final String PROP_NESTED = "nested";

  private static final String PROP_NESTED_METRIC_URN_KEY = "nestedMetricUrnKey";
  private static final String PROP_NESTED_METRIC_URN_KEY_DEFAULT = "metricUrn";

  private static final String PROP_NESTED_METRIC_URNS = "nestedMetricUrns";

  private static final String PROP_CLASS_NAME = "className";
  private static final String PROP_CACHE_PERIOD_LOOKBACK = "cachingPeriodLookback";

  // Max number of dimension combinations we can handle.
  private static final int MAX_DIMENSION_COMBINATIONS = 20000;

  // Stop running if the first several dimension combinations all failed.
  private static final int EARLY_STOP_THRESHOLD = 10;
  private static final double EARLY_STOP_THRESHOLD_PERCENTAGE = 0.1;

  // by default, don't pre-fetch data into the cache
  private static final long DEFAULT_CACHING_PERIOD_LOOKBACK = -1;

  // the max number of dimensions to calculate the evaluations for
  // this is to prevent storing the evaluations for too many dimensions
  private static final int DIMENSION_EVALUATION_LIMIT = 5;

  private final String metricUrn;
  private final int k;
  private final double minContribution;
  private final double minValue;
  private final double minValueHourly;
  private final double maxValueHourly;
  private final double minValueDaily;
  private final double maxValueDaily;
  private final double minLiveZone;
  private final double liveBucketPercentageThreshold;
  private final Period lookback;
  private final DateTimeZone timezone;
  private DateTime start;
  private DateTime end;

  protected final String nestedMetricUrnKey;
  protected final List<String> dimensions;
  protected final Collection<String> nestedMetricUrns;
  // the metric urn to calculate the evaluation metrics for, by default set to top 5 dimensions
  private final Set<String> evaluationMetricUrns;

  protected final List<Map<String, Object>> nestedProperties;

  public DimensionWrapper(DataProvider provider, DetectionConfigDTO config, long startTime, long endTime) {
    super(provider, config, startTime, endTime);

    // the metric used in dimension exploration
    this.metricUrn = MapUtils.getString(config.getProperties(), "metricUrn", null);

    this.minContribution = MapUtils.getDoubleValue(config.getProperties(), "minContribution", Double.NaN);
    this.minValue = MapUtils.getDoubleValue(config.getProperties(), "minValue", Double.NaN);
    this.minValueHourly = MapUtils.getDoubleValue(config.getProperties(), "minValueHourly", Double.NaN);
    this.maxValueHourly = MapUtils.getDoubleValue(config.getProperties(), "maxValueHourly", Double.NaN);
    this.minValueDaily = MapUtils.getDoubleValue(config.getProperties(), "minValueDaily", Double.NaN);
    this.maxValueDaily = MapUtils.getDoubleValue(config.getProperties(), "maxValueDaily", Double.NaN);
    this.k = MapUtils.getIntValue(config.getProperties(), "k", -1);
    this.dimensions = ConfigUtils.getList(config.getProperties().get("dimensions"));
    this.lookback = ConfigUtils.parsePeriod(MapUtils.getString(config.getProperties(), "lookback", "1w"));
    this.timezone = DateTimeZone.forID(MapUtils.getString(config.getProperties(), "timezone", "America/Los_Angeles"));

    /*
     * A bucket of the time series is taken into consider only if its value is above the minLiveZone. In other words,
     * if a bucket's value is smaller than minLiveZone, then this bucket is ignored when calculating the average value.
     * Used for outlier removal. Replace legacy average threshold filter.
     */
    this.minLiveZone = MapUtils.getDoubleValue(config.getProperties(), "minLiveZone", Double.NaN);
    this.liveBucketPercentageThreshold = MapUtils.getDoubleValue(config.getProperties(), "liveBucketPercentageThreshold", 0.5);

    // the metric to run the detection for
    this.nestedMetricUrns = ConfigUtils.getList(config.getProperties().get(PROP_NESTED_METRIC_URNS), Collections.singletonList(this.metricUrn));
    this.nestedMetricUrnKey = MapUtils.getString(config.getProperties(), PROP_NESTED_METRIC_URN_KEY, PROP_NESTED_METRIC_URN_KEY_DEFAULT);
    this.nestedProperties = ConfigUtils.getList(config.getProperties().get(PROP_NESTED));

    this.start = new DateTime(this.startTime, this.timezone);
    this.end = new DateTime(this.endTime, this.timezone);

    DateTime minStart = this.end.minus(this.lookback);
    if (minStart.isBefore(this.start)) {
      this.start = minStart;
    }

    this.evaluationMetricUrns = new HashSet<>();
  }

  /**
   * Run Dimension explore and return explored metrics.
   *
   * @return List of metrics to process.
   */
  private List<MetricEntity> dimensionExplore() {
    List<MetricEntity> nestedMetrics = new ArrayList<>();

    if (this.metricUrn != null) {
      // metric and dimension exploration
      Period testPeriod = new Period(this.start, this.end);

      MetricEntity metric = MetricEntity.fromURN(this.metricUrn);
      MetricConfigDTO metricConfig = this.provider.fetchMetrics(Collections.singleton(metric.getId())).get(metric.getId());
      MetricSlice slice = MetricSlice.from(metric.getId(), this.start.getMillis(), this.end.getMillis(), metric.getFilters());

      // We can push down the top k filter if min contribution is not defined.
      // Otherwise it is not accurate to calculate the contribution.
      int limit = -1;
      if (Double.isNaN(this.minContribution) && this.k > 0) {
        limit = this.k;
      }
      DataFrame aggregates = this.provider.fetchAggregates(Collections.singletonList(slice), this.dimensions, limit).get(slice);

      if (aggregates.isEmpty()) {
        return nestedMetrics;
      }

      final double total = aggregates.getDoubles(COL_VALUE).sum().fillNull().doubleValue();

      // min contribution
      if (!Double.isNaN(this.minContribution)) {
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).divide(total).gte(this.minContribution)).dropNull();
      }

      // min value
      // check min value if only min live zone not set, other wise use checkMinLiveZone below
      if (!Double.isNaN(this.minValue) && Double.isNaN(this.minLiveZone)) {
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).gte(this.minValue)).dropNull();
      }

      double hourlyMultiplier = MetricUtils.isAggCumulative(metricConfig) ?
          (TimeUnit.HOURS.toMillis(1) / (double) testPeriod.toDurationFrom(start).getMillis()) : 1.0;
      double dailyMultiplier = MetricUtils.isAggCumulative(metricConfig) ?
              (TimeUnit.DAYS.toMillis(1) / (double) testPeriod.toDurationFrom(start).getMillis()) : 1.0;

      if (!Double.isNaN(this.minValueHourly)) {
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).multiply(hourlyMultiplier).gte(this.minValueHourly)).dropNull();
      }

      if (!Double.isNaN(this.maxValueHourly)) {
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).multiply(hourlyMultiplier).lte(this.maxValueHourly)).dropNull();
      }

      if (!Double.isNaN(this.minValueDaily)) {
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).multiply(dailyMultiplier).gte(this.minValueDaily)).dropNull();
      }

      if (!Double.isNaN(this.maxValueDaily)) {
        aggregates = aggregates.filter(aggregates.getDoubles(COL_VALUE).multiply(dailyMultiplier).lte(this.maxValueDaily)).dropNull();
      }

      aggregates = aggregates.sortedBy(COL_VALUE).reverse();
      // top k
      if (this.k > 0) {
        aggregates = aggregates.head(this.k);
      }

      for (String nestedMetricUrn : this.nestedMetricUrns) {
        for (int i = 0; i < aggregates.size(); i++) {
          Multimap<String, String> nestedFilter = ArrayListMultimap.create(metric.getFilters());

          for (String dimName : this.dimensions) {
            nestedFilter.removeAll(dimName); // clear any filters for explored dimension
            nestedFilter.put(dimName, aggregates.getString(dimName, i));
          }

          MetricEntity me = MetricEntity.fromURN(nestedMetricUrn).withFilters(nestedFilter);
          nestedMetrics.add(me);
          if (i < DIMENSION_EVALUATION_LIMIT) {
            evaluationMetricUrns.add(me.getUrn());
          }
        }
      }

    } else {
      // metric exploration only

      for (String nestedMetricUrn : this.nestedMetricUrns) {
        nestedMetrics.add(MetricEntity.fromURN(nestedMetricUrn));
        evaluationMetricUrns.add(nestedMetricUrn);
      }
    }

    if (!Double.isNaN(this.minLiveZone) && !Double.isNaN(this.minValue)) {
      // filters all nested metric that didn't pass live zone check
      nestedMetrics = nestedMetrics.stream().filter(metricEntity -> checkMinLiveZone(metricEntity)).collect(Collectors.toList());
    }

    return nestedMetrics;
  }

  @Override
  public DetectionPipelineResult run() throws Exception {
    List<MetricEntity> nestedMetrics = dimensionExplore();
    if (nestedMetrics.isEmpty()) {
      return new DetectionPipelineResult(Collections.<MergedAnomalyResultDTO>emptyList(), -1);
    }
    if (nestedMetrics.size() > MAX_DIMENSION_COMBINATIONS) {
      throw new DetectionPipelineException(String.format(
          "Dimension combination for {} is {} which exceeds limit of {}",
          this.config.getId(), nestedMetrics.size(), MAX_DIMENSION_COMBINATIONS));
    }

    // don't use in-memory cache for dimension exploration, it will cause thrashing
    // we add a "duplicate" condition so that tests can run. will fix tests later on
    if (CacheConfig.getInstance().useCentralizedCache() && !CacheConfig.getInstance().useInMemoryCache()) {
      Map<Long, MetricConfigDTO> metricIdToConfigMap =
          this.provider.fetchMetrics(nestedMetrics.stream().map(MetricEntity::getId).collect(Collectors.toSet()));
      Map<String, DatasetConfigDTO> datasetToConfigMap = this.provider.fetchDatasets(
          metricIdToConfigMap.values().stream().map(MetricConfigBean::getDataset).collect(Collectors.toSet()));
      // group the metric entities by dataset
      Map<DatasetConfigDTO, List<MetricEntity>> nestedMetricsByDataset = nestedMetrics.stream()
          .collect(Collectors.groupingBy(metric -> datasetToConfigMap.get(metricIdToConfigMap.get(metric.getId()).getDataset()),
              Collectors.toList()));

      long cachingPeriodLookback = config.getProperties().containsKey(PROP_CACHE_PERIOD_LOOKBACK) ? MapUtils.getLong(config.getProperties(),
          PROP_CACHE_PERIOD_LOOKBACK) : DEFAULT_CACHING_PERIOD_LOOKBACK;
      // prefetch each
      for (Map.Entry<DatasetConfigDTO, List<MetricEntity>> entry : nestedMetricsByDataset.entrySet()) {
        // if cachingPeriodLookback is set in the config, use that value.
        // otherwise, set the lookback period based on data granularity.
        long metricLookbackPeriod = cachingPeriodLookback < 0 ? ThirdEyeUtils.getCachingPeriodLookback(entry.getKey().bucketTimeGranularity()) : cachingPeriodLookback;

        this.provider.fetchTimeseries(entry.getValue()
            .stream()
            .map(metricEntity -> MetricSlice.from(metricEntity.getId(), startTime - metricLookbackPeriod, endTime,
                metricEntity.getFilters()))
            .collect(Collectors.toList()));
      }
    }

    List<MergedAnomalyResultDTO> anomalies = new ArrayList<>();
    List<PredictionResult> predictionResults = new ArrayList<>();
    Map<String, Object> diagnostics = new HashMap<>();
    Set<Long> lastTimeStamps = new HashSet<>();

    warmUpAnomaliesCache(nestedMetrics);

    long totalNestedMetrics = nestedMetrics.size();
    long successNestedMetrics = 0; // record the number of successfully explored dimensions
    List<Exception> exceptions = new ArrayList<>();
    LOG.info("Run detection for {} metrics", totalNestedMetrics);
    for (int i = 0; i < totalNestedMetrics; i++) {
      checkEarlyStop(totalNestedMetrics, successNestedMetrics, i, exceptions);
      MetricEntity metric = nestedMetrics.get(i);
      List<Exception> exceptionsForNestedMetric = new ArrayList<>();
      LOG.info("running detection for metric urn {}. {}/{}", metric.getUrn(), i + 1, totalNestedMetrics);
      if (Thread.interrupted()) {
        /* This happens when Future.cancel() or ExecutorService.shutdown() is called. Since dimension wrapper run
         under ExecutorService, it might take too long to compute all dimensions before it gets timeout and cancelled.
         In that case, this method just returns all the computed dimensions and skips the rest of them.
         */
        LOG.error("Current thread is interrupted before running dimension {}/{} for detection {} metrics {}, "
                + "so skip the rest of dimensions and return the intermediate",
            i + 1 , totalNestedMetrics, this.config.getId(), metric.getUrn());
        break;
      }
      for (Map<String, Object> properties : this.nestedProperties) {
        DetectionPipelineResult intermediate;
        try {
          properties.put(this.nestedMetricUrnKey, metric.getUrn());
          intermediate = this.runNested(properties, this.startTime, this.endTime);
        } catch (Exception e) {
          LOG.warn("[DetectionConfigID{}] detecting anomalies for window {} to {} failed for metric urn {}.",
              this.config.getId(), this.start, this.end, metric.getUrn(), e);
          exceptionsForNestedMetric.add(e);
          continue;
        }
        lastTimeStamps.add(intermediate.getLastTimestamp());
        anomalies.addAll(intermediate.getAnomalies());
        diagnostics.put(metric.getUrn(), intermediate.getDiagnostics());
        predictionResults.addAll(intermediate.getPredictions());
      }
      // for one dimension, if all detectors run successfully, mark the dimension as successful
      if (exceptionsForNestedMetric.isEmpty()) {
        successNestedMetrics++;
      }
      exceptions.addAll(exceptionsForNestedMetric);
    }

    checkNestedMetricsStatus(totalNestedMetrics, successNestedMetrics, exceptions, predictionResults);
    return new DetectionPipelineResult(anomalies, DetectionUtils.consolidateNestedLastTimeStamps(lastTimeStamps),
        predictionResults, calculateEvaluationMetrics(predictionResults)).setDiagnostics(diagnostics);
  }

  private DatasetConfigDTO retrieveDatasetConfig(List<MetricEntity> nestedMetrics) {
    // All nested metrics after dimension explore belong to the same dataset
    long metricId = nestedMetrics.get(0).getId();
    MetricConfigDTO metricConfigDTO = this.provider.fetchMetrics(Collections.singletonList(metricId)).get(metricId);

    return this.provider.fetchDatasets(Collections.singletonList(metricConfigDTO.getDataset()))
        .get(metricConfigDTO.getDataset());
  }

  private void warmUpAnomaliesCache(List<MetricEntity> nestedMetrics) {
    DatasetConfigDTO dataset = retrieveDatasetConfig(nestedMetrics);
    long cachingPeriodLookback = config.getProperties().containsKey(PROP_CACHE_PERIOD_LOOKBACK) ?
        MapUtils.getLong(config.getProperties(), PROP_CACHE_PERIOD_LOOKBACK) : ThirdEyeUtils
        .getCachingPeriodLookback(dataset.bucketTimeGranularity());

    long paddingBuffer = TimeUnit.DAYS.toMillis(1);
    AnomalySlice anomalySlice = new AnomalySlice()
        .withDetectionId(this.config.getId())
        .withStart(startTime - cachingPeriodLookback - paddingBuffer)
        .withEnd(endTime + paddingBuffer);
    Multimap<AnomalySlice, MergedAnomalyResultDTO> warmUpResults = this.provider.fetchAnomalies(Collections.singleton(anomalySlice));
    LOG.info("Warmed up anomalies cache for detection {} with {} anomalies - start: {} end: {}", config.getId(),
        warmUpResults.values().size(), (startTime - cachingPeriodLookback), endTime);
  }

  private List<EvaluationDTO> calculateEvaluationMetrics(List<PredictionResult> predictionResults) {
    return predictionResults.stream().filter(predictionResult -> this.evaluationMetricUrns.contains(predictionResult.getMetricUrn()))
        .map(prediction -> EvaluationDTO.fromPredictionResult(prediction, this.startTime, this.endTime,
            this.config.getId()))
        .collect(Collectors.toList());
  }

  private void checkEarlyStop(long totalNestedMetrics, long successNestedMetrics, int i, List<Exception> exceptions)
      throws DetectionPipelineException {
    // if the first certain number of dimensions all failed, throw an exception
    // the threshold is set to the 10 percent of the number of dimensions and at least 10.
    long earlyStopThreshold = Math.max(EARLY_STOP_THRESHOLD, (int)(totalNestedMetrics * EARLY_STOP_THRESHOLD_PERCENTAGE));
    if (i == earlyStopThreshold && successNestedMetrics == 0) {
      throw new DetectionPipelineException(String.format(
          "Detection failed for first %d out of %d metric dimensions for monitoring window %d to %d, stop processing.",
          i, totalNestedMetrics, this.getStartTime(), this.getEndTime()), Iterables.getLast(exceptions));
    }
  }

  /**
   * Check the exception for all nested metric and determine whether to fail the detection.
   *
   * This method will throw an exception to outer wrappers if the all nested metrics failed.
   * UNLESS the exception for all nested metrics are failed by
   * {@link DetectorDataInsufficientException} and multiple detector are configured to run,
   * and some of them run successfully. In such case, this method will
   * still allow the detection to finish and return the generated anomalies. Because the data insufficient exception
   * thrown by one detector should not block other detectors's result.
   *
   * @param totalNestedMetrics the total number of nested metrics
   * @param successNestedMetrics the successfully run nested metrics
   * @param exceptions the list of all exceptions
   * @param predictions the prediction results generated, which can tell us whether there are detectors run successfully.
   * @throws DetectionPipelineException the exception to throw
   */
  private void checkNestedMetricsStatus(long totalNestedMetrics, long successNestedMetrics, List<Exception> exceptions,
      List<PredictionResult> predictions) throws DetectionPipelineException {
    // if all nested metrics failed, throw an exception
    if (successNestedMetrics == 0 && totalNestedMetrics > 0) {
      // if all exceptions are caused by DetectorDataInsufficientException and
      // there are other detectors run successfully, keep the detection running
      if (exceptions.stream().allMatch(e -> e.getCause() instanceof DetectorDataInsufficientException)
          && predictions.size() != 0) {
        LOG.warn(
            "The detection pipeline {} for monitoring window {} to {} is having detectors throwing the DataInsufficientException, "
                + "but the result for other successful detectors are preserved", this.config.getId(), this.getStartTime(), this.getEndTime());
      } else {
        throw new DetectionPipelineException(String.format(
            "Detection failed for all nested dimensions for detection config id %d for monitoring window %d to %d.",
            this.config.getId(), this.getStartTime(), this.getEndTime()), Iterables.getLast(exceptions));
      }
    }
  }

  private boolean checkMinLiveZone(MetricEntity me) {
    MetricSlice slice = MetricSlice.from(me.getId(), this.start.getMillis(), this.end.getMillis(), me.getFilters());
    DataFrame df = this.provider.fetchTimeseries(Collections.singleton(slice)).get(slice);
    long totalBuckets = df.size();
    df = df.filter(df.getDoubles(COL_VALUE).gt(this.minLiveZone)).dropNull();
    double liveBucketPercentage = (double) df.size() / (double) totalBuckets;
    if (liveBucketPercentage >= this.liveBucketPercentageThreshold) {
      return df.getDoubles(COL_VALUE).mean().getDouble(0)>= this.minValue;
    }
    return false;
  }
}
