package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ResponseParserUtils;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunction;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang.NullArgumentException;
import org.joda.time.DateTime;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyDetectionInputContextBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyDetectionInputContextBuilder.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  private AnomalyDetectionInputContext anomalyDetectionInputContext;
  private AnomalyFunctionDTO anomalyFunctionSpec;
  private BaseAnomalyFunction anomalyFunction;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private List<String> collectionDimensions;
  private String dataset;

  public AnomalyDetectionInputContextBuilder(AnomalyFunctionFactory anomalyFunctionFactory) {
    this.anomalyFunctionFactory = anomalyFunctionFactory;
  }

  public void init(AnomalyFunctionDTO anomalyFunctionSpec) throws Exception {
    init(anomalyFunctionSpec, new AnomalyDetectionInputContext());
  }
  public void init(AnomalyFunctionDTO anomalyFunctionSpec, AnomalyDetectionInputContext anomalyDetectionInputContext)
      throws Exception {
    this.anomalyFunctionSpec = anomalyFunctionSpec;
    this.anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);
    this.anomalyDetectionInputContext = anomalyDetectionInputContext;
    this.dataset = this.anomalyFunctionSpec.getCollection();
    DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);
    if (datasetConfig == null) {
      LOG.error("Dataset [" + dataset + "] is not found");
      throw new NullArgumentException(
          "Dataset [" + dataset + "] is not found with function : " + anomalyFunctionSpec
              .toString());
    }
    this.collectionDimensions = datasetConfig.getDimensions();
  }

  public AnomalyDetectionInputContext build() {
    return this.anomalyDetectionInputContext;
  }

  /**
   * Fetch TimeSeriese data from Pinot in the startEndTimeRanges
   * @param startEndTimeRanges
   * @return a AnomalyDetectionInputContextBuilder
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesData(List<Pair<Long, Long>> startEndTimeRanges, boolean endTimeInclusive)
      throws JobExecutionException, ExecutionException {
    Map<DimensionKey, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap = TimeSeriesUtil
        .getTimeSeriesForAnomalyDetection(anomalyFunctionSpec, startEndTimeRanges, endTimeInclusive);

    Map<DimensionMap, MetricTimeSeries> dimensionMapMetricTimeSeriesMap = new HashMap<>();
    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : dimensionKeyMetricTimeSeriesMap.entrySet()) {
      DimensionKey dimensionKey = entry.getKey();

      // If the current time series belongs to OTHER dimension, which consists of time series whose
      // sum of all its values belows 1% of sum of all time series values, then its anomaly is
      // meaningless and hence we don't want to detection anomalies on it.
      String[] dimensionValues = dimensionKey.getDimensionValues();
      boolean isOTHERDimension = false;
      for (String dimensionValue : dimensionValues) {
        if (dimensionValue.equalsIgnoreCase(ResponseParserUtils.OTHER) || dimensionValue.equalsIgnoreCase(ResponseParserUtils.UNKNOWN)) {
          isOTHERDimension = true;
          break;
        }
      }
      if (isOTHERDimension) {
        continue;
      }

      DimensionMap dimensionMap = DimensionMap.fromDimensionKey(dimensionKey, collectionDimensions);
      dimensionMapMetricTimeSeriesMap.put(dimensionMap, entry.getValue());

      if (entry.getValue().getTimeWindowSet().size() < 1) {
        LOG.warn("Insufficient data for {} to run anomaly detection function", dimensionMap);
        continue;
      }
    }
    this.anomalyDetectionInputContext.setDimensionKeyMetricTimeSeriesMap(dimensionMapMetricTimeSeriesMap);

    return this;
  }
  /**
   * Fetch TimeSeriese data from Pinot with given monitoring window start and end time
   * @param windowStart
   * @param windowEnd
   * @param endTimeInclusive
   * @return
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesData(DateTime windowStart, DateTime windowEnd, boolean endTimeInclusive)
      throws JobExecutionException, ExecutionException {
    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis());
    return fetchTimeSeriesData(startEndTimeRanges, endTimeInclusive);
  }
  /**
   * Fetch TimeSeriese data from Pinot with given monitoring window start and end time
   * endTimeInclusive is set to false in default
   * @param windowStart
   * @param windowEnd
   * @return
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesData(DateTime windowStart, DateTime windowEnd)
      throws JobExecutionException, ExecutionException {
    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis());
    return fetchTimeSeriesData(startEndTimeRanges, false);
  }
  /**
   * Fetch TimeSeriese data from Pinot with given time range
   * endTimeInclusive is set to false in default
   *
   * @param startEndTimeRanges
   * @return
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesData(List<Pair<Long, Long>> startEndTimeRanges)
      throws JobExecutionException, ExecutionException {
    return fetchTimeSeriesData(startEndTimeRanges, false);
  }

  /**
   * Fetch the root metric without dimension and filter in a given monitoring start and end time
   * @param windowStart
   * @param windowEnd
   * @return
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesMetricSum(DateTime windowStart, DateTime windowEnd)
      throws JobExecutionException, ExecutionException {
    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis());
    this.fetchTimeSeriesMetricSum(startEndTimeRanges);

    return this;
  }

  /**
   * Fetch the root metric without dimension and filter in a given time range
   * @param startEndTimeRanges
   * @return
   * @throws JobExecutionException
   * @throws ExecutionException
   */
  public AnomalyDetectionInputContextBuilder fetchTimeSeriesMetricSum (List<Pair<Long, Long>> startEndTimeRanges)
      throws JobExecutionException, ExecutionException {
    MetricTimeSeries metricSumTimeSeries = TimeSeriesUtil
        .getMetricSum(anomalyFunctionSpec, startEndTimeRanges);
    this.anomalyDetectionInputContext.setMetricSumTimeSeries(metricSumTimeSeries);

    return this;
  }
  /**
   * Fetech existing RawAnomalyResults
   * @param windowStart
   * @param windowEnd
   * @return
   */
  public AnomalyDetectionInputContextBuilder fetchExixtingRawAnomalies(DateTime windowStart, DateTime windowEnd) {
    // We always find existing raw anomalies to prevent duplicate raw anomalies are generated
    List<RawAnomalyResultDTO> existingRawAnomalies = getExistingRawAnomalies(anomalyFunctionSpec.getId(), windowStart.getMillis(), windowEnd.getMillis());
    ArrayListMultimap<DimensionMap, RawAnomalyResultDTO> dimensionNamesToKnownRawAnomalies = ArrayListMultimap.create();
    for (RawAnomalyResultDTO existingRawAnomaly : existingRawAnomalies) {
      dimensionNamesToKnownRawAnomalies.put(existingRawAnomaly.getDimensions(), existingRawAnomaly);
    }
    this.anomalyDetectionInputContext.setExistingRawAnomalies(dimensionNamesToKnownRawAnomalies);

    return this;
  }

  /**
   * Fetch existing MergedAnomalyResults
   * @param windowStart
   * @param windowEnd
   * @return
   */
  public AnomalyDetectionInputContextBuilder fetchExixtingMergedAnomalies(DateTime windowStart, DateTime windowEnd) {
// Get existing anomalies for this time range and this function id for all combinations of dimensions
    List<MergedAnomalyResultDTO> knownMergedAnomalies;
    if (anomalyFunction.useHistoryAnomaly()) {
      // if this anomaly function uses history data, then we get all time ranges
      knownMergedAnomalies = getKnownMergedAnomalies(anomalyFunctionSpec.getId(),
          anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis()));
    } else {
      // otherwise, we only get the merge anomaly for current window in order to remove duplicate raw anomalies
      List<Pair<Long, Long>> currentTimeRange = new ArrayList<>();
      currentTimeRange.add(new Pair<>(windowStart.getMillis(), windowEnd.getMillis()));
      knownMergedAnomalies = getKnownMergedAnomalies(anomalyFunctionSpec.getId(), currentTimeRange);
    }
    // Sort the known merged and raw anomalies by their dimension names
    ArrayListMultimap<DimensionMap, MergedAnomalyResultDTO> dimensionMapToKnownMergedAnomalies = ArrayListMultimap.create();
    for (MergedAnomalyResultDTO knownMergedAnomaly : knownMergedAnomalies) {
      dimensionMapToKnownMergedAnomalies.put(knownMergedAnomaly.getDimensions(), knownMergedAnomaly);
    }
    this.anomalyDetectionInputContext.setKnownMergedAnomalies(dimensionMapToKnownMergedAnomalies);

    return this;
  }

  /**
   * Fetch Scaling Factors
   * @param windowStart
   * @param windowEnd
   * @return
   */
  public AnomalyDetectionInputContextBuilder fetchSaclingFactors(DateTime windowStart, DateTime windowEnd) {
    List<ScalingFactor> scalingFactors = OverrideConfigHelper
        .getTimeSeriesScalingFactors(DAO_REGISTRY.getOverrideConfigDAO(), anomalyFunctionSpec.getCollection(),
            anomalyFunctionSpec.getMetric(), anomalyFunctionSpec.getId(),
            anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis()));
    this.anomalyDetectionInputContext.setScalingFactors(scalingFactors);
    return this;
  }

  /**
   * Returns existing raw anomalies in the given monitoring window
   *
   * @param functionId the id of the anomaly function
   * @param monitoringWindowStart inclusive
   * @param monitoringWindowEnd inclusive but it doesn't matter
   *
   * @return known raw anomalies in the given window
   */
  private List<RawAnomalyResultDTO> getExistingRawAnomalies(long functionId, long monitoringWindowStart,
      long monitoringWindowEnd) {
    List<RawAnomalyResultDTO> results = new ArrayList<>();
    try {
      results.addAll(DAO_REGISTRY.getRawAnomalyResultDAO().findAllByTimeAndFunctionId(monitoringWindowStart, monitoringWindowEnd, functionId));
    } catch (Exception e) {
      LOG.error("Exception in getting existing anomalies", e);
    }
    return results;
  }

  /**
   * Returns all known merged anomalies of the function id that are needed for anomaly detection, i.e., the merged
   * anomalies that overlap with the monitoring window and baseline windows.
   *
   * @param functionId the id of the anomaly function
   * @param startEndTimeRanges the time ranges for retrieving the known merge anomalies

   * @return known merged anomalies of the function id that are needed for anomaly detection
   */
  private List<MergedAnomalyResultDTO> getKnownMergedAnomalies(long functionId, List<Pair<Long, Long>> startEndTimeRanges) {

    List<MergedAnomalyResultDTO> results = new ArrayList<>();
    for (Pair<Long, Long> startEndTimeRange : startEndTimeRanges) {
      try {
        results.addAll(
            DAO_REGISTRY.getMergedAnomalyResultDAO().findAllConflictByFunctionId(functionId, startEndTimeRange.getFirst(),
                startEndTimeRange.getSecond()));
      } catch (Exception e) {
        LOG.error("Exception in getting merged anomalies", e);
      }
    }

    return results;
  }
}
