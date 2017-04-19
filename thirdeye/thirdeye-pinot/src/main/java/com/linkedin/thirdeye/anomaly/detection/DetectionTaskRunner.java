package com.linkedin.thirdeye.anomaly.detection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.merge.TimeBasedAnomalyMerger;
import com.linkedin.thirdeye.anomaly.override.OverrideConfigHelper;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.anomaly.utils.AnomalyUtils;
import com.linkedin.thirdeye.anomalydetection.datafilter.DataFilter;
import com.linkedin.thirdeye.anomalydetection.datafilter.DataFilterFactory;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionMap;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.ResponseParserUtils;
import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.bao.RawAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.function.AnomalyFunctionFactory;
import com.linkedin.thirdeye.detector.function.BaseAnomalyFunction;
import com.linkedin.thirdeye.detector.metric.transfer.MetricTransfer;
import com.linkedin.thirdeye.detector.metric.transfer.ScalingFactor;
import com.linkedin.thirdeye.anomaly.detection.DetectionJobContext.DetectionJobType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.NullArgumentException;
import org.joda.time.DateTime;
import org.mozilla.javascript.tools.debugger.Dim;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.linkedin.thirdeye.anomaly.utils.ThirdeyeMetricsUtil.*;

public class DetectionTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionTaskRunner.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  public static final String BACKFILL_PREFIX = "adhoc_";

  private List<DateTime> windowStarts;
  private List<DateTime> windowEnds;
  private AnomalyFunctionDTO anomalyFunctionSpec;
  private long jobExecutionId;
  private DetectionJobType detectionJobType;

  private List<String> collectionDimensions;
  private AnomalyFunctionFactory anomalyFunctionFactory;
  private BaseAnomalyFunction anomalyFunction;

  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    detectionTaskCounter.inc();
    List<TaskResult> taskResult = new ArrayList<>();

    LOG.info("Setting up task {}", taskInfo);
    setupTask(taskInfo, taskContext);

    // Run for all pairs of window start and window end
    for (int i = 0; i < windowStarts.size(); i ++) {
      runTask(windowStarts.get(i), windowEnds.get(i));
    }

    return taskResult;
  }

  private void setupTask(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    DetectionTaskInfo detectionTaskInfo = (DetectionTaskInfo) taskInfo;
    windowStarts = detectionTaskInfo.getWindowStartTime();
    windowEnds = detectionTaskInfo.getWindowEndTime();
    anomalyFunctionSpec = detectionTaskInfo.getAnomalyFunctionSpec();
    jobExecutionId = detectionTaskInfo.getJobExecutionId();
    anomalyFunctionFactory = taskContext.getAnomalyFunctionFactory();
    anomalyFunction = anomalyFunctionFactory.fromSpec(anomalyFunctionSpec);
    detectionJobType = detectionTaskInfo.getDetectionJobType();

    String dataset = anomalyFunctionSpec.getCollection();
    DatasetConfigDTO datasetConfig = DAO_REGISTRY.getDatasetConfigDAO().findByDataset(dataset);

    if (datasetConfig == null) {
      LOG.error("Dataset [" + dataset + "] is not found");
      throw new NullArgumentException(
          "Dataset [" + dataset + "] is not found with function : " + anomalyFunctionSpec
              .toString());
    }
    collectionDimensions = datasetConfig.getDimensions();

    LOG.info(
        "Running anomaly detection job with metricFunction: [{}], topic metric [{}], collection: [{}]",
        anomalyFunctionSpec.getFunctionName(), anomalyFunctionSpec.getTopicMetric(),
        anomalyFunctionSpec.getCollection());
  }


  private void runTask(DateTime windowStart, DateTime windowEnd) throws JobExecutionException, ExecutionException {

    LOG.info("Running anomaly detection for time range {} to  {}", windowStart, windowEnd);

    // TODO: Change to DataFetchers/DataSources
    AnomalyDetectionInputContext adContext = fetchData(windowStart, windowEnd);

    ListMultimap<DimensionMap, RawAnomalyResultDTO> resultRawAnomalies = dimensionalShuffleAndUnifyAnalyze(windowStart, windowEnd, adContext);
    detectionTaskSuccessCounter.inc();

    boolean isBackfill = false;
    // If the current job is a backfill (adhoc) detection job, set notified flag to true so the merged anomalies do not
    // induce alerts and emails.
    if (detectionJobType != null && (detectionJobType.equals(DetectionJobType.BACKFILL) ||
        detectionJobType.equals(DetectionJobType.OFFLINE))) {
      LOG.info("BACKFILL is triggered for Detection Job {}. Notified flag is set to be true", jobExecutionId);
      isBackfill = true;
    }

    // Update merged anomalies
    TimeBasedAnomalyMerger timeBasedAnomalyMerger = new TimeBasedAnomalyMerger(anomalyFunctionFactory);
    ListMultimap<DimensionMap, MergedAnomalyResultDTO> resultMergedAnomalies =
      timeBasedAnomalyMerger.mergeAnomalies(anomalyFunctionSpec, resultRawAnomalies, isBackfill);
    MetricTimeSeries metricTraffic = adContext.getMetricTraffic();
    // Calculate Traffic Contribution
    for (DimensionMap dimension : resultMergedAnomalies.keys()) {
      for (MergedAnomalyResultDTO mergedAnomaly : resultMergedAnomalies.get(dimension)) {
        mergedAnomaly.setTrafficContribution(calculateTrafficImpact(metricTraffic, mergedAnomaly));
      }
    }
    detectionTaskSuccessCounter.inc();

    // TODO: Change to DataSink
    AnomalyDetectionOutputContext adOutputContext = new AnomalyDetectionOutputContext();
    adOutputContext.setRawAnomalies(resultRawAnomalies);
    adOutputContext.setMergedAnomalies(resultMergedAnomalies);
    storeData(adOutputContext);
  }

  private double calculateTrafficImpact (MetricTimeSeries metricTraffic, MergedAnomalyResultDTO mergedAnomaly) {
    Set<Long> timestamps = metricTraffic.getTimeWindowSet();
    double avgTraffic = 0.0;
    int count = 0;
    for (long timestamp : timestamps) {
      if (timestamp >= mergedAnomaly.getStartTime() && timestamp <= mergedAnomaly.getEndTime()) {
        avgTraffic += metricTraffic.get(timestamp, mergedAnomaly.getMetric()).doubleValue();
        count++;
      }
    }
    avgTraffic /= count;
    return (mergedAnomaly.getAvgCurrentVal() - mergedAnomaly.getAvgBaselineVal()) / avgTraffic;
  }

  private AnomalyDetectionInputContext fetchData(DateTime windowStart, DateTime windowEnd)
      throws JobExecutionException, ExecutionException {
    AnomalyDetectionInputContext adContext = new AnomalyDetectionInputContext();

    // Get Time Series
    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis());
    Map<DimensionKey, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap = TimeSeriesUtil.getTimeSeriesForAnomalyDetection(anomalyFunctionSpec, startEndTimeRanges);
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
    adContext.setDimensionKeyMetricTimeSeriesMap(dimensionMapMetricTimeSeriesMap);

    // Get traffic contribution ratio of each dimension
    List<Pair<Long, Long>> monitoringTimeRange = new ArrayList<>();
    monitoringTimeRange.add(new Pair(windowStart, windowEnd));
    // Clone anomalyFunctionSpec and remove filter and dimension info
    AnomalyFunctionDTO cloneAnomalyFunctionSpec = new AnomalyFunctionDTO();
    cloneAnomalyFunctionSpec.setCollection(anomalyFunctionSpec.getCollection());
    cloneAnomalyFunctionSpec.setMetrics(anomalyFunctionSpec.getMetrics());
    cloneAnomalyFunctionSpec.setMetricFunction(anomalyFunctionSpec.getMetricFunction());
    cloneAnomalyFunctionSpec.setBucketSize(anomalyFunctionSpec.getBucketSize());
    cloneAnomalyFunctionSpec.setBucketUnit(anomalyFunctionSpec.getBucketUnit());
    dimensionKeyMetricTimeSeriesMap = TimeSeriesUtil.getTimeSeriesForAnomalyDetection(cloneAnomalyFunctionSpec, monitoringTimeRange);
    // Calculate traffic contribution ratio
    if (dimensionKeyMetricTimeSeriesMap.size() > 1) {
      LOG.warn("More than 1 dimensions when fetching traffic data for {}; take the 1st dimension", anomalyFunctionSpec);
    }
    DimensionKey dimensionKey = dimensionKeyMetricTimeSeriesMap.keySet().iterator().next();
    adContext.setMetricTraffic(dimensionKeyMetricTimeSeriesMap.get(dimensionKey));

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
    adContext.setKnownMergedAnomalies(dimensionMapToKnownMergedAnomalies);

    // We always find existing raw anomalies to prevent duplicate raw anomalies are generated
    List<RawAnomalyResultDTO> existingRawAnomalies = getExistingRawAnomalies(anomalyFunctionSpec.getId(), windowStart.getMillis(), windowEnd.getMillis());
    ArrayListMultimap<DimensionMap, RawAnomalyResultDTO> dimensionNamesToKnownRawAnomalies = ArrayListMultimap.create();
    for (RawAnomalyResultDTO existingRawAnomaly : existingRawAnomalies) {
      dimensionNamesToKnownRawAnomalies.put(existingRawAnomaly.getDimensions(), existingRawAnomaly);
    }
    adContext.setExistingRawAnomalies(dimensionNamesToKnownRawAnomalies);

    List<ScalingFactor> scalingFactors = OverrideConfigHelper
        .getTimeSeriesScalingFactors(DAO_REGISTRY.getOverrideConfigDAO(), anomalyFunctionSpec.getCollection(),
            anomalyFunctionSpec.getMetric(), anomalyFunctionSpec.getId(),
            anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis()));
    adContext.setScalingFactors(scalingFactors);

    return adContext;
  }

  private void storeData(AnomalyDetectionOutputContext anomalyDetectionOutputContext) {
    RawAnomalyResultManager rawAnomalyDAO = DAO_REGISTRY.getRawAnomalyResultDAO();
    MergedAnomalyResultManager mergedAmomalyDAO = DAO_REGISTRY.getMergedAnomalyResultDAO();

    for (RawAnomalyResultDTO rawAnomalyResultDTO : anomalyDetectionOutputContext.getRawAnomalies().values()) {
      rawAnomalyDAO.save(rawAnomalyResultDTO);
    }

    for (MergedAnomalyResultDTO mergedAnomalyResultDTO : anomalyDetectionOutputContext.getMergedAnomalies().values()) {
      mergedAmomalyDAO.update(mergedAnomalyResultDTO);
    }
  }

  private ListMultimap<DimensionMap, RawAnomalyResultDTO> dimensionalShuffleAndUnifyAnalyze(DateTime windowStart, DateTime windowEnd,
      AnomalyDetectionInputContext anomalyDetectionInputContext) {
    int anomalyCounter = 0;
    ListMultimap<DimensionMap, RawAnomalyResultDTO> resultRawAnomalies = ArrayListMultimap.create();

    DataFilter dataFilter = DataFilterFactory.fromSpec(anomalyFunctionSpec.getDataFilter());
    for (DimensionMap dimensionMap : anomalyDetectionInputContext.getDimensionKeyMetricTimeSeriesMap().keySet()) {
      // Skip anomaly detection if the current time series does not pass data filter, which may check if the traffic
      // or total count of the data has enough volume for produce sufficient confidence anomaly results
      MetricTimeSeries metricTimeSeries =
          anomalyDetectionInputContext.getDimensionKeyMetricTimeSeriesMap().get(dimensionMap);
      if (!dataFilter.isQualified(metricTimeSeries, dimensionMap, windowStart.getMillis(), windowEnd.getMillis())) {
        continue;
      }

      List<RawAnomalyResultDTO> resultsOfAnEntry = runAnalyze(windowStart, windowEnd, anomalyDetectionInputContext, dimensionMap);

      // Set raw anomalies' properties
      handleResults(resultsOfAnEntry);

      LOG.info("Dimension {} has {} anomalies in window {} to {}", dimensionMap, resultsOfAnEntry.size(),
          windowStart, windowEnd);
      anomalyCounter += resultsOfAnEntry.size();
      resultRawAnomalies.putAll(dimensionMap, resultsOfAnEntry);
    }

    LOG.info("{} anomalies found in total", anomalyCounter);
    return resultRawAnomalies;
  }

  private List<RawAnomalyResultDTO> runAnalyze(DateTime windowStart, DateTime windowEnd,
      AnomalyDetectionInputContext anomalyDetectionInputContext, DimensionMap dimensionMap) {

    List<RawAnomalyResultDTO> resultsOfAnEntry = Collections.emptyList();

    String metricName = anomalyFunction.getSpec().getTopicMetric();
    MetricTimeSeries metricTimeSeries = anomalyDetectionInputContext.getDimensionKeyMetricTimeSeriesMap().get(dimensionMap);

    /*
    Check if current task is running offline analysis
     */
    boolean isOffline = false;
    if (detectionJobType != null && detectionJobType.equals(DetectionJobType.OFFLINE)) {
      LOG.info("Detection Job {} is running under OFFLINE mode", jobExecutionId);
      isOffline = true;
    }

    // Get current entry's knownMergedAnomalies, which should have the same explored dimensions
    List<MergedAnomalyResultDTO> knownMergedAnomaliesOfAnEntry =
        anomalyDetectionInputContext.getKnownMergedAnomalies().get(dimensionMap);
    List<MergedAnomalyResultDTO> historyMergedAnomalies;
    if (anomalyFunction.useHistoryAnomaly()) {
      historyMergedAnomalies = retainHistoryMergedAnomalies(windowStart.getMillis(), knownMergedAnomaliesOfAnEntry);
    } else {
      historyMergedAnomalies = Collections.emptyList();
    }

    LOG.info("Analyzing anomaly function with explored dimensions: {}, windowStart: {}, windowEnd: {}",
        dimensionMap, windowStart, windowEnd);

    AnomalyUtils.logAnomaliesOverlapWithWindow(windowStart, windowEnd, historyMergedAnomalies);

    try {
      // Run algorithm
      // Scaling time series according to the scaling factor
      List<ScalingFactor> scalingFactors = anomalyDetectionInputContext.getScalingFactors();
      if (CollectionUtils.isNotEmpty(scalingFactors)) {
        Properties properties = anomalyFunction.getProperties();
        MetricTransfer.rescaleMetric(metricTimeSeries, windowStart.getMillis(), scalingFactors,
            metricName, properties);
      }

      if(isOffline) {
        resultsOfAnEntry = anomalyFunction
            .offlineAnalyze(dimensionMap, metricTimeSeries, windowStart, windowEnd, historyMergedAnomalies);
      } else {
        resultsOfAnEntry =
            anomalyFunction.analyze(dimensionMap, metricTimeSeries, windowStart, windowEnd, historyMergedAnomalies);
      }
    } catch (Exception e) {
      LOG.error("Could not compute for {}", dimensionMap, e);
    }

    // Remove detected anomalies that have existed in database
    if (CollectionUtils.isNotEmpty(resultsOfAnEntry)) {
      List<RawAnomalyResultDTO> existingRawAnomaliesOfAnEntry =
          anomalyDetectionInputContext.getExistingRawAnomalies().get(dimensionMap);
      resultsOfAnEntry = removeFromExistingRawAnomalies(resultsOfAnEntry, existingRawAnomaliesOfAnEntry);
    }
    if (CollectionUtils.isNotEmpty(resultsOfAnEntry)) {
      List<MergedAnomalyResultDTO> existingMergedAnomalies =
          retainExistingMergedAnomalies(windowStart.getMillis(), windowEnd.getMillis(), knownMergedAnomaliesOfAnEntry);
      resultsOfAnEntry = removeFromExistingMergedAnomalies(resultsOfAnEntry, existingMergedAnomalies);
    }

    return resultsOfAnEntry;
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
  public List<MergedAnomalyResultDTO> getKnownMergedAnomalies(long functionId, List<Pair<Long, Long>> startEndTimeRanges) {

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

  /**
   * Returns history anomalies of the monitoring window from the given known anomalies.
   *
   * Definition of history anomaly: An anomaly that starts before the monitoring window starts.
   *
   * @param monitoringWindowStart the start of the monitoring window
   * @param knownAnomalies the list of known anomalies
   *
   * @return all history anomalies of the monitoring window
   */
  private List<MergedAnomalyResultDTO> retainHistoryMergedAnomalies(long monitoringWindowStart,
      List<MergedAnomalyResultDTO> knownAnomalies) {
    List<MergedAnomalyResultDTO> historyAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO knownAnomaly : knownAnomalies) {
      if (knownAnomaly.getStartTime() < monitoringWindowStart) {
        historyAnomalies.add(knownAnomaly);
      }
    }
    return historyAnomalies;
  }

  /**
   * Returns anomalies that overlap with the monitoring window from the given known anomalies
   *
   * Definition of existing anomaly: An anomaly that happens in the monitoring window
   *
   * @param monitoringWindowStart the start of the monitoring window
   * @param monitoringWindowEnd the end of the monitoring window
   * @param knownAnomalies the list of known anomalies
   *
   * @return anomalies that happen in the monitoring window from the given known anomalies
   */
  private List<MergedAnomalyResultDTO> retainExistingMergedAnomalies(long monitoringWindowStart, long monitoringWindowEnd,
      List<MergedAnomalyResultDTO> knownAnomalies) {
    List<MergedAnomalyResultDTO> existingAnomalies = new ArrayList<>();
    for (MergedAnomalyResultDTO knownAnomaly : knownAnomalies) {
      if (knownAnomaly.getStartTime() <= monitoringWindowEnd && knownAnomaly.getEndTime() >= monitoringWindowStart) {
        existingAnomalies.add(knownAnomaly);
      }
    }
    return existingAnomalies;
  }

  /**
   * Given a list of raw anomalies, this method returns a list of raw anomalies that are not contained in any existing
   * merged anomalies.
   *
   * @param rawAnomalies
   * @param existingAnomalies
   * @return
   */
  private List<RawAnomalyResultDTO> removeFromExistingMergedAnomalies(List<RawAnomalyResultDTO> rawAnomalies,
      List<MergedAnomalyResultDTO> existingAnomalies) {
    if (CollectionUtils.isEmpty(rawAnomalies) || CollectionUtils.isEmpty(existingAnomalies)) {
      return rawAnomalies;
    }
    List<RawAnomalyResultDTO> newRawAnomalies = new ArrayList<>();

    for (RawAnomalyResultDTO rawAnomaly : rawAnomalies) {
      boolean isContained = false;
      for (MergedAnomalyResultDTO existingAnomaly : existingAnomalies) {
        if (Long.compare(existingAnomaly.getStartTime(), rawAnomaly.getStartTime()) <= 0
            && rawAnomaly.getEndTime().compareTo(existingAnomaly.getEndTime()) <= 0) {
          isContained = true;
          break;
        }
      }
      if (!isContained) {
        newRawAnomalies.add(rawAnomaly);
      }
    }

    return newRawAnomalies;
  }

  /**
   * Given a list of raw anomalies, this method returns a list of raw anomalies that are not contained in any existing
   * raw anomalies.
   *
   * @param rawAnomalies
   * @param existingRawAnomalies
   * @return
   */
  private List<RawAnomalyResultDTO> removeFromExistingRawAnomalies(List<RawAnomalyResultDTO> rawAnomalies,
      List<RawAnomalyResultDTO> existingRawAnomalies) {
    List<RawAnomalyResultDTO> newRawAnomalies = new ArrayList<>();

    for (RawAnomalyResultDTO rawAnomaly : rawAnomalies) {
      boolean matched = false;
      for (RawAnomalyResultDTO existingAnomaly : existingRawAnomalies) {
        if (existingAnomaly.getStartTime().compareTo(rawAnomaly.getStartTime()) <= 0
            && rawAnomaly.getEndTime().compareTo(existingAnomaly.getEndTime()) <= 0) {
          matched = true;
          break;
        }
      }
      if (!matched) {
        newRawAnomalies.add(rawAnomaly);
      }
    }

    return newRawAnomalies;
  }

  private void handleResults(List<RawAnomalyResultDTO> results) {
    for (RawAnomalyResultDTO result : results) {
      try {
        // Properties that always come from the function spec
        AnomalyFunctionDTO spec = anomalyFunction.getSpec();
        // make sure score and weight are valid numbers
        result.setScore(normalize(result.getScore()));
        result.setWeight(normalize(result.getWeight()));
        result.setFunction(spec);
      } catch (Exception e) {
        LOG.error("Exception in saving anomaly result : " + result.toString(), e);
      }
    }
  }

  /**
   * Handle any infinite or NaN values by replacing them with +/- max value or 0
   */
  private double normalize(double value) {
    if (Double.isInfinite(value)) {
      return (value > 0.0 ? 1 : -1) * Double.MAX_VALUE;
    } else if (Double.isNaN(value)) {
      return 0.0; // default?
    } else {
      return value;
    }
  }
}
