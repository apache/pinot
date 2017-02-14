package com.linkedin.thirdeye.anomaly.detectionex;

import com.linkedin.pinot.pql.parsers.utils.Pair;
import com.linkedin.thirdeye.anomaly.task.TaskContext;
import com.linkedin.thirdeye.anomaly.task.TaskInfo;
import com.linkedin.thirdeye.anomaly.task.TaskResult;
import com.linkedin.thirdeye.anomaly.task.TaskRunner;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionExDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionEx;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExContext;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExDataSource;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExFactory;
import com.linkedin.thirdeye.detector.functionex.AnomalyFunctionExResult;
import com.linkedin.thirdeye.detector.functionex.dataframe.DataFrame;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// NOTES:
// - no scaling
// - no backfill detection

public class DetectionExTaskRunner implements TaskRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DetectionExTaskRunner.class);

  private static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  static class MockDataSource implements AnomalyFunctionExDataSource<String, DataFrame> {
    @Override
    public DataFrame query(String query, AnomalyFunctionExContext context) {
      DataFrame df = new DataFrame(5);
      df.addSeries("long", 3, 4, 5, 6, 7);
      df.addSeries("double", 1.2, 3.5, 2.8, 6.4, 4.9);
      df.addSeries("stable", 1, 1, 1, 1, 1);
      return df;
    }
  }

  private AnomalyFunctionExDTO funcSpec;
  private long jobExecutionId;

  protected void setupTask(TaskInfo taskInfo) throws Exception {
    DetectionExTaskInfo task = (DetectionExTaskInfo) taskInfo;
    jobExecutionId = task.getJobExecutionId();
    funcSpec = task.getAnomalyFunctionSpec();
  }

  public List<TaskResult> execute(TaskInfo taskInfo, TaskContext taskContext) throws Exception {
    LOG.info("Begin executing task {}", taskInfo);

    setupTask(taskInfo);

    List<TaskResult> taskResult = new ArrayList<>();

    // factory
    AnomalyFunctionExFactory factory = new AnomalyFunctionExFactory();
    factory.addDataSource("mock", new MockDataSource());

    // instantiate function
    AnomalyFunctionEx func = factory.fromSpec(funcSpec);

    // apply
    AnomalyFunctionExResult result = func.apply();

    LOG.info("{} anomaly: {}", funcSpec.getName(), result.isAnomaly());
    LOG.info("{} message: {}", funcSpec.getName(), result.getMessage());

    // TODO transform output into raw anomalies?

    // TODO merge?

//    if(datasetConfig.isRequiresCompletenessCheck()) {
//      LOG.info("Dataset {} requires completeness check", dataset);
//      assertCompletenessCheck(dataset);
//    }
//
//    LOG.info(
//        "Running anomaly detection job with metricFunction: [{}], topic metric [{}], collection: [{}]",
//        anomalyFunctionSpec.getFunctionName(), anomalyFunctionSpec.getTopicMetric(),
//        anomalyFunctionSpec.getCollection());
//
//    collectionDimensions = datasetConfig.getDimensions();
//
//    // Get Time Series
//    List<Pair<Long, Long>> startEndTimeRanges = anomalyFunction.getDataRangeIntervals(windowStart.getMillis(), windowEnd.getMillis());
//    Map<DimensionKey, MetricTimeSeries> dimensionKeyMetricTimeSeriesMap = TimeSeriesUtil.getTimeSeriesForAnomalyDetection(anomalyFunctionSpec, startEndTimeRanges);
//
//    // construct data frame index
//    // TODO: fix the hack
//    Map.Entry<DimensionKey, MetricTimeSeries> eTemp = dimensionKeyMetricTimeSeriesMap.entrySet().iterator().next();
//    MetricTimeSeries tsTemp = eTemp.getValue();
//
//    long[] index = getTimestamps(tsTemp);
//
//    // construct data frame
//    DataFrame dfInput = new DataFrame(index);
//
//    for(Map.Entry<DimensionKey, MetricTimeSeries> e : dimensionKeyMetricTimeSeriesMap.entrySet()) {
//      DimensionKey key = e.getKey();
//      MetricTimeSeries ts = e.getValue();
//
//      LOG.info("processing key={} ts={}", key, ts);
//
//      for(String metric : ts.getSchema().getNames()) {
//
//        // construct name
//        String name = getSeriesName(key, metric);
//
//        // construct and add series
//        DataFrame.Series s = getSeries(ts, metric);
//        dfInput.addSeries(name, s);
//      }
//    }
//
//    DataFrame dfOutput =
//
//    List<RawAnomalyResultDTO> resultsOfAnEntry = anomalyFunction
//        .analyze(exploredDimensions, metricTimeSeries, windowStart, windowEnd, historyMergedAnomalies);
//
//    // Handle results
//    handleResults(resultsOfAnEntry);

    return taskResult;
  }

//  private DataFrame.Series getSeries(MetricTimeSeries ts, String metric) {
//    MetricType mtype = ts.getSchema().getMetricType(metric);
//    switch(mtype) {
//      case FLOAT:
//      case DOUBLE:
//        return getDoubleSeries(ts, metric);
//
//      case SHORT:
//      case INT:
//      case LONG:
//        return getLongSeries(ts, metric);
//
//      default:
//        throw new IllegalArgumentException(String.format("Unknown data type '%s'", mtype));
//    }
//  }
//
//  private DataFrame.DoubleSeries getDoubleSeries(MetricTimeSeries ts, String metric) {
//    long[] index = getTimestamps(ts);
//    double[] values = new double[index.length];
//    int i=0;
//    for(long t : index) {
//      values[i] = ts.get(t, metric).doubleValue();
//      i++;
//    }
//    return new DataFrame.DoubleSeries(values);
//  }
//
//  private DataFrame.LongSeries getLongSeries(MetricTimeSeries ts, String metric) {
//    long[] index = getTimestamps(ts);
//    long[] values = new long[index.length];
//    int i=0;
//    for(long t : index) {
//      values[i] = ts.get(t, metric).longValue();
//      i++;
//    }
//    return new DataFrame.LongSeries(values);
//  }

  private long[] getTimestamps(MetricTimeSeries ts) {
    long[] index = new long[ts.getTimeWindowSet().size()];
    List<Long> timestamps = new ArrayList<>(ts.getTimeWindowSet());
    Collections.sort(timestamps);
    // Note: no built-in toArray for primitive types
    for(int i=0; i<=timestamps.size(); i++) {
      index[i] = timestamps.get(i);
    }
    return index;
  }

  private String getSeriesName(DimensionKey key, String metric) {
    int ndim = key.getDimensionValues().length;
    if(ndim == 0) {
      return metric;
    } else if(ndim == 1) {
      return String.format("%s/%s", metric, key.getDimensionValues()[0]);
    } else {
      throw new IllegalStateException("dimensionality must be <= 1");
    }
  }

//  protected void assertCompletenessCheck(String dataset) {
//    List<DataCompletenessConfigDTO> completed =
//        DAO_REGISTRY.getDataCompletenessConfigDAO().findAllByDatasetAndInTimeRangeAndStatus(
//            dataset, windowStart.getMillis(), windowEnd.getMillis(), true);
//
//    LOG.debug("Found {} dataCompleteness records for dataset {} from {} to {}",
//        completed.size(), dataset, windowStart.getMillis(), windowEnd.getMillis());
//
//    if (completed.size() <= 0) {
//      LOG.warn("Dataset {} is incomplete. Skipping anomaly detection.", dataset);
//      throw new IllegalStateException(String.format("Dataset %s incomplete", dataset));
//    }
//  }

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
        if (existingAnomaly.getStartTime().compareTo(rawAnomaly.getStartTime()) <= 0
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
//    for (RawAnomalyResultDTO result : results) {
//      try {
//        // Properties that always come from the function spec
//        AnomalyFunctionDTO spec = anomalyFunction.getSpec();
//        // make sure score and weight are valid numbers
//        result.setScore(normalize(result.getScore()));
//        result.setWeight(normalize(result.getWeight()));
//        result.setFunction(spec);
//        DAO_REGISTRY.getRawAnomalyResultDAO().save(result);
//      } catch (Exception e) {
//        LOG.error("Exception in saving anomaly result : " + result.toString(), e);
//      }
//    }
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
