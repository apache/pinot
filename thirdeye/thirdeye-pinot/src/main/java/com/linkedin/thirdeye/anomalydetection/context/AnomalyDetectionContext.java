package com.linkedin.thirdeye.anomalydetection.context;

import com.linkedin.thirdeye.anomalydetection.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The context for performing an anomaly detection on the sets of time series from the same
 * dimension and metric. The context also provides field, to which the anomaly function can appends
 * the intermediate results such as transformed time series, trained prediction model, etc.
 */
public class AnomalyDetectionContext {
  // The followings are inputs for anomaly detection
  private AnomalyDetectionFunction anomalyDetectionFunction;

  private TimeSeriesKey timeSeriesKey;
  private long bucketSizeInMS; // the bucket size, gap between timestamps, in millisecond

  private Map<String, TimeSeries> current = new HashMap<>();
  private Map<String, List<TimeSeries>> baselines = new HashMap<>();

  //TODO: Add DAO for accessing historical anomalies or scaling factor
  private List<MergedAnomalyResultDTO> historicalAnomalies = new ArrayList<>();

  // The followings are intermediate results and are appended during anomaly detection
  private Map<String, TimeSeries> transformedCurrent = new HashMap<>();
  private Map<String, List<TimeSeries>> transformedBaselines = new HashMap<>();

  private Map<String, PredictionModel> trainedPredictionModel = new HashMap<>();

  /**
   * Returns the key of the time series, which contains metric name and dimension map.
   */
  public TimeSeriesKey getTimeSeriesKey() {
    return timeSeriesKey;
  }

  /**
   * Set the key of the time series.
   */
  public void setTimeSeriesKey(TimeSeriesKey key) {
    this.timeSeriesKey = key;
  }

  /**
   * Returns the current (observed) time series, which is not transformed.
   */
  public TimeSeries getCurrent(String metricName) {
    return current.get(metricName);
  }

  /**
   * Set the current (observed) time series.
   */
  public void setCurrent(String metricName, TimeSeries current) {
    this.current.put(metricName, current);
  }

  /**
   * Returns the set of baseline time series for training the prediction model.
   */
  public List<TimeSeries> getBaselines(String metricName) {
    return baselines.get(metricName);
  }

  /**
   * Sets the set of baseline time series for training the prediction model.
   */
  public void setBaselines(String metricName, List<TimeSeries> baselines) {
    this.baselines.put(metricName, baselines);
  }

  /**
   * Returns the list of historicalAnomalies.
   */
  public List<MergedAnomalyResultDTO> getHistoricalAnomalies() {
    return this.historicalAnomalies;
  }

  /**
   * Sets the set of baseline time series for training the prediction model.
   */
  public void setHistoricalAnomalies(List<MergedAnomalyResultDTO> anomalies) {
    this.historicalAnomalies = anomalies;
  }

  /**
   * Returns the bucket size, the gap between timestamps, in milliseconds.
   */
  public long getBucketSizeInMS() {
    return bucketSizeInMS;
  }

  /**
   * Sets the bucket size, the gap between timestamps, in milliseconds.
   */
  public void setBucketSizeInMS(long bucketSizeInMS) {
    this.bucketSizeInMS = bucketSizeInMS;
  }

  /**
   * Returns the anomaly detection function, which provides all the models for performing a job
   * of anomaly detection.
   */
  public AnomalyDetectionFunction getAnomalyDetectionFunction() {
    return anomalyDetectionFunction;
  }

  /**
   * Sets the anomaly detection function, which provides all the models for performing a job
   * of anomaly detection.
   */
  public void setAnomalyDetectionFunction(AnomalyDetectionFunction anomalyDetectionFunction) {
    this.anomalyDetectionFunction = anomalyDetectionFunction;
  }

  /**
   * Returns the transformed current (observed) time series. If no transformation function is setup
   * in the context, then the original current time series is returned.
   */
  public TimeSeries getTransformedCurrent(String metricName) {
    return transformedCurrent.get(metricName);
  }

  /**
   * Sets the transformed current (observed) time series. This method is supposed to be used by
   * transformation functions.
   */
  public void setTransformedCurrent(String metricName, TimeSeries transformedCurrent) {
    this.transformedCurrent.put(metricName, transformedCurrent);
  }

  /**
   * Returns the set of transformed baseline time series. If no transformation function is setup
   * in the context, then the original set of baseline time series is returned.
   */
  public List<TimeSeries> getTransformedBaselines(String metricName) {
    return transformedBaselines.get(metricName);
  }

  /**
   * Sets the set of transformed baseline time series. This method is supposed to be used by
   * transformation functions.
   */
  public void setTransformedBaselines(String metricName, List<TimeSeries> transformedBaselines) {
    this.transformedBaselines.put(metricName, transformedBaselines);
  }

  /**
   * Returns the trained prediction model.
   */
  public PredictionModel getTrainedPredictionModel(String metricName) {
    return trainedPredictionModel.get(metricName);
  }

  /**
   * Sets the trained prediction model, which is supposed to be set by the anomaly function.
   */
  public void setTrainedPredictionModel(String metricName, PredictionModel trainedPredictionModel) {
    this.trainedPredictionModel.put(metricName, trainedPredictionModel);
  }
}
