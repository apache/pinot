package com.linkedin.thirdeye.anomalydetection.context;

import com.linkedin.thirdeye.anomalydetection.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.List;

/**
 * The context for performing an anomaly detection on the sets of time series from the same
 * dimension and metric. The context also provides field, to which the anomaly function can appends
 * the intermediate results such as transformed time series, trained prediction model, raw
 * anomalies, etc.
 */
public class AnomalyDetectionContext {
  // The followings are inputs for anomaly detection
  private AnomalyDetectionFunction anomalyDetectionFunction;

  private TimeSeriesKey timeSeriesKey;

  private TimeSeries current;
  private List<TimeSeries> baselines;

  //TODO: Add DAO for accessing historical anomalies or scaling factor

  // The followings are appended during anomaly detection
  private TimeSeries transformedCurrent;
  private List<TimeSeries> transformedBaselines;

  private PredictionModel trainedPredictionModel;

  private List<RawAnomalyResultDTO> rawAnomalies;

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
  public TimeSeries getCurrent() {
    return current;
  }

  /**
   * Set the current (observed) time series.
   * @param current
   */
  public void setCurrent(TimeSeries current) {
    this.current = current;
  }

  /**
   * Returns the set of baseline time series for training the prediction model.
   */
  public List<TimeSeries> getBaselines() {
    return baselines;
  }

  /**
   * Sets the set of baseline time series for training the prediction model.
   */
  public void setBaselines(List<TimeSeries> baselines) {
    this.baselines = baselines;
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
  public TimeSeries getTransformedCurrent() {
    return transformedCurrent;
  }

  /**
   * Sets the transformed current (observed) time series. This method is supposed to be used by
   * transformation functions.
   */
  public void setTransformedCurrent(TimeSeries transformedCurrent) {
    this.transformedCurrent = transformedCurrent;
  }

  /**
   * Returns the set of transformed baseline time series. If no transformation function is setup
   * in the context, then the original set of baseline time series is returned.
   */
  public List<TimeSeries> getTransformedBaselines() {
    return transformedBaselines;
  }

  /**
   * Sets the set of transformed baseline time series. This method is supposed to be used by
   * transformation functions.
   */
  public void setTransformedBaselines(List<TimeSeries> transformedBaselines) {
    this.transformedBaselines = transformedBaselines;
  }

  /**
   * Returns the trained prediction model.
   */
  public PredictionModel getTrainedPredictionModel() {
    return trainedPredictionModel;
  }

  /**
   * Sets the trained prediction model, which is supposed to be set by the anomaly function.
   */
  public void setTrainedPredictionModel(PredictionModel trainedPredictionModel) {
    this.trainedPredictionModel = trainedPredictionModel;
  }

  /**
   * Returns the list of raw anomalies after anomaly detection.
   */
  public List<RawAnomalyResultDTO> getRawAnomalies() {
    return rawAnomalies;
  }

  /**
   * Sets the lists of raw anomalies. This method is supposed to be used by Detection Model.
   */
  public void setRawAnomalies(List<RawAnomalyResultDTO> rawAnomalies) {
    this.rawAnomalies = rawAnomalies;
  }
}
