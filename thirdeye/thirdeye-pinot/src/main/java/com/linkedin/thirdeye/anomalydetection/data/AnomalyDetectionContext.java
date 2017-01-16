package com.linkedin.thirdeye.anomalydetection.data;

import com.linkedin.thirdeye.anomalydetection.model.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.List;

/**
 * The context for performing an anomaly detection on the sets of time series from the same
 * dimension and metric.
 */
public class AnomalyDetectionContext {
  private AnomalyDetectionFunction anomalyDetectionFunction;

  private TimeSeriesKey timeSeriesKey;

  private TimeSeries current;
  private List<TimeSeries> baselines;

  private TimeSeries transformedCurrent;
  private List<TimeSeries> transformedBaselines;

  private PredictionModel trainedPredictionModel;

  private List<RawAnomalyResultDTO> rawAnomalies;


  public TimeSeriesKey getTimeSeriesKey() {
    return timeSeriesKey;
  }

  public void setTimeSeriesKey(TimeSeriesKey key) {
    this.timeSeriesKey = key;
  }

  public TimeSeries getCurrent() {
    return current;
  }

  public void setCurrent(TimeSeries current) {
    this.current = current;
  }

  public List<TimeSeries> getBaselines() {
    return baselines;
  }

  public void setBaselines(List<TimeSeries> baselines) {
    this.baselines = baselines;
  }

  public AnomalyDetectionFunction getAnomalyDetectionFunction() {
    return anomalyDetectionFunction;
  }

  public void setAnomalyDetectionFunction(AnomalyDetectionFunction anomalyDetectionFunction) {
    this.anomalyDetectionFunction = anomalyDetectionFunction;
  }

  public TimeSeries getTransformedCurrent() {
    return transformedCurrent;
  }

  public void setTransformedCurrent(TimeSeries transformedCurrent) {
    this.transformedCurrent = transformedCurrent;
  }

  public List<TimeSeries> getTransformedBaselines() {
    return transformedBaselines;
  }

  public void setTransformedBaselines(List<TimeSeries> transformedBaselines) {
    this.transformedBaselines = transformedBaselines;
  }

  public PredictionModel getTrainedPredictionModel() {
    return trainedPredictionModel;
  }

  public void setTrainedPredictionModel(PredictionModel trainedPredictionModel) {
    this.trainedPredictionModel = trainedPredictionModel;
  }

  public List<RawAnomalyResultDTO> getRawAnomalies() {
    return rawAnomalies;
  }

  public void setRawAnomalies(List<RawAnomalyResultDTO> rawAnomalies) {
    this.rawAnomalies = rawAnomalies;
  }
}
