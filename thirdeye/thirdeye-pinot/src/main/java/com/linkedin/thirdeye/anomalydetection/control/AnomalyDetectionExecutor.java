package com.linkedin.thirdeye.anomalydetection.control;

import com.linkedin.thirdeye.anomalydetection.data.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.data.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.model.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomalydetection.model.detection.AnomalyDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.TransformationFunction;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public class AnomalyDetectionExecutor implements Callable<List<RawAnomalyResultDTO>> {

  AnomalyDetectionContext anomalyDetectionContext;

  public AnomalyDetectionExecutor() { }

  public AnomalyDetectionExecutor(AnomalyDetectionContext anomalyDetectionContext) {
    this.anomalyDetectionContext = anomalyDetectionContext;
  }

  public void setAnomalyDetectionContext(AnomalyDetectionContext anomalyDetectionContext) {
    this.anomalyDetectionContext = anomalyDetectionContext;
  }

  /**
   * The anomaly detection is executed in the following flow:
   * 1. Transform current and baseline time series.
   * 2. Train prediction model using the baseline time series.
   * 3. Detect anomalies on the observed (current) time series against the expected time series,
   *    which is computed by the prediction model.
   *
   * @return a list of raw anomalies
   * @throws Exception
   */
  @Override
  public List<RawAnomalyResultDTO> call() throws Exception {
    if (!checkPrecondition(anomalyDetectionContext)) {
      return Collections.emptyList();
    }
    AnomalyDetectionFunction anomalyDetectionFunction =
        anomalyDetectionContext.getAnomalyDetectionFunction();

    // Transform the observed (current) time series
    if (anomalyDetectionContext.getTransformedCurrent() == null) {
      anomalyDetectionContext.setTransformedCurrent(anomalyDetectionContext.getCurrent());
    }
    for (TransformationFunction tf : anomalyDetectionFunction.getCurrentTimeSeriesTransformationChain()) {
      anomalyDetectionContext
          .setTransformedCurrent(tf.transform(anomalyDetectionContext.getTransformedCurrent()));
    }

    // Transform baseline time series
    if (anomalyDetectionContext.getTransformedBaselines() == null) {
      anomalyDetectionContext.setTransformedBaselines(anomalyDetectionContext.getBaselines());
    }
    for (TransformationFunction tf : anomalyDetectionFunction.getBaselineTimeSeriesTransformationChain()) {
      List<TimeSeries> transformedBaselines = new ArrayList<>();
      for (TimeSeries ts : transformedBaselines) {
        TimeSeries transformedTS = tf.transform(ts);
        transformedBaselines.add(transformedTS);
      }
      anomalyDetectionContext.setTransformedBaselines(transformedBaselines);
    }

    // Train Prediction Model
    PredictionModel predictionModel = anomalyDetectionFunction.getPredictionModel();
    predictionModel.train(anomalyDetectionContext.getTransformedBaselines());
    anomalyDetectionContext.setTrainedPredictionModel(predictionModel);

    // Detect anomalies
    AnomalyDetectionModel anomalyDetectionModel = anomalyDetectionFunction.getAnomalyDetectionModel();
    List<RawAnomalyResultDTO> rawAnomalies = anomalyDetectionModel.detect(anomalyDetectionContext);

    return rawAnomalies;
  }

  /**
   * Returns true if the following conditions hold:
   * 1. Anomaly detection context is not null.
   * 2. Anomaly detection function is not null.
   * 3. Current time series is not null.
   *
   * Note: Baseline time series could be null for MIN_MAX_FUNCTION.
   *
   * @param anomalyDetectionContext the context for anomaly detection.
   * @return true if the context satisfies the pre-condition of anomaly detection.
   */
  private boolean checkPrecondition(AnomalyDetectionContext anomalyDetectionContext) {
    return anomalyDetectionContext != null && anomalyDetectionContext.getCurrent() != null
        && anomalyDetectionContext.getAnomalyDetectionFunction() != null;
  }
}
