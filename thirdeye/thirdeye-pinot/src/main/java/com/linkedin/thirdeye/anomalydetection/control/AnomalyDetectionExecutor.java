package com.linkedin.thirdeye.anomalydetection.control;

import com.linkedin.thirdeye.anomalydetection.data.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.data.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.function.AnomalyDetectionFunction;
import com.linkedin.thirdeye.anomalydetection.model.detection.DetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.TransformationFunction;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datalayer.dto.RawAnomalyResultDTO;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;

/**
 * This class provides the control logic to perform actions on an anomaly detection context with
 * the given anomaly detection function. The actions can be anomaly detection, information update
 * of merged anomalies, etc. Ideally, this class provides methods to communicate with outer package.
 */
public class AnomalyDetectionExecutor {
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
  public static List<RawAnomalyResultDTO> analyze(AnomalyDetectionContext anomalyDetectionContext)
      throws Exception {
    if (!checkPrecondition(anomalyDetectionContext)) {
      return Collections.emptyList();
    }

    // Transform current and baseline time series and train the prediction model
    preparePredictionModel(anomalyDetectionContext);

    // Detect anomalies
    AnomalyDetectionFunction anomalyDetectionFunction = anomalyDetectionContext.getAnomalyDetectionFunction();
    DetectionModel detectionModel = anomalyDetectionFunction.getAnomalyDetectionModel();
    List<RawAnomalyResultDTO> rawAnomalies = detectionModel.detect(anomalyDetectionContext);

    return rawAnomalies;
  }

  /**
   * Updates the information of the given merged anomaly.
   *
   * @param anomalyDetectionContext
   * @param anomalyToUpdated
   * @throws Exception
   */
  public static void updateMergedAnomalyInfo(AnomalyDetectionContext anomalyDetectionContext,
      MergedAnomalyResultDTO anomalyToUpdated) throws Exception {
    if (checkPrecondition(anomalyDetectionContext)) {
      // Transform current and baseline time series and train the prediction model
      preparePredictionModel(anomalyDetectionContext);

      // TODO: Use Update Model to update the information of the merged anomaly
    }
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
  private static boolean checkPrecondition(AnomalyDetectionContext anomalyDetectionContext) {
    return anomalyDetectionContext != null && anomalyDetectionContext.getCurrent() != null
        && anomalyDetectionContext.getAnomalyDetectionFunction() != null;
  }

  /**
   * Performs the following operations on the given an anomaly detection context.
   * 1. Transform current and baseline time series.
   * 2. Train prediction model using the baseline time series.
   *
   * At the end of this method, a transformed current time series, transformed baselines, and a
   * trained prediction model are appended to the given anomaly detection context.
   *
   * The processed anomaly detection context has multiple usages. For example, it could be used for
   * detecting anomalies, plotting UI, updating the information of anomalies, etc.
   *
   * @param anomalyDetectionContext anomaly detection context that contains the necessary time
   *                                series for preparing the prediction model
   */
  private static void preparePredictionModel(AnomalyDetectionContext anomalyDetectionContext) {
    AnomalyDetectionFunction anomalyDetectionFunction = anomalyDetectionContext.getAnomalyDetectionFunction();

    // Transform the observed (current) time series
    if (anomalyDetectionContext.getTransformedCurrent() == null) {
      anomalyDetectionContext.setTransformedCurrent(anomalyDetectionContext.getCurrent());
    }
    List<TransformationFunction> currentTransformationChain =
        anomalyDetectionFunction.getCurrentTimeSeriesTransformationChain();
    if (CollectionUtils.isNotEmpty(currentTransformationChain)) {
      for (TransformationFunction tf : currentTransformationChain) {
        anomalyDetectionContext
            .setTransformedCurrent(tf.transform(anomalyDetectionContext.getTransformedCurrent()));
      }
    }

    // Transform baseline time series
    if (anomalyDetectionContext.getTransformedBaselines() == null) {
      anomalyDetectionContext.setTransformedBaselines(anomalyDetectionContext.getBaselines());
    }
    List<TransformationFunction> baselineTransformationChain =
        anomalyDetectionFunction.getCurrentTimeSeriesTransformationChain();
    if (CollectionUtils.isNotEmpty(baselineTransformationChain)) {
      for (TransformationFunction tf : baselineTransformationChain) {
        List<TimeSeries> transformedBaselines = new ArrayList<>();
        for (TimeSeries ts : transformedBaselines) {
          TimeSeries transformedTS = tf.transform(ts);
          transformedBaselines.add(transformedTS);
        }
        anomalyDetectionContext.setTransformedBaselines(transformedBaselines);
      }
    }

    // Train Prediction Model
    PredictionModel predictionModel = anomalyDetectionFunction.getPredictionModel();
    predictionModel.train(anomalyDetectionContext.getTransformedBaselines());
    anomalyDetectionContext.setTrainedPredictionModel(predictionModel);
  }
}
