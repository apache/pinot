package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.model.data.DataModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.DetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.MergeModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.TransformationFunction;
import java.util.List;

public interface ModularizedAnomalyFunctionModelProvider {

  /**
   * Returns the data model for retrieve current and training time series.
   * @return a data model for retrieve current and training time series.
   */
  DataModel getDataModel();

  /**
   * Returns the chain of transformation functions for current time series.
   * @return a chain of transformation functions.
   */
  List<TransformationFunction> getCurrentTimeSeriesTransformationChain();

  /**
   * Returns the chain of transformation functions for training time series (baselines).
   * @return a chain of transformation functions.
   */
  List<TransformationFunction> getBaselineTimeSeriesTransformationChain();

  /**
   * Returns the prediction model to be trained and used for forecasting.
   * @return a prediction model to be trained and used for forecasting.
   */
  PredictionModel getPredictionModel();

  /**
   * Returns the detection model for testing and detecting the anomalies in observerd time series.
   * @return a detection model for testing and detecting the anomalies in observerd time series.
   */
  DetectionModel getDetectionModel();

  /**
   * Returns the merge model for calculating score, weight, etc. of merged anomalies.
   * @return a merge model for calculating score, weight, etc. of merged anomalies.
   */
  MergeModel getMergeModel();
}
