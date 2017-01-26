package com.linkedin.thirdeye.anomalydetection.model.prediction;

import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import java.util.List;

public class NoopPredictionModel extends AbstractPredictionModel {
  @Override public void train(List<TimeSeries> historicalTimeSeries,
      AnomalyDetectionContext anomalyDetectionContext) {

  }
}
