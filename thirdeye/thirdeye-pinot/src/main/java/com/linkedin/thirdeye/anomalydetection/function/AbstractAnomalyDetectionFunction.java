package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.Utils;
import com.linkedin.thirdeye.anomalydetection.model.data.DataModel;
import com.linkedin.thirdeye.anomalydetection.model.data.NoopDataModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.DetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.NoopDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.NoopPredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.TransformationFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractAnomalyDetectionFunction implements AnomalyDetectionFunction {
  protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

  protected AnomalyFunctionDTO spec;
  protected Properties properties;

  protected DataModel dataModel = new NoopDataModel();
  protected List<TransformationFunction> currentTimeSeriesTransformationChain = new ArrayList<>();
  protected List<TransformationFunction> baselineSeriesTransformationChain = new ArrayList<>();
  protected PredictionModel predictionModel = new NoopPredictionModel();
  protected DetectionModel detectionModel = new NoopDetectionModel();

  @Override
  public void init(AnomalyFunctionDTO spec) throws Exception {
    this.spec = spec;
    this.properties = Utils.parseSpec(this.spec);
  }

  @Override
  public AnomalyFunctionDTO getSpec() {
    return spec;
  }

  @Override
  public List<Interval> getTimeSeriesIntervals(long monitoringWindowStartTime,
      long monitoringWindowEndTime) {
    return dataModel.getTrainingDataIntervals(monitoringWindowStartTime, monitoringWindowEndTime);
  }

  @Override
  public List<TransformationFunction> getCurrentTimeSeriesTransformationChain() {
    return currentTimeSeriesTransformationChain;
  }

  @Override
  public List<TransformationFunction> getBaselineTimeSeriesTransformationChain() {
    return baselineSeriesTransformationChain;
  }

  @Override
  public PredictionModel getPredictionModel() {
    return predictionModel;
  }

  @Override
  public DetectionModel getDetectionModel() {
    return detectionModel;
  }
}
