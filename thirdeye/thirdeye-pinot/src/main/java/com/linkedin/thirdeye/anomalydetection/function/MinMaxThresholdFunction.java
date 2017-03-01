package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.model.data.DataModel;
import com.linkedin.thirdeye.anomalydetection.model.data.NoopDataModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.DetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.MinMaxThresholdDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.NoopDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.MergeModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.MinMaxThresholdMergeModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.NoopMergeModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.NoopPredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.TransformationFunction;
import com.linkedin.thirdeye.anomalydetection.model.transform.ZeroRemovalFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.ArrayList;
import java.util.List;

public class MinMaxThresholdFunction extends AbstractModularizedAnomalyFunction {
  public static final String MIN_VAL = "min";
  public static final String MAX_VAL = "max";

  private DataModel dataModel = new NoopDataModel();
  private List<TransformationFunction> currentTimeSeriesTransformationChain = new ArrayList<>();
  private List<TransformationFunction> baselineTimeSeriesTransformationChain = new ArrayList<>();
  private PredictionModel predictionModel = new NoopPredictionModel();
  private DetectionModel detectionModel = new NoopDetectionModel();
  private MergeModel mergeModel = new NoopMergeModel();

  public static String[] getPropertyKeys() {
    return new String [] {MIN_VAL, MAX_VAL};
  }

  @Override
  public void init(AnomalyFunctionDTO spec) throws Exception {
    super.init(spec);

    // Removes zeros from time series, which currently mean empty values in ThirdEye.
    TransformationFunction zeroRemover = new ZeroRemovalFunction();
    currentTimeSeriesTransformationChain.add(zeroRemover);
    baselineTimeSeriesTransformationChain.add(zeroRemover);

    detectionModel = new MinMaxThresholdDetectionModel();
    detectionModel.init(properties);

    mergeModel = new MinMaxThresholdMergeModel();
    mergeModel.init(properties);
  }

  @Override public DataModel getDataModel() {
    return dataModel;
  }

  @Override public List<TransformationFunction> getCurrentTimeSeriesTransformationChain() {
    return currentTimeSeriesTransformationChain;
  }

  @Override public List<TransformationFunction> getBaselineTimeSeriesTransformationChain() {
    return baselineTimeSeriesTransformationChain;
  }

  @Override public PredictionModel getPredictionModel() {
    return predictionModel;
  }

  @Override public DetectionModel getDetectionModel() {
    return detectionModel;
  }

  @Override public MergeModel getMergeModel() {
    return mergeModel;
  }
}
