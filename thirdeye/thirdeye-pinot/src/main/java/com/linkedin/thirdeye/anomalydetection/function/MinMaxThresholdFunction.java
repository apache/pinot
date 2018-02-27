package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomaly.views.AnomalyTimelinesView;
import com.linkedin.thirdeye.anomalydetection.context.AnomalyDetectionContext;
import com.linkedin.thirdeye.anomalydetection.context.TimeSeries;
import com.linkedin.thirdeye.anomalydetection.model.data.DataModel;
import com.linkedin.thirdeye.anomalydetection.model.data.NoopDataModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.DetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.MinMaxThresholdDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.NoopDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.MergeModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.MinMaxThresholdMergeModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.NoopMergeModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.ExpectedTimeSeriesPredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.NoopPredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.TransformationFunction;
import com.linkedin.thirdeye.anomalydetection.model.transform.ZeroRemovalFunction;
import com.linkedin.thirdeye.dashboard.views.TimeBucket;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import java.util.ArrayList;
import java.util.List;

public class MinMaxThresholdFunction extends AbstractModularizedAnomalyFunction {
  public static final String MIN_VAL = "min";
  public static final String MAX_VAL = "max";

  public String[] getPropertyKeys() {
    return new String [] {MIN_VAL, MAX_VAL};
  }

  @Override
  public void init(AnomalyFunctionDTO spec) throws Exception {
    super.init(spec);

    // Removes zeros from time series, which currently mean empty values in ThirdEye.
    TransformationFunction zeroRemover = new ZeroRemovalFunction();
    currentTimeSeriesTransformationChain.add(zeroRemover);

    detectionModel = new MinMaxThresholdDetectionModel();
    detectionModel.init(properties);

    mergeModel = new MinMaxThresholdMergeModel();
    mergeModel.init(properties);
  }

  @Override
  public AnomalyTimelinesView getTimeSeriesView(AnomalyDetectionContext anomalyDetectionContext, long bucketMillis,
      String metric, long viewWindowStartTime, long viewWindowEndTime, List<MergedAnomalyResultDTO> knownAnomalies) {

    AnomalyTimelinesView anomalyTimelinesView = super
        .getTimeSeriesView(anomalyDetectionContext, bucketMillis, metric, viewWindowStartTime, viewWindowEndTime,
            knownAnomalies);

    // Get min / max props
    Double min = null;
    if (properties.containsKey(MIN_VAL)) {
      min = Double.valueOf(properties.getProperty(MIN_VAL));
    }
    Double max = null;
    if (properties.containsKey(MAX_VAL)) {
      max = Double.valueOf(properties.getProperty(MAX_VAL));
    }

    double value = 0d;
    if (min != null) {
      value = min;
    } else if (max != null) {
      value = max;
    }

    int bucketCount = (int) ((viewWindowEndTime - viewWindowStartTime) / bucketMillis);
    for (int i = 0; i < bucketCount; ++i) {
      anomalyTimelinesView.addBaselineValues(value);
    }

    return anomalyTimelinesView;
  }

  @Override
  public boolean useHistoryAnomaly() {
    return false;
  }
}
