package com.linkedin.thirdeye.anomalydetection.function;

import com.linkedin.thirdeye.anomalydetection.model.data.DataModel;
import com.linkedin.thirdeye.anomalydetection.model.data.NoopDataModel;
import com.linkedin.thirdeye.anomalydetection.model.data.SeasonalDataModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.DetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.NoopDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.detection.SimpleThresholdDetectionModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.MergeModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.NoopMergeModel;
import com.linkedin.thirdeye.anomalydetection.model.merge.SimplePercentageMergeModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.NoopPredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.PredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.prediction.SeasonalAveragePredictionModel;
import com.linkedin.thirdeye.anomalydetection.model.transform.MovingAverageSmoothingFunction;
import com.linkedin.thirdeye.anomalydetection.model.transform.TotalCountThresholdRemovalFunction;
import com.linkedin.thirdeye.anomalydetection.model.transform.TransformationFunction;
import com.linkedin.thirdeye.anomalydetection.model.transform.ZeroRemovalFunction;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

public class WeekOverWeekRuleFunction extends AbstractModularizedAnomalyFunction {
  public static final String BASELINE = "baseline";
  public static final String ENABLE_SMOOTHING = "enableSmoothing";

  private DataModel dataModel = new NoopDataModel();
  private List<TransformationFunction> currentTimeSeriesTransformationChain = new ArrayList<>();
  private List<TransformationFunction> baselineTimeSeriesTransformationChain = new ArrayList<>();
  private PredictionModel predictionModel = new NoopPredictionModel();
  private DetectionModel detectionModel = new NoopDetectionModel();
  private MergeModel mergeModel = new NoopMergeModel();

  public static String[] getPropertyKeys() {
    return new String[] { BASELINE, ENABLE_SMOOTHING,
        MovingAverageSmoothingFunction.MOVING_AVERAGE_SMOOTHING_WINDOW_SIZE,
        SimpleThresholdDetectionModel.AVERAGE_VOLUME_THRESHOLD,
        SimpleThresholdDetectionModel.CHANGE_THRESHOLD
    };
  }

  @Override
  public void init(AnomalyFunctionDTO spec) throws Exception {
    super.init(spec);
    this.init(this.properties);
  }

  public void init(Properties properties) {
    this.properties = properties;

    String baselineProp = this.properties.getProperty(BASELINE);
    if (StringUtils.isNotBlank(baselineProp)) {
      this.initPropertiesForDataModel(baselineProp);
    }
    dataModel = new SeasonalDataModel();
    dataModel.init(this.properties);

    // Removes zeros from time series, which currently mean empty values in ThirdEye.
    TransformationFunction zeroRemover = new ZeroRemovalFunction();
    currentTimeSeriesTransformationChain.add(zeroRemover);
    baselineTimeSeriesTransformationChain.add(zeroRemover);

    // Add total count threshold transformation
    if (this.properties.containsKey(TotalCountThresholdRemovalFunction.TOTAL_COUNT_METRIC_NAME)) {
      TransformationFunction totalCountThresholdFunction = new TotalCountThresholdRemovalFunction();
      totalCountThresholdFunction.init(this.properties);
      currentTimeSeriesTransformationChain.add(totalCountThresholdFunction);
    }

    // Add moving average smoothing transformation
    if (this.properties.containsKey(ENABLE_SMOOTHING)) {
      TransformationFunction movingAverageSoothingFunction = new MovingAverageSmoothingFunction();
      movingAverageSoothingFunction.init(this.properties);
      currentTimeSeriesTransformationChain.add(movingAverageSoothingFunction);
      baselineTimeSeriesTransformationChain.add(movingAverageSoothingFunction);
    }

    predictionModel = new SeasonalAveragePredictionModel();
    predictionModel.init(this.properties);

    detectionModel = new SimpleThresholdDetectionModel();
    detectionModel.init(this.properties);

    mergeModel = new SimplePercentageMergeModel();
    mergeModel.init(this.properties);
  }

  /**
   * Parses the human readable string of baseline property and sets up SEASONAL_PERIOD and
   * SEASONAL_SIZE.
   *
   * The string should be given in this regex format: [wW][/o][0-9]?[wW]. For example, this string
   * "Wo2W" means comparing the current week with the 2 week prior.
   *
   * If the string ends with "Avg", then the property becomes week-over-weeks-average. For instance,
   * "W/4wAvg" means comparing the current week with the average of the past 4 weeks.
   *
   * @param baselineProp The human readable string of baseline property.
   */
  private void initPropertiesForDataModel(String baselineProp) {
    // The basic settings for w/w
    this.properties.setProperty(SeasonalDataModel.SEASONAL_PERIOD, "1");
    this.properties.setProperty(SeasonalDataModel.SEASONAL_SIZE, "7");
    this.properties.setProperty(SeasonalDataModel.SEASONAL_UNIT, "DAYS");
    // Change the setting for different w/w types
    if (StringUtils.isBlank(baselineProp)) {
      return;
    }
    String intString = parseWowString(baselineProp);
    if (baselineProp.endsWith("Avg")) { // Week-Over-Weeks_Average
      // example: "w/4wAvg" --> SeasonalDataModel.SEASONAL_PERIOD = "4"
      this.properties.setProperty(SeasonalDataModel.SEASONAL_PERIOD, intString);
    } else { // Week-Over-Week
      // example: "w/2w" --> SeasonalDataModel.SEASONAL_SIZE = "14"
      int seasonalSize = Integer.valueOf(intString) * 7;
      this.properties.setProperty(SeasonalDataModel.SEASONAL_SIZE, Integer.toString(seasonalSize));
    }
  }

  /**
   * Returns the first integer of a string; returns 1 if no integer could be found.
   *
   * Examples:
   * 1. "w/w": returns 1
   * 2. "Wo4W": returns 4
   * 3. "W/343wABCD": returns 343
   * 4. "2abc": returns 2
   * 5. "A Random string 34 and it is 54 a long one": returns 34
   *
   * @param wowString a string.
   * @return the integer of a WoW string.
   */
  public static String parseWowString(String wowString) {
    if (StringUtils.isBlank(wowString)) {
      return "1";
    }

    char[] chars = wowString.toCharArray();
    int head = -1;
    for (int idx = 0; idx < chars.length; ++idx) {
      if ('0' <= chars[idx] && chars[idx] <= '9') {
        head = idx;
        break;
      }
    }
    if (head < 0) {
      return "1";
    }
    int tail = head + 1;
    for (; tail < chars.length; ++tail) {
      if (chars[tail] <= '0' || '9' <= chars[tail]) {
        break;
      }
    }
    return wowString.substring(head, tail);
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
