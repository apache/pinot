package com.linkedin.thirdeye.detection.yaml;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.anomalydetection.function.MinMaxThresholdFunction;
import com.linkedin.thirdeye.anomalydetection.function.WeekOverWeekRuleFunction;
import com.linkedin.thirdeye.detection.alert.filter.ToAllRecipientsDetectionAlertFilter;
import com.linkedin.thirdeye.detection.algorithm.BaselineAlgorithm;
import com.linkedin.thirdeye.detection.algorithm.MovingWindowAlgorithm;
import com.linkedin.thirdeye.detection.algorithm.stage.BaselineRuleFilterStage;
import com.linkedin.thirdeye.detector.email.filter.AlphaBetaAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.AverageChangeThresholdAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.DummyAlertFilter;
import com.linkedin.thirdeye.detector.email.filter.WeightThresholdAlertFilter;
import java.util.Map;


/**
 * The static map that converts rule/algorithm/filter names to class name
 */
public class DetectionRegistry {
  private static final Map<String, String> REGISTRY_MAP = ImmutableMap.<String, String>builder()
      // rule filter
      .put("BUSINESS_RULE_FILTER", BaselineRuleFilterStage.class.getName())
      // rule detection
      .put("BASELINE", String.valueOf(BaselineAlgorithm.class.getName()))
      .put("MOVING_WINDOW", MovingWindowAlgorithm.class.getName())
      // alerter
      .put("TO_ALL_RECIPIENTS", ToAllRecipientsDetectionAlertFilter.class.getName())
      // algorithm detection
      .put("REGRESSION_GAUSSIAN_SCAN", "com.linkedin.anomalydetection.function.RegressionGaussianScanFunction")
      .put("SPLINE_REGRESSION_VANILLA", "com.linkedin.anomalydetection.function.SplineRegressionFunction")
      .put("KALMAN_FILTER", "com.linkedin.thirdeye.controller.mp.function.KalmanThirdEyeFunction")
      .put("SCAN_STATISTICS", "com.linkedin.thirdeye.controller.mp.function.ScanStatisticsThirdEyeFunction")
      .put("SIGN_TEST", "com.linkedin.thirdeye.controller.mp.function.SignTestThirdEyeFunction")
      .put("SPLINE_REGRESSION", "com.linkedin.thirdeye.controller.mp.function.SplineRegressionThirdEyeFunction")
      .put("WEEK_OVER_WEEK_RULE", WeekOverWeekRuleFunction.class.getName())
      .put("MIN_MAX_THRESHOLD", MinMaxThresholdFunction.class.getName())
      .put("SIGN_TEST_VANILLA", "com.linkedin.anomalydetection.function.SignTestFunction")
      .put("CONFIDENCE_INTERVAL_SIGN_TEST", "com.linkedin.anomalydetection.function.ConfidenceIntervalSignTestFunction")
      .put("MOVING_AVERAGE_SIGN_TEST", "com.linkedin.anomalydetection.function.MovingAverageSignTestFunction")

      // algorithm alert filter
      .put("ALPHA_BETA_LOGISTIC_TWO_SIDE", "com.linkedin.filter.AlphaBetaLogisticAlertFilterTwoSide")
      .put("ALPHA_BETA_LOGISTIC", "com.linkedin.filter.AlphaBetaLogisticAlertFilter")
      .put("THRESHOLD_BASED", "com.linkedin.filter.ThresholdBasedAlertFilter")
      .put("WEIGHT_THRESHOLD", WeightThresholdAlertFilter.class.getName())
      .put("ALPHA_BETA", AlphaBetaAlertFilter.class.getName())
      .put("AVERAGE_CHANGE_THRESHOLD", AverageChangeThresholdAlertFilter.class.getName())
      .put("DUMMY", DummyAlertFilter.class.getName())
      .build();

  /**
   * Singleton
   */
  public static DetectionRegistry getInstance() {
    return INSTANCE;
  }

  /**
   * Internal constructor.
   */
  private DetectionRegistry() {
  }

  private static final DetectionRegistry INSTANCE = new DetectionRegistry();

  public String lookup(String type) {
    return REGISTRY_MAP.get(type.toUpperCase());
  }
}
