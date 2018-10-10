package com.linkedin.thirdeye.tools.migrate;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.StringUtils;
import java.util.HashMap;
import java.util.Properties;


public class GenericSplineFunctionMigrater extends BaseAnomalyFunctionMigrater{
  public static final String ANOMALY_FUNCTION_TYPE = "SPLINE_REGRESSION_WRAPPER";

  public GenericSplineFunctionMigrater() {
    defaultProperties = ImmutableMap.copyOf(new HashMap<String, String>(){
      {
        put(FUNCTION, "ConfigurableAnomalyDetectionFunction");
        put(WORKFLOW, "RegressionWorkflow");
        put(moduleConfigKey(DATA), "ContinuumDataModule");
        put(moduleConfigKey(TRAINING_PREPROCESS), "AnomalyRemovalByWeight");
        put(moduleConfigKey(TESTING_PREPROCESS), "DummyPreprocessModule");
        put(moduleConfigKey(TRAINING), "parametric.GenericSplineTrainingModule");
        put(moduleConfigKey(DETECTION), "ConfidenceIntervalDetectionModule");
        put(variableConfigKey("continuumOffset"), "P90D");
        put(variableConfigKey("anomalyRemovalThreshold"), "0.6,-0.6");
        put(variableConfigKey("seasonalities"), "DAILY_SEASONALITY");
        put(variableConfigKey("degree"), "3");
        put(variableConfigKey("numberOfKnots"), "0");
        put(variableConfigKey("pValueThreshold"), "0.025");
        put(variableConfigKey("recentPeriod"), "P14D");
        put(variableConfigKey("r2Cutoff"), "0.9");
        put(variableConfigKey("predictionMode"), "TRENDING");
      }
    });
    directKeyMap = ImmutableMap.copyOf(new HashMap<String, String>(){
      {
        put("anomalyRemovalThreshold", variableConfigKey("anomalyRemovalThreshold"));
        put("splineDegree", variableConfigKey("degree"));
        put("numberOfKnots", variableConfigKey("numberOfKnots"));
        put("pValueThreshold", variableConfigKey("pValueThreshold"));
        put("r2Cutoff", variableConfigKey("r2Cutoff"));
      }
    });
  }

  @Override
  public void migrate(AnomalyFunctionDTO anomalyFunction) {
    Properties oldProperties = anomalyFunction.toProperties();
    Properties newProperties = applyDefaultProperties(new Properties());
    newProperties = mapNewKeys(oldProperties, newProperties);
    int continuumOffset = Integer.valueOf(oldProperties.getProperty("continuumOffsetSize", "90"));
    newProperties.put(variableConfigKey("continuumOffset"), String.format("P%dD", continuumOffset));
//    if (!Boolean.valueOf(oldProperties.getProperty("useRobustBaseline", "true"))) {
//      newProperties.put(moduleConfigKey(TRAINING), "parametric.SplineRegressionTrainingModule");
//      newProperties.put(variableConfigKey("numberOfKnots"), "0");
//    }
//    if (Boolean.valueOf(oldProperties.getProperty("applyLogTransform", "false"))) {
//      newProperties.put(variableConfigKey("transformation"), "BOX_COX_TRANSFORM");
//    }
//    if (!Boolean.valueOf(oldProperties.getProperty("weeklyEffectModeled", "true"))) {
//      newProperties.remove(variableConfigKey("seasonalities"));
//    }
//    if (oldProperties.containsKey("anomalyRemovalThreshold")) {
//      double anomalyRemovalThreshold = Math.abs(Double.valueOf(oldProperties.getProperty("anomalyRemovalThreshold")));
//      newProperties.put(variableConfigKey("anomalyRemovalThreshold"), String.format("%.1f,%.1f",
//          anomalyRemovalThreshold, -1 * anomalyRemovalThreshold));
//    }
    anomalyFunction.setType(ANOMALY_FUNCTION_TYPE);
    anomalyFunction.setProperties(StringUtils.encodeCompactedProperties(newProperties));
  }
}
