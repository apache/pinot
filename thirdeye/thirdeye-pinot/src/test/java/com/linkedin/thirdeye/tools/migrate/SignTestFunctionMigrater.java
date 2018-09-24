package com.linkedin.thirdeye.tools.migrate;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.StringUtils;
import java.util.HashMap;
import java.util.Properties;


public class SignTestFunctionMigrater extends BaseAnomalyFunctionMigrater {
  public static final String ANOMALY_FUNCTION_TYPE = "SIGN_TEST_WRAPPER";

  public SignTestFunctionMigrater() {
    defaultProperties = ImmutableMap.copyOf(new HashMap<String, String>(){
      {
        put(FUNCTION, "ConfigurableAnomalyDetectionFunction");
        put(moduleConfigKey(DATA), "SeasonalDataModule");
        put(moduleConfigKey(TRAINING_PREPROCESS), "AnomalyRemovalByWeight");
        put(moduleConfigKey(TESTING_PREPROCESS), "DummyPreprocessModule");
        put(moduleConfigKey(TRAINING), "nonparametric.ThresholdBasedSeasonalMovingAverageTrainingModule");
        put(moduleConfigKey(DETECTION), "SignTestDetectionModule");
        put(variableConfigKey("seasonalCount"), "3");
        put(variableConfigKey("seasonalPeriod"), "P7D");
        put(variableConfigKey("anomalyRemovalThreshold"), "0.6,-0.6");
        put(variableConfigKey("decayRate"), "0.0");
        put(variableConfigKey("pattern"), "UP,DOWN");
        put(variableConfigKey("signTestBaselineShift"), "0.0,0.0");
        put(variableConfigKey("signTestBaselineLift"), "1.0,1.0");
        put(variableConfigKey("pValueThreshold"), "0.05");
        put(variableConfigKey("signTestWindowSize"), "24");
        put(variableConfigKey("signTestStepSize"), "1");
      }
    });
    directKeyMap = ImmutableMap.copyOf(new HashMap<String, String>(){
      {
        put("anomalyRemovalThreshold", variableConfigKey("anomalyRemovalThreshold"));
        put("slidingWindowWidth", variableConfigKey("slidingWindowWidth"));
        put("decayRate", variableConfigKey("decayRate"));
        put("confidenceLevel", variableConfigKey("confidenceLevel"));
        put("signTestPattern", variableConfigKey("pattern"));
        put("pValueThreshold", variableConfigKey("pValueThreshold"));
        put("signTestWindowSize", variableConfigKey("signTestWindowSize"));
        put("signTestStepSize", variableConfigKey("signTestStepSize"));
      }
    });
  }

  @Override
  public void migrate(AnomalyFunctionDTO anomalyFunction) {
    Properties oldProperties = anomalyFunction.toProperties();
    Properties newProperties = applyDefaultProperties(new Properties());
    newProperties = mapNewKeys(oldProperties, newProperties);
    String shift = oldProperties.getProperty("signTestBaselineShift", "0.0");

    double lift_up = 1.0;
    double lift_down = 1.0;
    String signTestBaselineLift = oldProperties.getProperty("signTestBaselineLift", "1.0");
    if (signTestBaselineLift.contains(",")) {
      String[] tokens = signTestBaselineLift.split(",");
      lift_up = Double.valueOf(tokens[0]);
      lift_down = Double.valueOf(tokens[1]);
    } else {
      double lift = Double.valueOf(oldProperties.getProperty("signTestBaselineLift", "1.0"));
      lift_up = (lift > 1.0) ? lift : 2 - lift;
      lift_down = (lift > 1.0) ? 2 - lift : lift;
    }
    if (oldProperties.containsKey("anomalyRemovalThreshold")) {
      double anomalyRemovalThreshold = Math.abs(Double.valueOf(oldProperties.getProperty("anomalyRemovalThreshold")));
      newProperties.put(variableConfigKey("anomalyRemovalThreshold"), String.format("%.1f,%.1f",
          anomalyRemovalThreshold, -1 * anomalyRemovalThreshold));
    }
    newProperties.setProperty(variableConfigKey("signTestBaselineShift"), String.format("%s,%s", shift, shift));
    newProperties.setProperty(variableConfigKey("signTestBaselineLift"), String.format("%.1f,%.1f", lift_up, lift_down));
    anomalyFunction.setType(ANOMALY_FUNCTION_TYPE);
    anomalyFunction.setProperties(StringUtils.encodeCompactedProperties(newProperties));
  }
}
