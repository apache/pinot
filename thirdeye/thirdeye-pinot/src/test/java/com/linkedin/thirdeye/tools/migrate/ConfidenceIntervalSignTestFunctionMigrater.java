package com.linkedin.thirdeye.tools.migrate;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.StringUtils;
import java.util.HashMap;
import java.util.Properties;
import org.joda.time.Period;


public class ConfidenceIntervalSignTestFunctionMigrater extends BaseAnomalyFunctionMigrater {
  public static final String ANOMALY_FUNCTION_TYPE = "SIGN_TEST_WRAPPER";

  public ConfidenceIntervalSignTestFunctionMigrater() {
    defaultProperties = ImmutableMap.copyOf(new HashMap<String, String>(){
      {
        put(FUNCTION, "ConfigurableAnomalyDetectionFunction");
        put(moduleConfigKey(DATA), "SeasonalDataModule");
        put(moduleConfigKey(TRAINING_PREPROCESS), "AnomalyRemovalByWeight");
        put(moduleConfigKey(TESTING_PREPROCESS), "DummyPreprocessModule");
        put(moduleConfigKey(TRAINING), "nonparametric.SeasonalSlidingWindowTrainingModule");
        put(moduleConfigKey(DETECTION), "SignTestDetectionModule");
        put(variableConfigKey("seasonalCount"), "3");
        put(variableConfigKey("seasonalPeriod"), "P7D");
        put(variableConfigKey("anomalyRemovalThreshold"), "0.6,-0.6");
        put(variableConfigKey("slidingWindowWidth"), "8");
        put(variableConfigKey("decayRate"), "0.5");
        put(variableConfigKey("confidenceLevel"), "0.99");
        put(variableConfigKey("pattern"), "UP,DOWN");
        put(variableConfigKey("pValueThreshold"), "0.05");
        put(variableConfigKey("signTestWindowSize"), "24");
        put(variableConfigKey("signTestStepSize"), "1");
      }
    });
    directKeyMap = ImmutableMap.copyOf(new HashMap<String, String>(){
      {
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
    if (oldProperties.containsKey("anomalyRemovalThreshold")) {
      double anomalyRemovalThreshold = Math.abs(Double.valueOf(oldProperties.getProperty("anomalyRemovalThreshold")));
      newProperties.put(variableConfigKey("anomalyRemovalThreshold"), String.format("%.1f,%.1f",
          anomalyRemovalThreshold, -1 * anomalyRemovalThreshold));
    }
    int slidingWindowWidth = Integer.valueOf(oldProperties.getProperty("slidingWindowWidth", "8"));
    Period slidingWindowPeriod = (new TimeGranularity(slidingWindowWidth/2 * anomalyFunction.getBucketSize(),
        anomalyFunction.getBucketUnit())).toPeriod();
    newProperties.put(variableConfigKey("trainPadding"), slidingWindowPeriod.toString() + "," + slidingWindowPeriod.toString());
    anomalyFunction.setProperties(StringUtils.encodeCompactedProperties(newProperties));
    anomalyFunction.setType(ANOMALY_FUNCTION_TYPE);
  }
}
