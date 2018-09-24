package com.linkedin.thirdeye.tools.migrate;

import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.util.StringUtils;
import java.util.HashMap;
import java.util.Properties;


public class RegressianGaussianScanFunctionMigrater extends BaseAnomalyFunctionMigrater {
  public static final String ANOMALY_FUNCTION_TYPE = "REGRESSION_GAUSSIAN_SCAN_WRAPPER";

  public RegressianGaussianScanFunctionMigrater() {
    defaultProperties = ImmutableMap.copyOf(new HashMap<String, String>(){
      {
        put(FUNCTION, "ConfigurableAnomalyDetectionFunction");
        put(moduleConfigKey(DATA), "ContinuumDataModule");
        put(moduleConfigKey(TRAINING_PREPROCESS), "AnomalyRemovalByWeight");
        put(moduleConfigKey(TESTING_PREPROCESS), "AnomalyRemovalByWeight");
        put(moduleConfigKey(TRAINING), "parametric.NullBasisRegressionTrainingModule");
        put(moduleConfigKey(DETECTION), "GaussianScanDetectionModule");
        put(variableConfigKey("continuumOffset"), "P60D");
        put(variableConfigKey("seasonalities"), "HOURLY_SEASONALITY,DAILY_SEASONALITY");
        put(variableConfigKey("anomalyRemovalThreshold"), "1.0,-1.0");
        put(variableConfigKey("scanStepSize"), "1");
        put(variableConfigKey("scanMinWindowSize"), "1");
        put(variableConfigKey("scanMaxWindowSize"), "48");
        put(variableConfigKey("scanUseBootStrap"), "true");
        put(variableConfigKey("scanNumSimulations"), "500");
        put(variableConfigKey("pValueThreshold"), "0.01");
        put(variableConfigKey("scanTargetNumAnomalies"), "1");
      }
    });
    directKeyMap = ImmutableMap.copyOf(new HashMap<String, String>(){
      {
        put("scanMinWindowSize", variableConfigKey("scanMinWindowSize"));
        put("scanMaxWindowSize", variableConfigKey("scanMaxWindowSize"));
        put("scanUseBootStrap", variableConfigKey("scanUseBootStrap"));
        put("scanNumSimulations", variableConfigKey("scanNumSimulations"));
        put("scanPValueThreshold", variableConfigKey("pValueThreshold"));
        put("scanTargetNumAnomalies", variableConfigKey("scanTargetNumAnomalies"));
      }
    });
  }

  @Override
  public void migrate(AnomalyFunctionDTO anomalyFunction) {
    Properties oldProperties = anomalyFunction.toProperties();
    Properties newProperties = applyDefaultProperties(new Properties());
    newProperties = mapNewKeys(oldProperties, newProperties);
    int continuumOffset = Integer.valueOf(oldProperties.getProperty("continuumOffsetSize", "1440")) / 24;
    newProperties.put(variableConfigKey("continuumOffset"), String.format("P%dD", continuumOffset));
    if (oldProperties.containsKey("anomalyRemovalThreshold")) {
      double anomalyRemovalThreshold = Math.abs(Double.valueOf(oldProperties.getProperty("anomalyRemovalThreshold")));
      newProperties.put(variableConfigKey("anomalyRemovalThreshold"), String.format("%.1f,%.1f",
          anomalyRemovalThreshold, -1 * anomalyRemovalThreshold));
    }
    anomalyFunction.setProperties(StringUtils.encodeCompactedProperties(newProperties));
    anomalyFunction.setType(ANOMALY_FUNCTION_TYPE);
  }
}
