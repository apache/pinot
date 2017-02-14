package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.AlertFilterAutotuneFactory;
import com.linkedin.thirdeye.anomalydetection.alertFilterAutotune.BaseAlertFilterAutotune;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by ychung on 2/9/17.
 */
public class AlertFilterFactory {
  private static Logger LOGGER = LoggerFactory.getLogger(AlertFilterFactory.class);
  private final Properties props;

  private static final String OLD_ALERTFILTER_TYPE = "alpha_beta";
  private static final String NEW_ALERTFILTER_TYPE = "alpha_beta_logistic";
  private static final String ALERTFILTER_INTERCEPT = "INTERCEPT";
  private static final String ALERTFILTER_SLOPE = "slope";
  private static final String ALERTFILTER_BETA = "beta";
  private static final String ALERTFILTER_THRESHOLD = "threshold";
  private static final String ALERTFILTER_TYPE = "type";

  private static Map<String, String> getDefaultAlphaBetaLogisticAlertFilter(){
    Map<String, String> defaultParams = new HashMap<>();
    defaultParams.put(ALERTFILTER_INTERCEPT, "1");
    defaultParams.put(ALERTFILTER_SLOPE,"0");
    defaultParams.put(ALERTFILTER_BETA, "0");
    defaultParams.put(ALERTFILTER_THRESHOLD, "0.5");
    defaultParams.put(ALERTFILTER_TYPE, NEW_ALERTFILTER_TYPE);
    return defaultParams;
  }

  public AlertFilterFactory(String functionConfigPath) {
    props = new Properties();
    try {
      InputStream input = new FileInputStream(functionConfigPath);
      loadPropertiesFromInputStream(input);
    } catch (FileNotFoundException e) {
      LOGGER.error("File {} not found", functionConfigPath, e);
    }

  }

  public AlertFilterFactory(InputStream input) {
    props = new Properties();
    loadPropertiesFromInputStream(input);
  }

  private void loadPropertiesFromInputStream(InputStream input) {
    try {
      props.load(input);
    } catch (IOException e) {
      LOGGER.error("Error loading the functions from config", e);
    } finally {
      IOUtils.closeQuietly(input);
    }

    LOGGER.info("Found {} entries in alert filter autotune configuration file {}", props.size());
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      LOGGER.info("{}: {}", entry.getKey(), entry.getValue());
    }
  }

  public AlertFilter fromSpec(AnomalyFunctionDTO anomalyFunctionSpec) throws Exception {
    AlertFilter alertFilter = null;
    String type = anomalyFunctionSpec.getType();
    if (!props.containsKey(type)) {
      throw new IllegalArgumentException("Unsupported type " + type);
    }
    String className = props.getProperty(type);
    alertFilter = (BaseAlertFilter) Class.forName(className).newInstance();
    alertFilter.setParameters(anomalyFunctionSpec.getAlertFilter());
    return alertFilter;
  }
}
