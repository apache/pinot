package com.linkedin.thirdeye.detector.email.filter;

import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class AlertFilterFactory {
  private static Logger LOGGER = LoggerFactory.getLogger(AlertFilterFactory.class);
  public static final String FILTER_TYPE_KEY = "type";
  public static final String ALPHA_BETA_ALERT_FILTER = "alpha_beta";
  public static final String DUMMY_ALERT_FILTER = "dummy";
  private final Properties props;


  public AlertFilterFactory(String functionConfigPath) {
    props = new Properties();
    try {
      InputStream input = new FileInputStream(functionConfigPath);
      loadPropertiesFromInputStream(input);
    } catch (FileNotFoundException e) {
      LOGGER.error("Alert Filter Property File {} not found", functionConfigPath, e);
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
      LOGGER.error("Error loading the alert filters from config", e);
    } finally {
      IOUtils.closeQuietly(input);
    }

    LOGGER.info("Found {} entries in alert filter configuration file {}", props.size());
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      LOGGER.info("{}: {}", entry.getKey(), entry.getValue());
    }
  }


  /**
   * Initiates an alert filter for the given anomaly function.
   *
   * @param anomalyFunctionSpec the anomaly function that contains the alert filter spec.
   *
   * @return the alert filter specified by the alert filter spec or a dummy filter if the function
   * does not have an alert filter spec or this method fails to initiates an alert filter from the
   * spec.
   */
  public AlertFilter getAlertFilter(AnomalyFunctionBean anomalyFunctionSpec) {
    Map<String, String> alertFilterInfo = anomalyFunctionSpec.getAlertFilter();
    if (alertFilterInfo == null) {
      alertFilterInfo = Collections.emptyMap();
    }
    AlertFilter alertFilter = new DummyAlertFilter();
    if (alertFilterInfo.containsKey(FILTER_TYPE_KEY)) {
      String alertFilterType = alertFilterInfo.get(FILTER_TYPE_KEY);
      if(props.containsKey(alertFilterType.toUpperCase())) {
        String className = props.getProperty(alertFilterType.toUpperCase());
        try {
          alertFilter = (AlertFilter) Class.forName(className).newInstance();
        } catch (Exception e) {
          LOGGER.info(e.getMessage());
        }
      } else if(alertFilterType.equals(ALPHA_BETA_ALERT_FILTER)) {
        alertFilter = new AlphaBetaAlertFilter();
      }
    }
    alertFilter.setParameters(alertFilterInfo);
    return alertFilter;
  }
}
