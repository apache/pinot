package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyTimeSeriesViewFactory {
  private static Logger LOG = LoggerFactory.getLogger(AnomalyTimeSeriesViewFactory.class);
  private final Properties props;

  public AnomalyTimeSeriesViewFactory(String viewConfigPath) {
    props = new Properties();

    try {
      InputStream input = new FileInputStream(viewConfigPath);
      loadPropertiesFromInputStream(input);
    } catch (FileNotFoundException e) {
      LOG.error("File {} not found", viewConfigPath, e);
    }
  }

  private void loadPropertiesFromInputStream(InputStream input) {
    try {
      props.load(input);
    } catch (IOException e) {
      LOG.error("Error loading the anomaly time series view from config", e);
    } finally {
      IOUtils.closeQuietly(input);
    }

    LOG.info("Found {} entries in anomaly time series view configuration file {}", props.size());
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      LOG.debug("{}: {}", entry.getKey(), entry.getValue());
    }
  }

  public AnomalyTimeSeriesView fromSpec(AnomalyFunctionDTO functionSpec) {
    AnomalyTimeSeriesView anomalyTimeSeriesView = null;
    String type = functionSpec.getType();
    if (props.containsKey(type)) {
      try {
        String className = props.getProperty(type);
        anomalyTimeSeriesView = (AnomalyTimeSeriesView) Class.forName(className).newInstance();
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        anomalyTimeSeriesView = null;
      }
    }
    if (anomalyTimeSeriesView == null) {
      LOG.info("Unable to determine view type: {}, for anomaly function {}; using default w/w view.", type,
          functionSpec.getFunctionName());
      anomalyTimeSeriesView = new WeekOverWeekAnomalyTimeSeriesView();
    }
    anomalyTimeSeriesView.init(functionSpec);
    return anomalyTimeSeriesView;
  }
}
