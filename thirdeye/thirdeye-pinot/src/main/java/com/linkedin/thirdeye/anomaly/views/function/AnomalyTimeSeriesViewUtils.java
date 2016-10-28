package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalyTimeSeriesViewUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyTimeSeriesViewUtils.class);
  private static final String VIEW_TYPE_KEY = "viewType";
  private static final String TIME_SERIES_VIEW_CLASS_PATH = "com.linkedin.thirdeye.anomaly.views.function.";

  public static AnomalyTimeSeriesView fromAnomalyFunctionSpec(AnomalyFunctionDTO spec) {
    AnomalyTimeSeriesView anomalyTimeSeriesView;
    Properties props;
    String className = null;
    try {
      props = getPropertiesFromSpec(spec);
      if (!props.containsKey(VIEW_TYPE_KEY)) {
        throw (new ClassNotFoundException());
      } else {
        className = getFullClassName(props.getProperty(VIEW_TYPE_KEY));
      }
      anomalyTimeSeriesView = (AnomalyTimeSeriesView) Class.forName(className).newInstance();
    } catch (Exception e) {
      LOG.info("Unable to determine view type: {}, for anomaly function {}; using default w/w view.", className,
          spec.getFunctionName());
      anomalyTimeSeriesView = new WeekOverWeekAnomalyTimeSeriesView();
    }
    anomalyTimeSeriesView.init(spec);
    return anomalyTimeSeriesView;
  }

  private static String getFullClassName(String className) {
    return TIME_SERIES_VIEW_CLASS_PATH + className;
  }

  public static Properties getPropertiesFromSpec(AnomalyFunctionDTO spec) throws IOException {
    Properties props = new Properties();
    if (spec.getProperties() != null) {
      String[] tokens = spec.getProperties().split(";");
      for (String token : tokens) {
        props.load(new ByteArrayInputStream(token.getBytes()));
      }
    }
    return props;
  }
}
