package com.linkedin.thirdeye.anomaly.views.function;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;


public class AnomalyTimeSeriesViewUtils {
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
