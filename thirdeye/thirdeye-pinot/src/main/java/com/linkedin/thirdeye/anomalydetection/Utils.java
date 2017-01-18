package com.linkedin.thirdeye.anomalydetection;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

public class Utils {
  /**
   * Parses an anomaly function specification and returns the corresponding property object.
   *
   * @param spec the anomaly function spec
   * @return the corresponding property object
   * @throws IOException
   */
  public static Properties parseSpec(AnomalyFunctionDTO spec) throws IOException {
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
