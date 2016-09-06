package com.linkedin.thirdeye.detector.function;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;

public abstract class AnomalyFunctionUtils {
  public static final String BASELINE = "baseline";

  private AnomalyFunctionUtils() {
  }

  public static long getBaselineOffset(AnomalyFunctionDTO spec) throws IOException {
    long baselineMillis;
    Properties props = new Properties();
    if (spec.getProperties() != null) {
      String[] tokens = spec.getProperties().split(";");
      for (String token : tokens) {
        props.load(new ByteArrayInputStream(token.getBytes()));
      }
    }
    String baselineProp = props.getProperty(BASELINE);
    if ("w/3w".equals(baselineProp)) {
      baselineMillis = TimeUnit.MILLISECONDS.convert(21, TimeUnit.DAYS);
    } else if ("w/2w".equals(baselineProp)) {
      baselineMillis = TimeUnit.MILLISECONDS.convert(14, TimeUnit.DAYS);
    } else {
      // default
      baselineMillis = TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS);
    }
    return baselineMillis;
  }
}
