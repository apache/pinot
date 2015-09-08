package com.linkedin.thirdeye.anomaly.server.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.linkedin.thirdeye.anomaly.api.function.AnomalyResult;

/**
 * Makes ftl template happy...
 */
public class AnomalyResultPrintable extends AnomalyResult {

  private final String propertiesString;

  public AnomalyResultPrintable(AnomalyResult ar) throws IOException {
    super(ar.isAnomaly(), ar.getTimeWindow(), ar.getAnomalyScore(), ar.getAnomalyVolume(), ar.getProperties());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    ar.getProperties().store(os, "ResultProperties");
    propertiesString = new String(os.toByteArray());
  }

  public String getPropertiesString() {
    return propertiesString;
  }
}