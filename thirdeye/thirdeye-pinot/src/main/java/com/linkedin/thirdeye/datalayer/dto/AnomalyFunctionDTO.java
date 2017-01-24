package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.datalayer.pojo.AnomalyFunctionBean;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnomalyFunctionDTO extends AnomalyFunctionBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(AnomalyFunctionDTO.class);

  public static final String BUCKET_SIZE = "bucketSize";
  public static final String BUCKET_UNIT = "bucketUnit";

  /**
   * Parses the properties of String and returns the corresponding Properties object.
   *
   * @return a Properties object corresponds to the properties String of this anomaly function.
   */
  public Properties toProperties() {
    Properties props = new Properties();

    props.put(BUCKET_SIZE, getBucketSize());
    props.put(BUCKET_UNIT, getBucketUnit());

    if (this.getProperties() != null) {
      String[] tokens = getProperties().split(";");
      for (String token : tokens) {
        try {
          props.load(new ByteArrayInputStream(token.getBytes()));
        } catch (IOException e) {
          LOGGER.warn("Failed to parse property string ({}) for anomaly function: {}", token, getId());
        }
      }
    }
    return props;
  }
}
