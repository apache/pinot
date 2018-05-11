package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.datalayer.dto.DetectionAlertConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import java.lang.reflect.Constructor;


/**
 * The Detection alert filter loader.
 */
public class DetectionAlertFilterLoader {
  private static final String PROP_CLASS_NAME = "className";

  /**
   * Return a detection alert filter from detection alert filter.
   *
   * @param provider the provider
   * @param config the config
   * @param start the start
   * @param end the end
   * @return the detection alert filter
   * @throws Exception the exception
   */
  public DetectionAlertFilter from(DataProvider provider, DetectionAlertConfigDTO config, long start, long end)
      throws Exception {
    String className = config.getProperties().get(PROP_CLASS_NAME).toString();
    Constructor<?> constructor = Class.forName(className)
        .getConstructor(DataProvider.class, DetectionAlertConfigDTO.class, long.class, long.class);
    return (DetectionAlertFilter) constructor.newInstance(provider, config, start, end);
  }
}
