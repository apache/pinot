package com.linkedin.thirdeye.detection.alert;

import com.linkedin.thirdeye.datalayer.dto.AlertConfigDTO;
import com.linkedin.thirdeye.detection.DataProvider;
import java.lang.reflect.Constructor;


public class DetectionAlertFilterLoader {
  private static final String PROP_CLASS_NAME = "className";

  public DetectionAlertFilter from(DataProvider provider, AlertConfigDTO config, long start, long end)
      throws Exception {
    String className = config.getProperties().get(PROP_CLASS_NAME).toString();
    Constructor<?> constructor =
        Class.forName(className).getConstructor(DataProvider.class, AlertConfigDTO.class, long.class, long.class);
    return (DetectionAlertFilter) constructor.newInstance(provider, config, start, end);
  }
}
