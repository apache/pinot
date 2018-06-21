package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import java.lang.reflect.Constructor;


public class DetectionPipelineLoader {
  private static final String PROP_CLASS_NAME = "className";

  public DetectionPipeline from(DataProvider provider, DetectionConfigDTO config, long start, long end) throws Exception {
    String className = config.getProperties().get(PROP_CLASS_NAME).toString();
    Constructor<?> constructor = Class.forName(className).getConstructor(DataProvider.class, DetectionConfigDTO.class, long.class, long.class);
    return (DetectionPipeline) constructor.newInstance(provider, config, start, end);
  }
}
