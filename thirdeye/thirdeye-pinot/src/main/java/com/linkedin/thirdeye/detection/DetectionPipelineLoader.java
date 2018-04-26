package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.datalayer.dto.DetectionConfigDTO;
import java.lang.reflect.Constructor;


public class DetectionPipelineLoader {
  public DetectionPipeline from(DataProvider provider, DetectionConfigDTO config, long start, long end) throws Exception {
    Constructor<?> constructor = Class.forName(config.getClassName()).getConstructor(DataProvider.class, DetectionConfigDTO.class, long.class, long.class);
    return (DetectionPipeline) constructor.newInstance(provider, config, start, end);
  }
}
