package com.linkedin.thirdeye.rootcause.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.RCAConfiguration;
import com.linkedin.thirdeye.rootcause.RCAPipelineConfiguration;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineLoader {

  private static final Logger LOG = LoggerFactory.getLogger(PipelineLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public static List<Pipeline> getPipelinesFromConfig(File rcaConfig) {
    List<Pipeline> pipelines = new ArrayList<>();

    try {
      RCAConfiguration rcaConfiguration = OBJECT_MAPPER.readValue(rcaConfig, RCAConfiguration.class);
      List<RCAPipelineConfiguration> rcaPipelinesConfiguration = rcaConfiguration.getRcaPipelinesConfiguration();
      if (CollectionUtils.isNotEmpty(rcaPipelinesConfiguration)) {
        for (RCAPipelineConfiguration pipelineConfig : rcaPipelinesConfiguration) {
          String name = pipelineConfig.getName();
          String className = pipelineConfig.getClassName();
          Map<String, String> properties = pipelineConfig.getProperties();

          Constructor<?> constructor = Class.forName(className).getConstructor(Map.class);
          Pipeline pipeline = (Pipeline) constructor.newInstance(properties);
          pipelines.add(pipeline);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in loading rca configs", e);
    }
    return pipelines;
  }

}
