package com.linkedin.thirdeye.rootcause.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.rootcause.Pipeline;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * PipelineLoader creates Pipeline instances based on a YAML config file. It expects the Pipeline
 * implementation to support a constructor that takes (name, inputs, properties map) as arguments.
 * It further augments certain properties with additional information, e.g. the {@code PROP_PATH}
 * property with absolute path information.
 */
public class PipelineLoader {
  public static final String PROP_PATH = "path";

  private static final Logger LOG = LoggerFactory.getLogger(PipelineLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public static List<Pipeline> getPipelinesFromConfig(File rcaConfig) throws Exception {
    List<Pipeline> pipelines = new ArrayList<>();

    RCAConfiguration rcaConfiguration = OBJECT_MAPPER.readValue(rcaConfig, RCAConfiguration.class);
    List<PipelineConfiguration> rcaPipelinesConfiguration = rcaConfiguration.getRcaPipelinesConfiguration();
    if (CollectionUtils.isNotEmpty(rcaPipelinesConfiguration)) {
      for (PipelineConfiguration pipelineConfig : rcaPipelinesConfiguration) {
        String outputName = pipelineConfig.getOutputName();
        Set<String> inputNames = new HashSet<>(pipelineConfig.getInputNames());
        String className = pipelineConfig.getClassName();
        Map<String, String> properties = pipelineConfig.getProperties();
        if(properties == null)
          properties = new HashMap<>();

        properties = augmentPathProperty(properties, rcaConfig);

        LOG.info("Creating pipeline '{}' [{}] with inputs '{}'", outputName, className, inputNames);
        Constructor<?> constructor = Class.forName(className).getConstructor(String.class, Set.class, Map.class);
        Pipeline pipeline = (Pipeline) constructor.newInstance(outputName, inputNames, properties);

        pipelines.add(pipeline);
      }
    }

    return pipelines;
  }

  static Map<String, String> augmentPathProperty(Map<String, String> properties, File rcaConfig) {
    if(properties.containsKey(PROP_PATH)) {
      File path = new File(properties.get(PROP_PATH));
      if(!path.isAbsolute())
        properties.put(PROP_PATH, rcaConfig.getParent() + File.separator + path);
    }
    return properties;
  }

}
