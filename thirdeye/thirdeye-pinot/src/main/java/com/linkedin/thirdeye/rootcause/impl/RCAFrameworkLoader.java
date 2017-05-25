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
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RCAFrameworkLoader creates Pipeline instances based on a YAML config file. It expects the Pipeline
 * implementation to support a constructor that takes (name, inputs, properties map) as arguments.
 * It further augments certain properties with additional information, e.g. the {@code PROP_PATH}
 * property with absolute path information.
 */
public class RCAFrameworkLoader {
  public static final String PROP_PATH = "path";
  public static final String PROP_PATH_POSTFIX = "Path";

  private static final Logger LOG = LoggerFactory.getLogger(RCAFrameworkLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public static List<Pipeline> getPipelinesFromConfig(File rcaConfig, String frameworkName) throws Exception {
    List<Pipeline> pipelines = new ArrayList<>();

    LOG.info("Loading framework '{}' from '{}'", frameworkName, rcaConfig);

    RCAConfiguration rcaConfiguration = OBJECT_MAPPER.readValue(rcaConfig, RCAConfiguration.class);
    Map<String, List<PipelineConfiguration>> rcaPipelinesConfiguration = rcaConfiguration.getFrameworks();
    if (!MapUtils.isEmpty(rcaPipelinesConfiguration)) {
      if(!rcaPipelinesConfiguration.containsKey(frameworkName))
        throw new IllegalArgumentException(String.format("Framework '%s' does not exist", frameworkName));

      for (PipelineConfiguration pipelineConfig : rcaPipelinesConfiguration.get(frameworkName)) {
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
    for(Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().equals(PROP_PATH) ||
          entry.getKey().endsWith(PROP_PATH_POSTFIX)) {
        File path = new File(entry.getValue());
        if (!path.isAbsolute()) {
          properties.put(entry.getKey(), rcaConfig.getParent() + File.separator + path);
        }
      }
    }
    return properties;
  }
}
