package com.linkedin.thirdeye.rootcause.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.RCAFramework;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
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

  public static Map<String, RCAFramework> getFrameworksFromConfig(File rcaFile, ExecutorService executor) throws Exception {
    Map<String, RCAFramework> frameworks = new HashMap<>();

    LOG.info("Loading all frameworks from '{}'", rcaFile);
    RCAConfiguration rcaConfiguration = OBJECT_MAPPER.readValue(rcaFile, RCAConfiguration.class);

    for(String frameworkName : rcaConfiguration.getFrameworks().keySet()) {
      List<Pipeline> pipelines = getPipelinesFromConfig(rcaFile, frameworkName);
      frameworks.put(frameworkName, new RCAFramework(pipelines, executor));
    }

    return frameworks;
  }

  public static List<Pipeline> getPipelinesFromConfig(File rcaFile, String frameworkName) throws Exception {
    LOG.info("Loading framework '{}' from '{}'", frameworkName, rcaFile);
    RCAConfiguration rcaConfiguration = OBJECT_MAPPER.readValue(rcaFile, RCAConfiguration.class);

    return getPipelines(rcaConfiguration, rcaFile, frameworkName);
  }

  static List<Pipeline> getPipelines(RCAConfiguration config, File configPath, String frameworkName) throws Exception {
    List<Pipeline> pipelines = new ArrayList<>();
    Map<String, List<PipelineConfiguration>> rcaPipelinesConfiguration = config.getFrameworks();
    if (!MapUtils.isEmpty(rcaPipelinesConfiguration)) {
      if(!rcaPipelinesConfiguration.containsKey(frameworkName))
        throw new IllegalArgumentException(String.format("Framework '%s' does not exist", frameworkName));

      for (PipelineConfiguration pipelineConfig : rcaPipelinesConfiguration.get(frameworkName)) {
        String outputName = pipelineConfig.getOutputName();
        Set<String> inputNames = new HashSet<>(pipelineConfig.getInputNames());
        String className = pipelineConfig.getClassName();
        Map<String, Object> properties = pipelineConfig.getProperties();
        if(properties == null)
          properties = new HashMap<>();

        properties = augmentPathProperty(properties, configPath);

        LOG.info("Creating pipeline '{}' [{}] with inputs '{}'", outputName, className, inputNames);
        Constructor<?> constructor = Class.forName(className).getConstructor(String.class, Set.class, Map.class);
        Pipeline pipeline = (Pipeline) constructor.newInstance(outputName, inputNames, properties);

        pipelines.add(pipeline);
      }
    }

    return pipelines;
  }

  static Map<String, Object> augmentPathProperty(Map<String, Object> properties, File rcaConfig) {
    for(Map.Entry<String, Object> entry : properties.entrySet()) {
      if ((entry.getKey().equals(PROP_PATH) ||
          entry.getKey().endsWith(PROP_PATH_POSTFIX)) &&
          entry.getValue() instanceof String) {
        File path = new File(entry.getValue().toString());
        if (!path.isAbsolute()) {
          properties.put(entry.getKey(), rcaConfig.getParent() + File.separator + path);
        }
      }
    }
    return properties;
  }
}
