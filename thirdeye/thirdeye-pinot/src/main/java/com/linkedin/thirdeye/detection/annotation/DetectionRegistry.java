/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.annotation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.linkedin.thirdeye.detection.spi.components.BaseComponent;
import com.linkedin.thirdeye.detection.yaml.YamlDetectionConfigTranslator;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The detection registry.
 */
public class DetectionRegistry {
  // component type to component class name and annotation
  private static final Map<String, Map> REGISTRY_MAP = new HashMap<>();
  // component class name to tuner annotation
  private static final Map<String, Tune> TUNE_MAP = new HashMap<>();
  // yaml pipeline type to yaml converter class name
  private static final Map<String, String> YAML_MAP = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(DetectionRegistry.class);
  private static final String KEY_CLASS_NAME = "className";
  private static final String KEY_ANNOTATION = "annotation";

  /**
   * Singleton
   */
  public static DetectionRegistry getInstance() {
    return INSTANCE;
  }

  public static void registerComponent(String className, String type) {
    REGISTRY_MAP.put(type, ImmutableMap.of(KEY_CLASS_NAME, className));
  }

  /**
   * Read all the components, tune, and yaml annotations and initialize the registry.
   */
  public static void init() {
    try {
      Reflections reflections = new Reflections();
      // register components
      Set<Class<? extends BaseComponent>> classes = reflections.getSubTypesOf(BaseComponent.class);
      for (Class clazz : classes) {
        String className = clazz.getName();
        for (Annotation annotation : clazz.getAnnotations()) {
          if (annotation instanceof Components) {
            Components componentsAnnotation = (Components) annotation;
            REGISTRY_MAP.put(componentsAnnotation.type(),
                ImmutableMap.of(KEY_CLASS_NAME, className, KEY_ANNOTATION, componentsAnnotation));
          }
          if (annotation instanceof Tune) {
            Tune trainingAnnotation = (Tune) annotation;
            TUNE_MAP.put(className, trainingAnnotation);
          }
        }
      }
      // register yaml translators
      Set<Class<? extends YamlDetectionConfigTranslator>> yamlConverterClasses =
          reflections.getSubTypesOf(YamlDetectionConfigTranslator.class);
      for (Class clazz : yamlConverterClasses) {
        for (Annotation annotation : clazz.getAnnotations()) {
          if (annotation instanceof Yaml) {
            YAML_MAP.put(((Yaml) annotation).pipelineType(), clazz.getName());
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("initialize detection registry error", e);
    }
  }

  private static final DetectionRegistry INSTANCE = new DetectionRegistry();

  /**
   * Look up the class name for a given component
   * @param type the type used in the YAML configs
   * @return component class name
   */
  public String lookup(String type) {
    Preconditions.checkArgument(REGISTRY_MAP.containsKey(type.toUpperCase()), type + " not found in registry");
    return MapUtils.getString(REGISTRY_MAP.get(type.toUpperCase()), KEY_CLASS_NAME);
  }

  /**
   * Look up the tunable class name for a component class name
   * @return tunable class name
   */
  public String lookupTunable(String className) {
    Preconditions.checkArgument(TUNE_MAP.containsKey(className), className + " not found in registry");
    return TUNE_MAP.get(className).tunable();
  }

  /**
   * Look up the yaml converter class name for a pipeline type
   * @return yaml converter class name
   */
  public String lookupYamlConverter(String pipelineType) {
    Preconditions.checkArgument(YAML_MAP.containsKey(pipelineType), pipelineType + " not found in registry");
    return YAML_MAP.get(pipelineType);
  }

  public boolean isTunable(String className) {
    return TUNE_MAP.containsKey(className);
  }

  /**
   * Return all component implementation annotations
   * @return List of component annotation
   */
  public List<Components> getAllAnnotation() {
    List<Components> annotations = new ArrayList<>();
    for (Map.Entry<String, Map> entry : REGISTRY_MAP.entrySet()) {
      Map infoMap = entry.getValue();
      if (infoMap.containsKey(KEY_ANNOTATION)) {
        annotations.add((Components) infoMap.get(KEY_ANNOTATION));
      }
    }
    return annotations;
  }
}
