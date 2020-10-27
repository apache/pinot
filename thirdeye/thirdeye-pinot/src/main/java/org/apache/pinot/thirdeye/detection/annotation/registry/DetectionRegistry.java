/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection.annotation.registry;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import org.apache.pinot.thirdeye.detection.annotation.Components;
import org.apache.pinot.thirdeye.detection.annotation.Tune;
import org.apache.pinot.thirdeye.detection.spi.components.BaseComponent;
import org.apache.pinot.thirdeye.detection.spi.components.BaselineProvider;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.MapUtils;
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
  private static final String KEY_IS_BASELINE_PROVIDER = "isBaselineProvider";

  private static DetectionRegistry INSTANCE;

  public static DetectionRegistry getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new DetectionRegistry();
    }

    return INSTANCE;
  }

  private DetectionRegistry () {
    init();
  }

  /**
   * Read all the components, tune, and yaml annotations and initialize the registry.
   */
  private static void init() {
    try (ScanResult scanResult = new ClassGraph().enableAnnotationInfo().enableClassInfo().scan()) {
      // register components
      ClassInfoList classes = scanResult.getClassesImplementing(BaseComponent.class.getName());
      for (ClassInfo classInfo : classes) {
        String className = classInfo.getName();
        for (AnnotationInfo annotationInfo : classInfo.getAnnotationInfo()) {
          Annotation annotation = annotationInfo.loadClassAndInstantiate();
          if (annotation instanceof Components) {
            Components componentsAnnotation = (Components) annotation;
            REGISTRY_MAP.put(componentsAnnotation.type(),
                ImmutableMap.of(KEY_CLASS_NAME, className, KEY_ANNOTATION, componentsAnnotation,
                    KEY_IS_BASELINE_PROVIDER, isBaselineProvider(Class.forName(className))));
            LOG.info("Registered component {} - {}", componentsAnnotation.type(), className);
          }
          if (annotation instanceof Tune) {
            Tune tunableAnnotation = (Tune) annotation;
            TUNE_MAP.put(className, tunableAnnotation);
            LOG.info("Registered tuner {}", className);
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("initialize detection registry error", e);
    }
  }

  public static void registerComponent(String className, String type) {
    try {
      Class<? extends BaseComponent> clazz = (Class<? extends BaseComponent>) Class.forName(className);
      REGISTRY_MAP.put(type, ImmutableMap.of(KEY_CLASS_NAME, className, KEY_IS_BASELINE_PROVIDER, isBaselineProvider(clazz)));
      LOG.info("Registered component {} {}", type, className);
    } catch (Exception e) {
      LOG.warn("Encountered exception when registering component {}", className, e);
    }
  }

  public static void registerTunableComponent(String className, String tunable, String type) {
    try {
      Class<? extends BaseComponent> clazz = (Class<? extends BaseComponent>) Class.forName(className);
      REGISTRY_MAP.put(type, ImmutableMap.of(KEY_CLASS_NAME, className, KEY_IS_BASELINE_PROVIDER, isBaselineProvider(clazz)));
      Tune tune = new Tune(){
        @Override
        public String tunable() {
          return tunable;
        }

        @Override
        public Class<? extends Annotation> annotationType() {
          return Tune.class;
        }
      };
      TUNE_MAP.put(className, tune);
      LOG.info("Registered tunable component {} {}", type, className);
    } catch (Exception e) {
      LOG.warn("Encountered exception when registering component {}", className, e);
    }
  }

  public static void registerYamlConvertor(String className, String type) {
    YAML_MAP.put(type, className);
    LOG.info("Registered yaml convertor {} {}", type, className);
  }

  /**
   * Look up the class name for a given component
   * @param type the type used in the YAML configs
   * @return component class name
   */
  public String lookup(String type) {
    Preconditions.checkArgument(REGISTRY_MAP.containsKey(type), type + " not found in registry");
    return MapUtils.getString(REGISTRY_MAP.get(type), KEY_CLASS_NAME);
  }

  /**
   * Look up the tunable class name for a component class name
   * @return tunable class name
   */
  public String lookupTunable(String type) {
    Preconditions.checkArgument(TUNE_MAP.containsKey(type), type + " not found in registry");
    return this.lookup(TUNE_MAP.get(type).tunable());
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

  public boolean isBaselineProvider(String type) {
    Preconditions.checkArgument(REGISTRY_MAP.containsKey(type), type + " not found in registry");
    return MapUtils.getBooleanValue(REGISTRY_MAP.get(type), KEY_IS_BASELINE_PROVIDER);
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

  public String printAnnotations() {
    return String.join(", ", YAML_MAP.keySet());
  }

  private static boolean isBaselineProvider(Class<?> clazz) {
    return BaselineProvider.class.isAssignableFrom(clazz);
  }
}
