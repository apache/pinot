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

  private static final Map<String, Map> REGISTRY_MAP = new HashMap<>();
  private static final Map<String, Tune> TRAINING_MODULE_MAP = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(DetectionRegistry.class);
  private static final String KEY_CLASS_NAME = "className";
  private static final String KEY_ANNOTATION = "annotation";

  /**
   * Singleton
   */
  public static DetectionRegistry getInstance() {
    return INSTANCE;
  }

  /**
   * Internal constructor. Read the Components annotation from each stage implementation.
   */
  private DetectionRegistry() {
    try {
      Reflections reflections = new Reflections();
      Set<Class<? extends BaseComponent>> classes = reflections.getSubTypesOf(BaseComponent.class);
      for (Class clazz : classes) {
        String className = clazz.getName();
        if(BaseComponent.class.isAssignableFrom(clazz)){
          for (Annotation annotation : clazz.getAnnotations()) {
            if (annotation instanceof Components) {
              Components componentsAnnotation = (Components) annotation;
              REGISTRY_MAP.put(componentsAnnotation.type(), ImmutableMap.of(KEY_CLASS_NAME, className, KEY_ANNOTATION,
                  componentsAnnotation));
            }
            if (annotation instanceof Tune) {
              Tune trainingAnnotation = (Tune) annotation;
              TRAINING_MODULE_MAP.put(className, trainingAnnotation);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("Build detection registry error", e);
    }
  }

  private static final DetectionRegistry INSTANCE = new DetectionRegistry();

  /**
   * Look up the class name for a given algorithm
   * @param type the type used in the YAML configs
   * @return algorithm class name
   */
  public String lookup(String type) {
    String stageClassName = MapUtils.getString(REGISTRY_MAP.get(type.toUpperCase()), KEY_CLASS_NAME);
    Preconditions.checkArgument(stageClassName != null, type + " not found");
    return stageClassName;
  }

  public boolean isTrainable(String className) {
    return TRAINING_MODULE_MAP.containsKey(className);
  }

  /**
   * Return all stage implementation annotations
   * @return List of detection annotation
   */
  public List<Components> getAllAnnotation() {
    List<Components> annotations = new ArrayList<>();
    for (Map.Entry<String, Map> entry : REGISTRY_MAP.entrySet()){
      Map infoMap = entry.getValue();
      if (infoMap.containsKey(KEY_ANNOTATION)){
        annotations.add((Components) infoMap.get(KEY_ANNOTATION));
      }
    }
    return annotations;
  }
}
