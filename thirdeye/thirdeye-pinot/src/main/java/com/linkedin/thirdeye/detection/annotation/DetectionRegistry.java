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

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.ClassPath;
import com.linkedin.thirdeye.detection.algorithm.stage.AnomalyDetectionStage;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The detection registry.
 */
public class DetectionRegistry {

  private static final Map<String, Map> REGISTRY_MAP = new HashMap<>();
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
   * Internal constructor. Read the Detection annotation from each stage implementation.
   */
  private DetectionRegistry() {
    try {
      Set<ClassPath.ClassInfo> classInfos = ClassPath.from(Thread.currentThread().getContextClassLoader())
          .getTopLevelClasses(AnomalyDetectionStage.class.getPackage().getName());
      for (ClassPath.ClassInfo classInfo : classInfos) {
        Class clazz = Class.forName(classInfo.getName());
        for (Annotation annotation : clazz.getAnnotations()) {
          if (annotation instanceof Detection) {
            Detection detectionAnnotation = (Detection) annotation;
            REGISTRY_MAP.put(detectionAnnotation.type(), ImmutableMap.of(KEY_CLASS_NAME, classInfo.getName(), KEY_ANNOTATION, detectionAnnotation));
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
    return MapUtils.getString(REGISTRY_MAP.get(type.toUpperCase()), KEY_CLASS_NAME);
  }

  /**
   * Return all stage implementation annotations
   * @return List of detection annotation
   */
  public List<Detection> getAllAnnotation() {
    List<Detection> annotations = new ArrayList<>();
    for (Map.Entry<String, Map> entry : REGISTRY_MAP.entrySet()){
      Map infoMap = entry.getValue();
      if (infoMap.containsKey(KEY_ANNOTATION)){
        annotations.add((Detection) infoMap.get(KEY_ANNOTATION));
      }
    }
    return annotations;
  }
}
