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
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import org.apache.pinot.thirdeye.detection.alert.suppress.DetectionAlertSuppressor;
import org.apache.pinot.thirdeye.detection.annotation.AlertFilter;
import org.apache.pinot.thirdeye.detection.annotation.AlertScheme;
import org.apache.pinot.thirdeye.detection.annotation.AlertSuppressor;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The detection alert registry.
 */
public class DetectionAlertRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(DetectionAlertRegistry.class);

  // Alert Scheme type to Alert Scheme class name
  private static final Map<String, String> ALERT_SCHEME_MAP = new HashMap<>();

  // Alert Suppressor type to Alert Suppressor class name
  private static final Map<String, String> ALERT_SUPPRESSOR_MAP = new HashMap<>();

  // Alert Filter Type Map
  private static final Map<String, String> ALERT_FILTER_MAP = new HashMap<>();

  private static DetectionAlertRegistry INSTANCE;

  public static DetectionAlertRegistry getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new DetectionAlertRegistry();
    }

    return INSTANCE;
  }

  private DetectionAlertRegistry () {
    init();
  }

  /**
   * Read all the alert schemes and suppressors and initialize the registry.
   */
  private static void init() {
    try {
      Reflections reflections = new Reflections("org.apache.pinot.thirdeye.detection");

      // register alert filters
      Set<Class<? extends DetectionAlertFilter>> alertFilterClasses =
          reflections.getSubTypesOf(DetectionAlertFilter.class);
      for (Class clazz : alertFilterClasses) {
        for (Annotation annotation : clazz.getAnnotations()) {
          if (annotation instanceof AlertFilter) {
            ALERT_FILTER_MAP.put(((AlertFilter) annotation).type(), clazz.getName());
          }
        }
      }

      // register alert schemes
      Set<Class<? extends DetectionAlertScheme>> alertSchemeClasses =
          reflections.getSubTypesOf(DetectionAlertScheme.class);
      for (Class clazz : alertSchemeClasses) {
        for (Annotation annotation : clazz.getAnnotations()) {
          if (annotation instanceof AlertScheme) {
            ALERT_SCHEME_MAP.put(((AlertScheme) annotation).type(), clazz.getName());
          }
        }
      }

      // register alert suppressors
      Set<Class<? extends DetectionAlertSuppressor>> alertSuppressorClasses =
          reflections.getSubTypesOf(DetectionAlertSuppressor.class);
      for (Class clazz : alertSuppressorClasses) {
        for (Annotation annotation : clazz.getAnnotations()) {
          if (annotation instanceof AlertSuppressor) {
            ALERT_SUPPRESSOR_MAP.put(((AlertSuppressor) annotation).type(), clazz.getName());
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("initialize detection registry error", e);
    }
  }

  public void registerAlertFilter(String type, String className) {
    ALERT_FILTER_MAP.put(type, className);
  }

  public void registerAlertScheme(String type, String className) {
    ALERT_SCHEME_MAP.put(type, className);
  }

  public void registerAlertSuppressor(String type, String className) {
    ALERT_SUPPRESSOR_MAP.put(type, className);
  }

  /**
   * Look up the class name for a given alert filter
   * @param type the type used in the YAML configs
   */
  public String lookupAlertFilters(String type) {
    Preconditions.checkArgument(ALERT_FILTER_MAP.containsKey(type.toUpperCase()), type + " not found in registry");
    return ALERT_FILTER_MAP.get(type.toUpperCase());
  }

  /**
   * Look up the {@link #ALERT_SCHEME_MAP} for the Alert scheme class name from the type
   */
  public String lookupAlertSchemes(String schemeType) {
    Preconditions.checkArgument(ALERT_SCHEME_MAP.containsKey(schemeType.toUpperCase()), schemeType + " not found in registry");
    return ALERT_SCHEME_MAP.get(schemeType.toUpperCase());
  }

  /**
   * Look up the {@link #ALERT_SUPPRESSOR_MAP} for the Alert suppressor class name from the type
   */
  public String lookupAlertSuppressors(String suppressorType) {
    Preconditions.checkArgument(ALERT_SUPPRESSOR_MAP.containsKey(suppressorType.toUpperCase()), suppressorType + " not found in registry");
    return ALERT_SUPPRESSOR_MAP.get(suppressorType.toUpperCase());
  }
}
