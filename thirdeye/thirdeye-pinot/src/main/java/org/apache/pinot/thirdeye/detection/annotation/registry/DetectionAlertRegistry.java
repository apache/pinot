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
import io.github.classgraph.AnnotationInfo;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.thirdeye.detection.alert.DetectionAlertFilter;
import org.apache.pinot.thirdeye.detection.alert.scheme.DetectionAlertScheme;
import org.apache.pinot.thirdeye.detection.alert.suppress.DetectionAlertSuppressor;
import org.apache.pinot.thirdeye.detection.annotation.AlertFilter;
import org.apache.pinot.thirdeye.detection.annotation.AlertScheme;
import org.apache.pinot.thirdeye.detection.annotation.AlertSuppressor;
import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
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

  // default package name for reflection to scan
  private static final String DEFAULT_PACKAGE_LIST = "org.apache.pinot.thirdeye";

  // list of package names for reflection to scan
  private static List<String> packageList = new ArrayList<>();

  // singleton instance
  private static DetectionAlertRegistry INSTANCE;

  public static DetectionAlertRegistry getInstance() {
    if (INSTANCE == null) {
      synchronized (DetectionAlertRegistry.class) {
        // another check after acquiring the lock before initializing
        if (INSTANCE == null) {
          packageList.add(DEFAULT_PACKAGE_LIST);
          INSTANCE = new DetectionAlertRegistry();
        }
      }
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
    try (ScanResult scanResult = new ClassGraph()
        .acceptPackages(packageList.toArray(new String[packageList.size()]))
        .enableAnnotationInfo()
        .enableClassInfo().scan()) {
      // register alert filters
      ClassInfoList alertFilterClassClasses = scanResult.getSubclasses(DetectionAlertFilter.class.getName());
      for (ClassInfo classInfo : alertFilterClassClasses) {
        for (AnnotationInfo annotationInfo : classInfo.getAnnotationInfo()) {
          if (annotationInfo.getName().equals(AlertFilter.class.getName())) {
            Annotation annotation = annotationInfo.loadClassAndInstantiate();
            ALERT_FILTER_MAP.put(((AlertFilter) annotation).type(), classInfo.getName());
            LOG.info("Registered Alter Filter {}", classInfo.getName());
          }
        }
      }

      // register alert schemes
      ClassInfoList alertSchemeClasses = scanResult.getSubclasses(DetectionAlertScheme.class.getName());
      for (ClassInfo classInfo : alertSchemeClasses) {
        for (AnnotationInfo annotationInfo : classInfo.getAnnotationInfo()) {
          if (annotationInfo.getName().equals(AlertScheme.class.getName())) {
            Annotation annotation = annotationInfo.loadClassAndInstantiate();
            ALERT_SCHEME_MAP.put(((AlertScheme) annotation).type(), classInfo.getName());
            LOG.info("Registered Alter Scheme {}", classInfo.getName());
          }
        }
      }

      // register alert suppressors
      ClassInfoList alertSuppressorClasses = scanResult.getSubclasses(DetectionAlertSuppressor.class.getName());
      for (ClassInfo classInfo : alertSuppressorClasses) {
        for  (AnnotationInfo annotationInfo : classInfo.getAnnotationInfo()) {
          if (annotationInfo.getName().equals( AlertSuppressor.class.getName())) {
            Annotation annotation = annotationInfo.loadClassAndInstantiate();
            ALERT_SUPPRESSOR_MAP.put(((AlertSuppressor) annotation).type(), classInfo.getName());
            LOG.info("Registered Alter AlertSuppressor {}", classInfo.getName());
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

  public static void setPackageList(List<String> packageList) {
    DetectionAlertRegistry.packageList = packageList;
  }
}
