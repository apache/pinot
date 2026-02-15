/**
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
package org.apache.pinot.broker.routing.tablesampler;

import com.google.common.annotations.VisibleForTesting;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.annotations.tablesampler.TableSamplerProvider;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableSamplerFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableSamplerFactory.class);
  private static final String ANNOTATION_PACKAGES_KEY = "annotation.packages";
  // Keep this list in sync with built-in TableSampler locations; additional packages can be configured via broker
  // config, but defaults should always include built-ins.
  private static final List<String> DEFAULT_ANNOTATION_PACKAGES =
      List.of("org.apache.pinot.broker.routing.tablesampler");
  private static final Map<String, String> REGISTERED_TABLE_SAMPLER_CLASS_MAP = new ConcurrentHashMap<>();

  private TableSamplerFactory() {
  }

  public static void init(PinotConfiguration tableSamplerConfig) {
    if (tableSamplerConfig == null) {
      return;
    }
    registerAnnotatedTableSamplers(tableSamplerConfig);
  }

  public static void register(String alias, String className) {
    if (StringUtils.isBlank(alias)) {
      LOGGER.warn("Skipping table sampler registration because alias is blank");
      return;
    }
    if (StringUtils.isBlank(className)) {
      LOGGER.warn("Skipping table sampler registration for alias '{}' because class name is blank", alias);
      return;
    }
    String normalizedAlias = normalizeType(alias);
    String trimmedClassName = className.trim();
    String previousClassName = REGISTERED_TABLE_SAMPLER_CLASS_MAP.put(normalizedAlias, trimmedClassName);
    if (previousClassName == null) {
      LOGGER.info("Registered table sampler alias '{}' -> '{}'", alias, trimmedClassName);
    } else if (!previousClassName.equals(trimmedClassName)) {
      LOGGER.warn("Overriding table sampler alias '{}' from '{}' to '{}'", alias, previousClassName, trimmedClassName);
    }
  }

  public static TableSampler create(String type) {
    String resolvedClassName = resolveClassName(type);
    String classNameToLoad = resolvedClassName != null ? resolvedClassName : type;
    try {
      return PluginManager.get().createInstance(classNameToLoad);
    } catch (Exception e) {
      String errorMessage = resolvedClassName != null
          ? String.format("Failed to create TableSampler for alias '%s' mapped to class '%s'", type,
              resolvedClassName)
          : "Failed to create TableSampler for type: " + type;
      throw new RuntimeException(errorMessage, e);
    }
  }

  @VisibleForTesting
  static void clearRegistry() {
    REGISTERED_TABLE_SAMPLER_CLASS_MAP.clear();
  }

  private static void registerAnnotatedTableSamplers(PinotConfiguration tableSamplerConfig) {
    List<String> configuredPackages = getConfiguredAnnotationPackages(tableSamplerConfig);
    LinkedHashSet<String> combinedPackages = new LinkedHashSet<>(DEFAULT_ANNOTATION_PACKAGES);
    for (String packageName : configuredPackages) {
      if (StringUtils.isNotBlank(packageName)) {
        combinedPackages.add(packageName.trim());
      }
    }
    List<String> sanitizedPackages = new ArrayList<>(combinedPackages);
    if (sanitizedPackages.isEmpty()) {
      LOGGER.info("No table sampler annotation packages configured");
      return;
    }
    Set<Class<?>> samplerClasses =
        PinotReflectionUtils.getClassesThroughReflection(sanitizedPackages, ".*", TableSamplerProvider.class);
    for (Class<?> samplerClass : samplerClasses) {
      TableSamplerProvider annotation = samplerClass.getAnnotation(TableSamplerProvider.class);
      if (annotation == null || !annotation.enabled()) {
        continue;
      }
      if (!TableSampler.class.isAssignableFrom(samplerClass)) {
        LOGGER.warn("Skipping table sampler class '{}' because it does not implement TableSampler",
            samplerClass.getName());
        continue;
      }
      if (!Modifier.isPublic(samplerClass.getModifiers()) || Modifier.isAbstract(samplerClass.getModifiers())) {
        LOGGER.warn("Skipping table sampler class '{}' because it is not a public concrete class",
            samplerClass.getName());
        continue;
      }
      String alias = annotation.name();
      if (StringUtils.isBlank(alias)) {
        LOGGER.warn("Skipping table sampler class '{}' because annotation name is blank", samplerClass.getName());
        continue;
      }
      register(alias, samplerClass.getName());
    }
  }

  private static List<String> getConfiguredAnnotationPackages(PinotConfiguration tableSamplerConfig) {
    String configuredPackages = tableSamplerConfig.getProperty(ANNOTATION_PACKAGES_KEY, "");
    if (StringUtils.isBlank(configuredPackages)) {
      return List.of();
    }
    List<String> packageList = new ArrayList<>();
    for (String packageName : configuredPackages.split(",")) {
      if (StringUtils.isNotBlank(packageName)) {
        packageList.add(packageName.trim());
      }
    }
    return packageList;
  }

  private static String resolveClassName(String type) {
    if (StringUtils.isBlank(type)) {
      return null;
    }
    return REGISTERED_TABLE_SAMPLER_CLASS_MAP.get(normalizeType(type));
  }

  /**
   * Normalizes a table sampler alias for registry lookup.
   *
   * <p>Both registration and lookup go through this method, so aliases are matched case-insensitively after trimming.
   */
  private static String normalizeType(String type) {
    return type.trim().toLowerCase(Locale.ROOT);
  }
}
