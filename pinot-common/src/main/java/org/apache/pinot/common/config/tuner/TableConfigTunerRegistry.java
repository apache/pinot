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
package org.apache.pinot.common.config.tuner;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.table.tuner.TableConfigTuner;
import org.apache.pinot.spi.config.table.tuner.Tuner;
import org.reflections.Reflections;
import org.reflections.scanners.ResourcesScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to dynamically register all annotated {@link Tuner} methods
 */
public class TableConfigTunerRegistry {
  private TableConfigTunerRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigTunerRegistry.class);
  private static final Map<String, TableConfigTuner> _configTunerMap = new HashMap<>();

  static {
    Reflections reflections = new Reflections(
        new ConfigurationBuilder().setUrls(ClasspathHelper.forPackage("org.apache.pinot"))
            .filterInputsBy(new FilterBuilder.Include(".*\\.tuner\\..*"))
            .setScanners(new ResourcesScanner(), new TypeAnnotationsScanner(), new SubTypesScanner()));
    Set<Class<?>> classes = reflections.getTypesAnnotatedWith(Tuner.class);
    classes.forEach(tunerClass -> {
      Tuner tunerAnnotation = tunerClass.getAnnotation(Tuner.class);
      if (tunerAnnotation.enabled()) {
        if (tunerAnnotation.name().isEmpty()) {
          LOGGER.error("Cannot register an unnamed config tuner for annotation {} ", tunerAnnotation.toString());
        } else {
          String tunerName = tunerAnnotation.name();
          TableConfigTuner tuner;
          try {
            tuner = (TableConfigTuner) tunerClass.newInstance();
            _configTunerMap.putIfAbsent(tunerName, tuner);
          } catch (Exception e) {
            LOGGER.error(String.format("Unable to register tuner %s . Cannot instantiate.", tunerName), e);
          }
        }
      }
    });
    LOGGER.info("Initialized TableConfigTunerRegistry with {} tuners: {}", _configTunerMap.size(),
        _configTunerMap.keySet());
  }

  public static TableConfigTuner getTuner(String name) {
    return _configTunerMap.get(name);
  }
}
