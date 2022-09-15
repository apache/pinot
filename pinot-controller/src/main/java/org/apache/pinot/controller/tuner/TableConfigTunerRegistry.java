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
package org.apache.pinot.controller.tuner;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to dynamically register all annotated {@link Tuner} classes.
 */
public class TableConfigTunerRegistry {
  private TableConfigTunerRegistry() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigTunerRegistry.class);
  private static final Map<String, TableConfigTuner> CONFIG_TUNER_MAP = new ConcurrentHashMap<>();
  private static boolean _init = false;

  /**
   * Init method that initializes the _configTunerMap with all available tuners.
   * <ul>
   *   <li>Scans all packages specified, for class paths that have 'tuner' in path.</li>
   *   <li>Looks for {@link Tuner} annotation for classes and adds them to the map. </li>
   *   <li>Also, asserts that init was not already called before.</li>
   * </ul>
   * @param packages Packages to scan.
   */
  public static void init(List<String> packages) {
    if (_init) {
      LOGGER.info("TableConfigTunerRegistry already initialized, skipping.");
      return;
    }
    long startTime = System.currentTimeMillis();

    Set<Class<?>> tunerClasses =
        PinotReflectionUtils.getClassesThroughReflection(packages, ".*\\.tuner\\..*", Tuner.class);
    for (Class<?> tunerClass : tunerClasses) {
      Tuner tunerAnnotation = tunerClass.getAnnotation(Tuner.class);
      if (tunerAnnotation.enabled()) {
        if (tunerAnnotation.name().isEmpty()) {
          LOGGER.error("Cannot register an unnamed config tuner for annotation {} ", tunerAnnotation);
        } else {
          String tunerName = tunerAnnotation.name();
          TableConfigTuner tuner;
          try {
            tuner = (TableConfigTuner) tunerClass.newInstance();
            CONFIG_TUNER_MAP.putIfAbsent(tunerName, tuner);
          } catch (Exception e) {
            LOGGER.error(String.format("Unable to register tuner %s . Cannot instantiate.", tunerName), e);
          }
        }
      }
    }

    _init = true;
    LOGGER.info("Initialized TableConfigTunerRegistry with {} tuners: {} in {} ms", CONFIG_TUNER_MAP.size(),
        CONFIG_TUNER_MAP.keySet(), (System.currentTimeMillis() - startTime));
  }

  public static TableConfigTuner getTuner(String name) {
    Preconditions.checkState(_init, "TableConfigTunerRegistry not yet initialized.");
    return CONFIG_TUNER_MAP.get(name);
  }
}
