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
package org.apache.pinot.tsdb.spi.series;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tsdb.spi.PinotTimeSeriesConfiguration;


/**
 * Loads all series builder providers for all configured time-series query languages.
 */
public class TimeSeriesBuilderFactoryProvider {
  private static final Map<String, TimeSeriesBuilderFactory> FACTORY_MAP = new HashMap<>();

  private TimeSeriesBuilderFactoryProvider() {
  }

  public static void init(PinotConfiguration pinotConfiguration) {
    String[] languages = pinotConfiguration.getProperty(PinotTimeSeriesConfiguration.getEnabledLanguagesConfigKey(), "")
        .split(",");
    for (String language : languages) {
      String seriesBuilderClass = pinotConfiguration
          .getProperty(PinotTimeSeriesConfiguration.getSeriesBuilderFactoryConfigKey(language));
      try {
        Class<?> klass = TimeSeriesBuilderFactoryProvider.class.getClassLoader().loadClass(seriesBuilderClass);
        Object untypedSeriesBuilderFactory = klass.getConstructor().newInstance();
        if (!(untypedSeriesBuilderFactory instanceof TimeSeriesBuilderFactory)) {
          throw new RuntimeException("Series builder factory class " + seriesBuilderClass
              + " does not implement SeriesBuilderFactory");
        }
        TimeSeriesBuilderFactory seriesBuilderFactory = (TimeSeriesBuilderFactory) untypedSeriesBuilderFactory;
        seriesBuilderFactory.init(pinotConfiguration.subset(
            PinotTimeSeriesConfiguration.CONFIG_PREFIX + "." + language));
        FACTORY_MAP.putIfAbsent(language, seriesBuilderFactory);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static TimeSeriesBuilderFactory getSeriesBuilderFactory(String engine) {
    return Objects.requireNonNull(FACTORY_MAP.get(engine),
        "No series builder factory found for engine: " + engine);
  }

  @VisibleForTesting
  public static void registerSeriesBuilderFactory(String engine, TimeSeriesBuilderFactory factory) {
    if (FACTORY_MAP.put(engine, factory) != null) {
      throw new IllegalStateException(String.format("Entry for engine=%s already exists in series builder", engine));
    }
  }
}
