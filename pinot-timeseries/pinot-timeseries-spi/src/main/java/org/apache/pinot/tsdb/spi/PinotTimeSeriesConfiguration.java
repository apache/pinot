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
package org.apache.pinot.tsdb.spi;

import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


public class PinotTimeSeriesConfiguration {
  private PinotTimeSeriesConfiguration() {
  }

  public static final String CONFIG_PREFIX = "pinot.timeseries";
  private static final String ENABLE_LANGUAGES_SUFFIX = ".languages";
  private static final String SERIES_BUILDER_FACTORY_SUFFIX = ".series.builder.factory";
  private static final String LOGICAL_PLANNER_CLASS_SUFFIX = ".logical.planner.class";

  /**
   * Config key that controls which time-series languages are enabled in a given Pinot cluster.
   */
  public static String getEnabledLanguagesConfigKey() {
    return CONFIG_PREFIX + ENABLE_LANGUAGES_SUFFIX;
  }

  /**
   * Returns the config key which determines the class name for the {@link TimeSeriesBuilderFactory} to be used for a
   * given language. Each language can have its own {@link TimeSeriesBuilderFactory}, which allows each language to
   * support custom time-series functions.
   */
  public static String getSeriesBuilderFactoryConfigKey(String language) {
    return CONFIG_PREFIX + "." + language + SERIES_BUILDER_FACTORY_SUFFIX;
  }

  /**
   * Returns config key which determines the class name for the {@link TimeSeriesLogicalPlanner} to be used for a given
   * language. Pinot broker will load this logical planner on start-up dynamically. This is called for each language
   * configured via {@link #getEnabledLanguagesConfigKey()}.
   */
  public static String getLogicalPlannerConfigKey(String language) {
    return CONFIG_PREFIX + "." + language + LOGICAL_PLANNER_CLASS_SUFFIX;
  }
}
