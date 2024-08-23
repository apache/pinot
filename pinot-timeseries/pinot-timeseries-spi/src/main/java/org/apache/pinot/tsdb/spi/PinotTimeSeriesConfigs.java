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

public class PinotTimeSeriesConfigs {
  private PinotTimeSeriesConfigs() {
  }

  public static final String CONFIG_PREFIX = "pinot.time.series";
  private static final String ENABLE_LANGUAGES_SUFFIX = ".languages";
  private static final String LOGICAL_PLANNER_CLASS_SUFFIX = ".logical.planner.class";

  public static String getEnabledLanguagesSuffix() {
    return CONFIG_PREFIX + ENABLE_LANGUAGES_SUFFIX;
  }

  public static String getSeriesBuilderClass(String engine) {
    return CONFIG_PREFIX + "." + engine + ".series.builder.class";
  }

  public static String getLogicalPlannerClassConfig(String engine) {
    return CONFIG_PREFIX + "." + engine + LOGICAL_PLANNER_CLASS_SUFFIX;
  }
}
