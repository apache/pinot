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
package org.apache.pinot.common.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;


/**
 * Logger utils for process level logger management.
 */
public class LoggerUtils {
  private static final String ROOT = "root";
  private static final String NAME = "name";
  private static final String LEVEL = "level";
  private static final String FILTER = "filter";

  private LoggerUtils() {
  }

  /**
   * Set logger level at runtime.
   * @param loggerName
   * @param logLevel
   * @return logger info
   */
  public static Map<String, String> setLoggerLevel(String loggerName, String logLevel) {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    if (!getAllLoggers().contains(loggerName)) {
      throw new RuntimeException("Logger - " + loggerName + " not found");
    }
    LoggerConfig loggerConfig = getLoggerConfig(config, loggerName);
    try {
      loggerConfig.setLevel(Level.valueOf(logLevel));
    } catch (Exception e) {
      throw new RuntimeException("Unrecognized logger level - " + logLevel, e);
    }
    // This causes all Loggers to re-fetch information from their LoggerConfig.
    context.updateLoggers();
    return getLoggerResponse(loggerConfig);
  }

  /**
   * Fetch logger info of name, level and filter.
   * @param loggerName
   * @return logger info
   */
  @Nullable
  public static Map<String, String> getLoggerInfo(String loggerName) {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    if (!getAllLoggers().contains(loggerName)) {
      return null;
    }
    LoggerConfig loggerConfig = getLoggerConfig(config, loggerName);
    return getLoggerResponse(loggerConfig);
  }

  /**
   * @return a list of all the logger names
   */
  public static List<String> getAllLoggers() {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    return config.getLoggers().values().stream().map(LoggerConfig::toString).collect(Collectors.toList());
  }

  private static LoggerConfig getLoggerConfig(Configuration config, String loggerName) {
    return loggerName.equalsIgnoreCase(ROOT) ? config.getRootLogger() : config.getLoggerConfig(loggerName);
  }

  private static Map<String, String> getLoggerResponse(LoggerConfig loggerConfig) {
    Map<String, String> result = new HashMap<>();
    result.put(NAME, loggerConfig.toString());
    result.put(LEVEL, loggerConfig.getLevel().name());
    Filter filter = loggerConfig.getFilter();
    result.put(FILTER, filter == null ? null : filter.toString());
    return result;
  }
}
