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
import org.apache.logging.log4j.spi.AbstractLogger;


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
   * @param loggerName name of the logger whose level is to be changed
   * @param logLevel the new log level
   * @return logger info
   */
  public static Map<String, String> setLoggerLevel(String loggerName, String logLevel) {
    Level level;
    try {
      level = Level.valueOf(logLevel);
    } catch (Exception e) {
      throw new RuntimeException("Unrecognized logger level - " + logLevel, e);
    }
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig loggerConfig;
    if (getAllConfiguredLoggers().contains(loggerName)) {
      loggerConfig = getLoggerConfig(config, loggerName);
      loggerConfig.setLevel(level);
    } else {
      // Check if the loggerName exists by comparing it to all known loggers in the context
      if (getAllLoggers().stream().noneMatch(logger -> {
        if (!logger.startsWith(loggerName)) {
          return false;
        }
        if (logger.equals(loggerName)) {
          return true;
        }
        // Check if loggerName is a valid parent / descendant logger for any known logger
        return logger.substring(loggerName.length()).startsWith(".");
      })) {
        throw new RuntimeException("Logger - " + loggerName + " not found");
      }
      loggerConfig = new LoggerConfig(loggerName, level, true);
      config.addLogger(loggerName, loggerConfig);
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
    if (!getAllConfiguredLoggers().contains(loggerName)) {
      return null;
    }
    LoggerConfig loggerConfig = getLoggerConfig(config, loggerName);
    return getLoggerResponse(loggerConfig);
  }

  /**
   * @return a list of all the configured logger names
   */
  public static List<String> getAllConfiguredLoggers() {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    return config.getLoggers().values().stream().map(LoggerConfig::toString).collect(Collectors.toList());
  }

  /**
   * @return a list of all the logger names
   */
  public static List<String> getAllLoggers() {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    return context.getLoggers().stream().map(AbstractLogger::getName).collect(Collectors.toList());
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
