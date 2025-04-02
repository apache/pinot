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
package org.apache.pinot.controller.helix.core.rebalance;

import org.slf4j.Logger;
import org.slf4j.helpers.MessageFormatter;


public class TableRebalanceLogger {
  private final Logger _logger;
  private final String _rebalanceJobId;

  public TableRebalanceLogger(Logger baseLogger) {
    _logger = baseLogger;
    _rebalanceJobId = "";
  }

  public TableRebalanceLogger(Logger baseLogger, String rebalanceJobId) {
    _logger = baseLogger;
    _rebalanceJobId = rebalanceJobId;
  }

  private static String formatMessage(String message, String rebalanceJobId, Object... args) {
    return String.format("[%s] %s", rebalanceJobId, MessageFormatter.arrayFormat(message, args).getMessage());
  }

  public void info(String message, Object... args) {
    _logger.info(formatMessage(message, _rebalanceJobId, args));
  }

  public void warn(String message, Object... args) {
    _logger.warn(formatMessage(message, _rebalanceJobId, args));
  }

  public void error(String message, Object... args) {
    _logger.error(formatMessage(message, _rebalanceJobId, args));
  }

  public void debug(String message, Object... args) {
    _logger.debug(formatMessage(message, _rebalanceJobId, args));
  }
}
