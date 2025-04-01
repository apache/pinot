package org.apache.pinot.controller.helix.core.rebalance;

import org.slf4j.Logger;


public class TableRebalanceLogger {
  private final Logger logger;
  private final String rebalanceJobId;

  public TableRebalanceLogger(Logger baseLogger) {
    this.logger = baseLogger;
    this.rebalanceJobId = "";
  }

  public TableRebalanceLogger(Logger baseLogger, String rebalanceJobId) {
    this.logger = baseLogger;
    this.rebalanceJobId = rebalanceJobId;
  }

  private static String formatMessage(String message, String rebalanceJobId) {
    return String.format("[%s] %s", rebalanceJobId, message);
  }

  public void info(String message, Object... args) {
    logger.info(formatMessage(String.format(message, args), rebalanceJobId));
  }

  public void warn(String message, Object... args) {
    logger.warn(formatMessage(String.format(message, args), rebalanceJobId));
  }

  public void error(String message, Object... args) {
    logger.error(formatMessage(String.format(message, args), rebalanceJobId));
  }

  public void debug(String message, Object... args) {
    logger.debug(formatMessage(String.format(message, args), rebalanceJobId));
  }
}