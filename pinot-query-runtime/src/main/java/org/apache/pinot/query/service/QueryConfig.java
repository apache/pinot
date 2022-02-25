package org.apache.pinot.query.service;

/**
 * Configuration for setting up query runtime.
 */
public class QueryConfig {

  public static final String KEY_OF_QUERY_RUNNER_HOSTNAME = "pinot.query.runner.hostname";
  public static final String DEFAULT_QUERY_RUNNER_HOSTNAME = "localhost";
  // query runner port is the mailbox port.
  public static final String KEY_OF_QUERY_RUNNER_PORT = "pinot.query.runner.port";
  public static final int DEFAULT_QUERY_RUNNER_PORT = -1;
}
