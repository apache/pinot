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
package org.apache.pinot.query.service;

/**
 * Configuration for setting up query runtime.
 */
public class QueryConfig {
  public static final long DEFAULT_TIMEOUT_NANO = 10_000_000_000L;

  public static final String KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = "pinot.query.runner.max.msg.size.bytes";
  public static final int DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;

  public static final String KEY_OF_QUERY_SERVER_PORT = "pinot.query.server.port";
  public static final int DEFAULT_QUERY_SERVER_PORT = 0;

  public static final String KEY_OF_QUERY_RUNNER_HOSTNAME = "pinot.query.runner.hostname";
  public static final String DEFAULT_QUERY_RUNNER_HOSTNAME = "localhost";
  // query runner port is the mailbox port.
  public static final String KEY_OF_QUERY_RUNNER_PORT = "pinot.query.runner.port";
  public static final int DEFAULT_QUERY_RUNNER_PORT = 0;

  private QueryConfig() {
    // do not instantiate.
  }
}
