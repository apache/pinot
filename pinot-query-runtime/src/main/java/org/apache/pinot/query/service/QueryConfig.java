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
  /**
   * Configuration for mailbox data block size
   */
  public static final String KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = "pinot.query.runner.max.msg.size.bytes";
  public static final int DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES = 16 * 1024 * 1024;

  public static final String KEY_OF_MAILBOX_TIMEOUT_MS = "pinot.query.runner.mailbox.timeout.ms";
  public static final long DEFAULT_MAILBOX_TIMEOUT_MS = 10_000L;

  /**
   * Configuration for server port, port that opens and accepts
   * {@link org.apache.pinot.query.runtime.plan.DistributedStagePlan} and start executing query stages.
   */
  public static final String KEY_OF_QUERY_SERVER_PORT = "pinot.query.server.port";
  public static final int DEFAULT_QUERY_SERVER_PORT = 0;

  /**
   * Configuration for mailbox hostname and port, this hostname and port opens streaming channel to receive
   * {@link org.apache.pinot.common.datablock.DataBlock}.
   */
  public static final String KEY_OF_QUERY_RUNNER_HOSTNAME = "pinot.query.runner.hostname";
  public static final String DEFAULT_QUERY_RUNNER_HOSTNAME = "localhost";
  public static final String KEY_OF_QUERY_RUNNER_PORT = "pinot.query.runner.port";
  public static final int DEFAULT_QUERY_RUNNER_PORT = 0;

  /**
   * Configuration keys for {@link org.apache.pinot.common.proto.Worker.QueryRequest} extra metadata.
   */
  public static final String KEY_OF_BROKER_REQUEST_ID = "pinot.query.runner.broker.request.id";
  public static final String KEY_OF_BROKER_REQUEST_TIMEOUT_MS = "pinot.query.runner.broker.request.timeout.ms";

  /**
   * Configuration keys for {@link org.apache.pinot.common.proto.Worker.QueryResponse} extra metadata.
   */
  public static final String KEY_OF_SERVER_RESPONSE_STATUS_ERROR = "ERROR";
  public static final String KEY_OF_SERVER_RESPONSE_STATUS_OK = "OK";

  /**
   * Configuration keys for managing the scheduler
   */

  /**
   * The maximum time that a operator chain will be held in the queue without being scheduled for execution.
   * This is intended as a defensive measure for situations where we notice that an operator is not being
   * scheduled when it otherwise should be. The default value, -1, indicates that we should never release
   * an operator chain despite any amount of time elapsed.
   */
  public static final String KEY_OF_SCHEDULER_RELEASE_TIMEOUT_MS = "pinot.query.scheduler.release.timeout.ms";
  public static final long DEFAULT_SCHEDULER_RELEASE_TIMEOUT_MS = 10_000;

  private QueryConfig() {
    // do not instantiate.
  }
}
