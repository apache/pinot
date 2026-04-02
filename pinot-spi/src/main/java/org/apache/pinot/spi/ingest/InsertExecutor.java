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
package org.apache.pinot.spi.ingest;

/**
 * Executes INSERT INTO statements and tracks their lifecycle.
 *
 * <p>Implementations are responsible for ingesting data (rows or files), managing the statement
 * state machine, and producing segments. Different backends (e.g., Ratis-based row ingestion,
 * minion-based file ingestion) provide their own implementations.
 *
 * <p>Implementations must be thread-safe; a single instance may be invoked concurrently from
 * multiple broker request threads.
 */
public interface InsertExecutor {

  /**
   * Executes the given insert request.
   *
   * @param request the insert request containing data and metadata
   * @return the result reflecting the current state of the statement
   */
  InsertResult execute(InsertRequest request);

  /**
   * Returns the current status of a previously submitted statement.
   *
   * @param statementId the unique identifier of the statement
   * @return the result reflecting the current state, or a result with
   *         {@link InsertStatementState#ABORTED} if the statement is unknown
   */
  InsertResult getStatus(String statementId);

  /**
   * Aborts a previously submitted statement, releasing any resources it holds.
   *
   * @param statementId the unique identifier of the statement to abort
   * @return the result reflecting the final aborted state
   */
  InsertResult abort(String statementId);
}
