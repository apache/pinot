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

import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Coordinates the atomic publication of segments produced by an INSERT INTO statement.
 *
 * <p>This interface abstracts the final step of making segments visible to queries. It ensures
 * that all segments for a statement become queryable atomically — either all are published or
 * none are.
 *
 * <p>Implementations must be thread-safe for concurrent operations across different statements.
 */
public interface PublishCoordinator {

  /**
   * Initializes the coordinator with the given configuration. Called once during startup.
   *
   * @param config the Pinot configuration
   */
  void init(PinotConfiguration config);

  /**
   * Begins a publish transaction for the given statement and segments.
   *
   * @param statementId       the statement identifier
   * @param tableNameWithType the fully qualified table name (e.g., {@code myTable_OFFLINE})
   * @param segmentNames      the names of segments to be published
   * @return a publish identifier that must be passed to {@link #commitPublish} or
   *         {@link #abortPublish}
   */
  String beginPublish(String statementId, String tableNameWithType, List<String> segmentNames);

  /**
   * Commits the publish transaction, making all segments visible to queries.
   *
   * @param publishId the publish identifier returned by {@link #beginPublish}
   */
  void commitPublish(String publishId);

  /**
   * Aborts the publish transaction, rolling back any partially published segments.
   *
   * @param publishId the publish identifier returned by {@link #beginPublish}
   */
  void abortPublish(String publishId);
}
