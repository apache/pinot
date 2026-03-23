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

import java.io.File;
import java.util.List;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Local persistent store for data that has been prepared but not yet committed.
 *
 * <p>During the two-phase commit protocol, data from the shard log is materialized into the
 * prepared store so that it survives server restarts. After commit, data is promoted to segments
 * and cleaned up from this store.
 *
 * <p>Implementations must be thread-safe for concurrent access across different statements.
 * Concurrent access to the same statement from multiple threads is not expected.
 */
public interface PreparedStore {

  /**
   * Initializes the store with the given configuration and local data directory.
   *
   * @param config  the Pinot configuration
   * @param dataDir the local directory for persisting prepared data
   */
  void init(PinotConfiguration config, File dataDir);

  /**
   * Stores a chunk of prepared data for the given statement, partition, and sequence number.
   *
   * @param statementId the statement identifier
   * @param partitionId the partition identifier
   * @param sequenceNo  the sequence number within the partition
   * @param data        the serialized data to store
   */
  void store(String statementId, int partitionId, long sequenceNo, byte[] data);

  /**
   * Loads a previously stored chunk of prepared data.
   *
   * @param statementId the statement identifier
   * @param partitionId the partition identifier
   * @param sequenceNo  the sequence number within the partition
   * @return the serialized data, or {@code null} if not found
   */
  byte[] load(String statementId, int partitionId, long sequenceNo);

  /**
   * Lists all statement identifiers that have prepared data in this store.
   *
   * @return a list of statement identifiers
   */
  List<String> listPreparedStatements();

  /**
   * Removes all prepared data for the given statement.
   *
   * @param statementId the statement identifier
   */
  void cleanup(String statementId);
}
