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

import java.util.Iterator;


/**
 * A durable, ordered log for a single table partition used to stage INSERT data before it is
 * committed to segments.
 *
 * <p>The shard log provides the fundamental building blocks for two-phase commit: data is first
 * appended, then the offset range is prepared, and finally committed (or aborted). Implementations
 * may use Ratis, Kafka, or any other replicated log.
 *
 * <p>Implementations must be thread-safe for concurrent appends but may serialize prepare/commit
 * operations for a given statement.
 */
public interface ShardLog {

  /**
   * Appends data to the log and returns the offset at which it was written.
   *
   * @param data the serialized data to append
   * @return the offset of the appended entry
   */
  long append(byte[] data);

  /**
   * Marks the given offset range as prepared for the specified statement. After this call, the
   * data in {@code [startOffset, endOffset]} is considered part of the statement's transaction.
   *
   * @param statementId the statement identifier
   * @param startOffset the start offset (inclusive)
   * @param endOffset   the end offset (inclusive)
   */
  void prepare(String statementId, long startOffset, long endOffset);

  /**
   * Commits the prepared data for the specified statement, making it eligible for segment building.
   *
   * @param statementId the statement identifier
   */
  void commit(String statementId);

  /**
   * Aborts the prepared data for the specified statement, discarding the offset range.
   *
   * @param statementId the statement identifier
   */
  void abort(String statementId);

  /**
   * Reads log entries starting from the given offset.
   *
   * @param fromOffset the offset to start reading from (inclusive)
   * @return an iterator over the serialized data entries
   */
  Iterator<byte[]> read(long fromOffset);
}
