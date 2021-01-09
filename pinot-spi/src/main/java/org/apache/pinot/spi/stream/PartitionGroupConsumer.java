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
package org.apache.pinot.spi.stream;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;


/**
 * Consumer interface for consuming from a partition group of a stream
 */
public interface PartitionGroupConsumer extends Closeable {

  /**
   * Fetch messages and offsets from the stream partition group
   *
   * @param startCheckpoint The offset of the first message desired, inclusive
   * @param endCheckpoint The offset of the last message desired, exclusive, or null
   * @param timeoutMs Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An iterable containing messages fetched from the stream partition and their offsets
   */
  MessageBatch fetchMessages(Checkpoint startCheckpoint, Checkpoint endCheckpoint, int timeoutMs)
      throws TimeoutException;
}
