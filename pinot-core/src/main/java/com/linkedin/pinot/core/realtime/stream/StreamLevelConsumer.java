/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.stream;

import com.linkedin.pinot.core.data.GenericRow;


/**
 * Interface for a consumer that consumes at stream level and is unaware of any partitions of the stream
 */
public interface StreamLevelConsumer {

  /**
   * Initialize and start the stream level consumer
   * @throws Exception
   */
  void start() throws Exception;

  /**
   * Set the offset to consume from
   * @param offset
   */
  void setOffset(long offset);

  /**
   * Get next row from the stream and decode it into a generic row
   * @param destination
   * @return
   */
  GenericRow next(GenericRow destination);

  /**
   * Get decoded row from the stream at a given offset
   * @param offset
   * @return
   */
  GenericRow next(long offset);

  /**
   * Get the current offset for the stream
   * @return
   */
  long currentOffset();

  /**
   * Checkpoint the stream at the current offset
   */
  void commit();

  /**
   * Checkpoint the stream at the given offset
   * @param offset
   */
  void commit(long offset);

  /**
   * Shutdown the stream consumer
   * @throws Exception
   */
  void shutdown() throws Exception;
}
