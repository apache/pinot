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
package org.apache.pinot.query.runtime.operator.utils;

/**
 * An interface that represents an abstract blocking stream of elements that can be consumed.
 *
 * These streams are designed to be consumed by a single thread and do not support null elements.
 *
 * @param <E> The type of the elements, usually a {@link org.apache.pinot.query.runtime.blocks.TransferableBlock}
 */
public interface BlockingStream<E> {
  /**
   * The id of the stream. Mostly used for logging.
   *
   * Implementations of this method must be thread safe.
   */
  Object getId();

  /**
   * Returns the next element on the stream, blocking if there is no element ready.
   */
  E get();

  /**
   * Cancels the stream.
   *
   * This method can be called by any thread.
   */
  void cancel();
}
