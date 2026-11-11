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

import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;


/// Thrown by a [StreamMessageDecoder] when a message failed to decode but may decode later, e.g. because a schema
/// registry it depends on is unavailable. Instead of dropping the message, the realtime consumer keeps its offset,
/// backs off and fetches the message again, even when `stopOnDecodeError` is set.
///
/// Do not throw it for failures of the message itself, such as a malformed payload: unless
/// [StreamConfigProperties#DECODE_RETRY_MAX_DURATION_MILLIS] is set, the consumer retries that message forever and
/// stops ingesting the partition.
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RetriableDecodeException extends RuntimeException {

  public RetriableDecodeException(String message) {
    super(message);
  }

  public RetriableDecodeException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetriableDecodeException(Throwable cause) {
    super(cause);
  }
}
