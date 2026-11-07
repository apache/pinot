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


/// Thrown by a [StreamMessageDecoder] when a message could not be decoded now but may decode later, for example
/// because a schema registry it depends on timed out, throttled the request or returned a server error.
///
/// Any other decode failure drops the message permanently (unless `stopOnDecodeError` is set). On this exception the
/// realtime consumer instead keeps its offset, backs off and fetches the same message again, so a transient outage of
/// a decoder dependency delays ingestion rather than losing data. Do not throw it for failures of the message itself,
/// such as a malformed payload or an unknown schema: unless [StreamConfigProperties#DECODE_RETRY_MAX_DURATION_MILLIS]
/// is set, the consumer would retry that message indefinitely and stop ingesting the partition. That setting bounds an
/// outage rather than each message: retriable failures less than that duration apart belong to the same outage, and
/// once it has lasted that long, they are handled like any other decode failure until none happens for that long.
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
