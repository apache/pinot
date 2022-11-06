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

/**
 * A decoder for {@link StreamMessage}
 */
public interface StreamDataDecoder {
  /**
   * Decodes a {@link StreamMessage}
   *
   * Please note that the expectation is that the implementations of this class should never throw an exception.
   * Instead, it should encapsulate the exception within the {@link StreamDataDecoderResult} object.
   *
   * @param message {@link StreamMessage} that contains the data payload and optionally, a key and row metadata
   * @return {@link StreamDataDecoderResult} that either contains the decoded row or the exception
   */
  StreamDataDecoderResult decode(StreamMessage message);
}
