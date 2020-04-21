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

import java.util.Map;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Interface for a decoder of messages fetched from the stream
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface StreamMessageDecoder<T, U> {

  static final String RECORD_EXTRACTOR_CONFIG_KEY = "recordExtractorClass";

  /**
   * Initialize the decoder with decoder properties map, the stream topic name and stream schema
   * @param props
   * @throws Exception
   */
  void init(Map<String, String> props, Schema indexingSchema, String topicName)
      throws Exception;

  /**
   * Decodes the payload received into a generic row
   * @param payload
   * @return
   */
  U decode(T payload);

  /**
   * Decodes a row.
   *
   * @param payload The buffer from which to read the row.
   * @param offset The offset into the array from which the row contents starts
   * @param length The length of the row contents in bytes
   * @return A new row decoded from the buffer
   */
  U decode(T payload, int offset, int length);
}
