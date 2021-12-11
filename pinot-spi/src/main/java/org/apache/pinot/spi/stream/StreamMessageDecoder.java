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
import java.util.Set;
import org.apache.pinot.spi.annotations.InterfaceAudience;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Interface for a decoder of messages fetched from the stream
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface StreamMessageDecoder<T> {

  String RECORD_EXTRACTOR_CONFIG_KEY = "recordExtractorClass";

  /**
   * Initializes the decoder.
   *
   * @param props Decoder properties extracted from the {@link StreamConfig}
   * @param fieldsToRead The fields to read from the source stream. If blank, reads all fields (only for AVRO/JSON
   *                     currently)
   * @param topicName Topic name of the stream
   * @throws Exception If an error occurs
   */
  void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception;

  /**
   * Decodes a row.
   *
   * @param payload The buffer from which to read the row.
   * @return A new row decoded from the buffer
   */
  GenericRow decode(T payload, GenericRow destination);

  /**
   * Decodes a row.
   *
   * @param payload The buffer from which to read the row.
   * @param offset The offset into the array from which the row contents starts
   * @param length The length of the row contents in bytes
   * @param destination The {@link GenericRow} to write the decoded row into
   * @return A new row decoded from the buffer
   */
  GenericRow decode(T payload, int offset, int length, GenericRow destination);
}
