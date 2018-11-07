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

import java.util.Map;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;


/**
 * Interface for a decoder of messages fetched from the stream
 * @param <T>
 */
public interface StreamMessageDecoder<T> {

  /**
   * Initialize the decoder with decoder properties map, the stream topic name and stream schema
   * @param props
   * @throws Exception
   */
  void init(Map<String, String> props, Schema indexingSchema, String topicName) throws Exception;

  /**
   * Decodes the payload received into a generic row
   * @param payload
   * @return
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
