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
import org.apache.pinot.spi.plugin.PluginManager;


/**
 * Provider for {@link StreamMessageDecoder}
 */
public class StreamDecoderProvider {
  private StreamDecoderProvider() {
  }

  /**
   * Creates a {@link StreamMessageDecoder} using properties in {@link StreamConfig}.
   *
   * @param streamConfig The stream config from the table config
   * @param fieldsToRead The fields to read from the source stream
   * @return The initialized StreamMessageDecoder
   */
  public static StreamMessageDecoder create(StreamConfig streamConfig, Set<String> fieldsToRead) {
    String decoderClass = streamConfig.getDecoderClass();
    Map<String, String> decoderProperties = streamConfig.getDecoderProperties();
    try {
      StreamMessageDecoder decoder = PluginManager.get().createInstance(decoderClass);
      decoder.init(decoderProperties, fieldsToRead, streamConfig.getTopicName());
      return decoder;
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while creating StreamMessageDecoder from stream config: " + streamConfig, e);
    }
  }
}
