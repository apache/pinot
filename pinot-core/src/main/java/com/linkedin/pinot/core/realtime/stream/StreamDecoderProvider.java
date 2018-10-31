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

import com.linkedin.pinot.common.Utils;
import com.linkedin.pinot.common.data.Schema;
import java.util.Map;


/**
 * Provider for {@link StreamMessageDecoder}
 */
public abstract class StreamDecoderProvider {

  /**
   * Constructs a {@link StreamMessageDecoder} using properties in {@link StreamConfig} and initializes it
   * @param streamConfig
   * @param schema
   * @return
   */
  public static StreamMessageDecoder create(StreamConfig streamConfig, Schema schema) {
    StreamMessageDecoder decoder = null;
    String decoderClass = streamConfig.getDecoderClass();
    Map<String, String> decoderProperties = streamConfig.getDecoderProperties();
    try {
      decoder = (StreamMessageDecoder) Class.forName(decoderClass).newInstance();
      decoder.init(decoderProperties, schema, streamConfig.getTopicName());
    } catch (Exception e) {
      Utils.rethrowException(e);
    }
    return decoder;
  }
}