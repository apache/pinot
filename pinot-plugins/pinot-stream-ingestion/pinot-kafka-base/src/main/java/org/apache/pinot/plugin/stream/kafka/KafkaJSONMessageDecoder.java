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
package org.apache.pinot.plugin.stream.kafka;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Map;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaJSONMessageDecoder implements StreamMessageDecoder<byte[], Map<String, Object>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJSONMessageDecoder.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void init(Map<String, String> props, Schema indexingSchema, String topicName) {
  }

  @Override
  public Map<String, Object> decode(byte[] payload) {
    try {
      JsonNode message = JsonUtils.bytesToJsonNode(payload);
      return OBJECT_MAPPER.convertValue(message, new TypeReference<Map<String, Object>>(){});
    } catch (Exception e) {
      LOGGER.error("Caught exception while decoding row, discarding row. Payload is {}", new String(payload), e);
      return null;
    }
  }

  @Override
  public Map<String, Object> decode(byte[] payload, int offset, int length) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length));
  }
}
