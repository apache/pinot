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
package org.apache.pinot.plugin.inputformat.json;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of StreamMessageDecoder to read JSON records from a stream.
 */
public class JSONMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JSONMessageDecoder.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String JSON_RECORD_EXTRACTOR_CLASS =
      "org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor";

  private RecordExtractor<Map<String, Object>> _jsonRecordExtractor;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    String recordExtractorClass = null;
    if (props != null) {
      recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
    }
    if (recordExtractorClass == null) {
      recordExtractorClass = JSON_RECORD_EXTRACTOR_CLASS;
    }
    _jsonRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
    _jsonRecordExtractor.init(fieldsToRead, null);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      JsonNode message = JsonUtils.bytesToJsonNode(payload);
      Map<String, Object> from = OBJECT_MAPPER.convertValue(message, new TypeReference<Map<String, Object>>() {
      });
      _jsonRecordExtractor.extract(from, destination);
      return destination;
    } catch (Exception e) {
      LOGGER.error("Caught exception while decoding row, discarding row. Payload is {}", new String(payload), e);
      return null;
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
