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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * An implementation of StreamMessageDecoder to read JSON records from a stream.
 */
public class JSONMessageDecoder implements StreamMessageDecoder<byte[]> {
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
    return decode(payload, 0, payload.length, destination);
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    try {
      JsonNode message = JsonUtils.bytesToJsonNode(payload, offset, length);
      return _jsonRecordExtractor.extract(JsonUtils.jsonNodeToMap(message), destination);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while decoding JSON record with payload: " + new String(payload, offset, length), e);
    }
  }
}
