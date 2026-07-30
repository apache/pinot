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
package org.apache.pinot.plugin.inputformat.json.format;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;


/// Base for formats that Jackson can read directly given a binary [JsonFactory] (Smile, CBOR). Subclasses only
/// supply the factory and their magic-byte [#matches] check.
///
/// The [ObjectReader] is immutable and thread-safe, so a single instance is shared across all decode calls.
abstract class JacksonPayloadParser implements JsonPayloadParser {

  private final ObjectReader _mapReader;

  JacksonPayloadParser(JsonFactory factory) {
    _mapReader = new ObjectMapper(factory).readerFor(JsonUtils.MAP_TYPE_REFERENCE);
  }

  @Override
  public Map<String, Object> parse(byte[] payload, int offset, int length)
      throws Exception {
    return _mapReader.readValue(payload, offset, length);
  }
}
