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
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;


/// Base for formats that Jackson can read directly given a binary [JsonFactory] (Smile, CBOR). Subclasses only
/// supply the factory and their magic-byte [#matches] check.
///
/// The [ObjectReader] is immutable and thread-safe, so a single instance is shared across all decode calls.
abstract class JacksonPayloadParser implements JsonPayloadParser {

  private final JsonFactory _factory;
  private final ObjectReader _mapReader;
  private final ObjectReader _valueReader;

  JacksonPayloadParser(JsonFactory factory) {
    this(new ObjectMapper(factory).reader());
  }

  /// The reader must use default deserialization features: [#readValue]'s scalar fast path bypasses databind,
  /// so features like `USE_BIG_DECIMAL_FOR_FLOATS` would apply to containers but silently not to top-level
  /// scalars. Every current caller passes a default-configured reader.
  JacksonPayloadParser(ObjectReader reader) {
    _factory = reader.getFactory();
    _mapReader = reader.forType(JsonUtils.MAP_TYPE_REFERENCE);
    _valueReader = reader.forType(Object.class);
  }

  @Override
  public Map<String, Object> parse(byte[] payload, int offset, int length)
      throws Exception {
    return parseMap(payload, offset, length);
  }

  protected final Map<String, Object> parseMap(byte[] payload, int offset, int length)
      throws Exception {
    return _mapReader.readValue(payload, offset, length);
  }

  /// Streams the top-level object fields into the row. Nested selected values still use Jackson's normal
  /// materialization and are converted by [JSONRecordExtractor], but the top-level map is never allocated.
  /// Unselected containers are skipped without materializing their contents.
  @Override
  public boolean parse(byte[] payload, int offset, int length, GenericRow destination,
      @Nullable Set<String> fields)
      throws Exception {
    if (fields != null) {
      // Match JSONRecordExtractor's missing-field behavior and overwrite values left in a reused row.
      for (String field : fields) {
        destination.putValue(field, null);
      }
    }
    try (JsonParser parser = _factory.createParser(payload, offset, length)) {
      if (parser.nextToken() != JsonToken.START_OBJECT) {
        throw new IllegalArgumentException("Top-level JSON value must be an object");
      }
      JsonToken token;
      while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {
        if (token == null) {
          throw new IllegalArgumentException("Unexpected end of JSON object");
        }
        if (token != JsonToken.FIELD_NAME) {
          throw new IllegalArgumentException("Expected a JSON object field, found: " + token);
        }
        String fieldName = parser.currentName();
        JsonToken valueToken = parser.nextToken();
        if (valueToken == null) {
          throw new IllegalArgumentException("Unexpected end of JSON value for field: " + fieldName);
        }
        if (fields == null || fields.contains(fieldName)) {
          destination.putValue(fieldName, readValue(parser, valueToken));
        } else if (valueToken.isStructStart()) {
          parser.skipChildren();
        }
      }
    }
    return true;
  }

  /// Materializes the value at the parser's current token in [JSONRecordExtractor]'s converted shape. Scalars
  /// are read straight off the parser rather than through databind, which allocates a fresh
  /// `DeserializationContext` per `readValue` call. `getNumberValue()` keeps the binary formats' `Float`
  /// (never upcast to `Double`) and text JSON's `Double`, matching Jackson's untyped materialization.
  @Nullable
  private Object readValue(JsonParser parser, JsonToken valueToken)
      throws IOException {
    switch (valueToken) {
      case VALUE_STRING:
        return parser.getText();
      case VALUE_NUMBER_INT:
      case VALUE_NUMBER_FLOAT:
        // Oversized ints widen to BigDecimal via the shared contract (Pinot has no BigInteger type)
        return JSONRecordExtractor.convert(parser.getNumberValue());
      case VALUE_TRUE:
        return Boolean.TRUE;
      case VALUE_FALSE:
        return Boolean.FALSE;
      case VALUE_NULL:
        return null;
      case VALUE_EMBEDDED_OBJECT:
        // Binary formats' native scalars (e.g. byte[]) pass through unchanged, as in the map path.
        return parser.getEmbeddedObject();
      default:
        // START_OBJECT / START_ARRAY: containers materialize through databind and convert recursively.
        Object value = _valueReader.readValue(parser);
        return value != null ? JSONRecordExtractor.convert(value) : null;
    }
  }
}
