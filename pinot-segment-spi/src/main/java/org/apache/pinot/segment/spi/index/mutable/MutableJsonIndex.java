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
package org.apache.pinot.segment.spi.index.mutable;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.spi.utils.JsonUtils;


public interface MutableJsonIndex extends JsonIndexReader, MutableIndex {

  @Override
  default void add(Object value, int dictId, int docId) {
    try {
      if (value instanceof Map || value instanceof List || value instanceof JsonNode) {
        // Already-parsed JSON value (e.g. a Map / JsonNode cached on the GenericRow before it was serialized for the
        // forward index): flatten it directly, avoiding the serialize-then-reparse round-trip.
        addParsed(value);
      } else {
        // String (the common case) or, for any other unexpected type, fail fast with a ClassCastException as before.
        add((String) value);
      }
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  /// Indexes an already-parsed JSON document ({@code Map} / {@code List}). The default serializes and re-parses for
  /// compatibility; implementations should override to flatten the parsed value directly, avoiding the re-parse.
  default void addParsed(Object parsedValue)
      throws IOException {
    add(JsonUtils.objectToString(parsedValue));
  }

  /// Whether {@link #addParsed} flattens a parsed value directly (no serialize+reparse). Callers should feed the
  /// cached parsed value via {@link #addParsed} only when this is {@code true}; otherwise feeding a parsed value would
  /// trigger the default serialize+reparse, which is slower than passing the already-serialized string. Default
  /// {@code false}; implementations that override {@link #addParsed} should return {@code true}.
  default boolean supportsParsedValue() {
    return false;
  }

  @Override
  default void add(Object[] values, @Nullable int[] dictIds, int docId) {
    throw new UnsupportedOperationException("Mutable JSON indexes are not supported for multi-valued columns");
  }

  /**
   * Index a JSON document
   * @param jsonString the JSON
   */
  void add(String jsonString)
      throws IOException;
}
