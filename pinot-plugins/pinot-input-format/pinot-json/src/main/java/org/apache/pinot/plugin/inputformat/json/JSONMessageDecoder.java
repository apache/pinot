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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.plugin.inputformat.json.format.JsonPayloadFormat;
import org.apache.pinot.plugin.inputformat.json.format.JsonPayloadParser;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;


/**
 * An implementation of StreamMessageDecoder to read JSON records from a stream.
 *
 * <p>Set the {@value #JSON_FORMAT_CONFIG_KEY} decoder property to pin the payload encoding to one of
 * {@code TEXT}, {@code POSTGRES_JSONB}, {@code SQLITE_JSONB}, {@code SMILE} or {@code CBOR}.
 *
 * <p>When unset (equivalently {@code AUTO}) the encoding is detected per message from its leading magic /
 * version bytes, falling back to text JSON. Detection is allocation-free and cannot mis-route a well-formed
 * text JSON document: a top-level <code>&#123;</code> or <code>[</code> (optionally after whitespace) collides
 * with none of the binary signatures. Pin {@code TEXT} to skip detection entirely.
 * See {@link JsonPayloadFormat}.
 */
public class JSONMessageDecoder implements StreamMessageDecoder<byte[]> {
  public static final String JSON_FORMAT_CONFIG_KEY = "jsonFormat";

  /// Caps on the payload bytes echoed into a decode-failure message. Hex renders two characters per byte, so
  /// the binary cap is a quarter of the text one to keep both messages a comparable length.
  private static final int MAX_DIAGNOSTIC_TEXT_BYTES = 512;
  private static final int MAX_DIAGNOSTIC_HEX_BYTES = 128;

  private static final String JSON_RECORD_EXTRACTOR_CLASS =
      "org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor";

  private RecordExtractor<Map<String, Object>> _jsonRecordExtractor;
  // For AUTO this resolves the concrete format per message; otherwise it is the pinned format's parser.
  private JsonPayloadParser _parser;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    String recordExtractorClass = null;
    String jsonFormat = null;
    if (props != null) {
      recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
      jsonFormat = props.get(JSON_FORMAT_CONFIG_KEY);
    }
    if (recordExtractorClass == null) {
      recordExtractorClass = JSON_RECORD_EXTRACTOR_CLASS;
    }
    _jsonRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
    _jsonRecordExtractor.init(fieldsToRead, null);
    _parser = JsonPayloadFormat.fromConfig(jsonFormat).getParser();
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    return decode(payload, 0, payload.length, destination);
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    try {
      // Parse directly to Map, avoiding an intermediate JsonNode representation for better performance.
      Map<String, Object> jsonMap = _parser.parse(payload, offset, length);
      return _jsonRecordExtractor.extract(jsonMap, destination);
    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while decoding JSON record with payload: " + describePayload(payload, offset, length), e);
    }
  }

  /// Renders a bounded, log-safe description of a payload for an error message.
  ///
  /// The payload may now be any of the binary encodings, so it is neither necessarily text nor necessarily
  /// small. A leading window is rendered as UTF-8 when it looks like text (no control characters other than
  /// whitespace) and as hex otherwise, so a binary message cannot flood the log with megabytes of mojibake,
  /// while a malformed text record still shows enough of itself to be identified. The charset is always
  /// explicit rather than the platform default.
  private static String describePayload(byte[] payload, int offset, int length) {
    boolean text = looksLikeText(payload, offset, Math.min(length, MAX_DIAGNOSTIC_TEXT_BYTES));
    int previewLength = Math.min(length, text ? MAX_DIAGNOSTIC_TEXT_BYTES : MAX_DIAGNOSTIC_HEX_BYTES);
    StringBuilder description = new StringBuilder(32 + (text ? previewLength : 2 * previewLength));
    description.append(length).append(" bytes: ");
    if (text) {
      // Truncation may split a multi-byte character; UTF-8 decoding substitutes U+FFFD rather than throwing.
      description.append(new String(payload, offset, previewLength, StandardCharsets.UTF_8));
    } else {
      description.append("0x");
      for (int i = 0; i < previewLength; i++) {
        description.append(Character.forDigit((payload[offset + i] >> 4) & 0xF, 16))
            .append(Character.forDigit(payload[offset + i] & 0xF, 16));
      }
    }
    if (previewLength < length) {
      description.append("...(truncated)");
    }
    return description.toString();
  }

  /// Whether the region contains no control characters other than JSON's insignificant whitespace. Bytes at or
  /// above 0x80 are accepted so non-ASCII UTF-8 text still renders as text.
  private static boolean looksLikeText(byte[] payload, int offset, int length) {
    for (int i = offset, end = offset + length; i < end; i++) {
      byte b = payload[i];
      if (b >= 0 && b < 0x20 && b != '\t' && b != '\n' && b != '\r') {
        return false;
      }
    }
    return true;
  }
}
