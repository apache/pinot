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

import java.util.Locale;
import javax.annotation.Nullable;


/// The payload encodings the JSON stream decoder understands, selected through the `jsonFormat` decoder
/// property.
///
/// An unset property means [#TEXT], preserving the decoder's historical behavior exactly. [#AUTO] is opt-in:
/// detection is a heuristic over a few leading bytes, and while it never mis-routes a well-formed text JSON
/// document, it can claim a corrupt message that text decoding would have rejected outright. Turning that on
/// for every existing stream would be a silent correctness change, so operators ask for it explicitly.
public enum JsonPayloadFormat {
  /// Detect the encoding per message from its leading magic / version bytes, falling back to text JSON.
  /// Opt-in; see the note on this enum.
  AUTO(new AutoDetectPayloadParser()),
  /// UTF-8 text JSON (the historical default).
  TEXT(new TextJsonPayloadParser()),
  /// PostgreSQL `jsonb` binary wire format (version byte + text JSON).
  POSTGRES_JSONB(new PostgresJsonbPayloadParser()),
  /// SQLite 3.45+ JSONB binary format.
  SQLITE_JSONB(new SqliteJsonbPayloadParser()),
  /// Jackson Smile binary JSON.
  SMILE(new SmileJsonPayloadParser()),
  /// CBOR (RFC 8949).
  CBOR(new CborJsonPayloadParser());

  // AUTO detection precedence: formats with strong, unambiguous magic bytes first; text (the catch-all,
  // detected only by a leading '{' / '[') last so it never shadows a binary format. POSTGRES_JSONB precedes
  // TEXT because its body *is* text JSON behind a version byte.
  private static final JsonPayloadFormat[] DETECTION_ORDER =
      {SMILE, CBOR, POSTGRES_JSONB, SQLITE_JSONB, TEXT};

  private final JsonPayloadParser _parser;

  JsonPayloadFormat(JsonPayloadParser parser) {
    _parser = parser;
  }

  /// The parser for this format. For [#AUTO] this is a parser that resolves the concrete format per message.
  public JsonPayloadParser getParser() {
    return _parser;
  }

  /// Resolves a configured format name (case-insensitive). An unset value means [#TEXT], the decoder's
  /// historical behavior; [#AUTO] must be requested explicitly.
  public static JsonPayloadFormat fromConfig(@Nullable String value) {
    if (value == null || value.trim().isEmpty()) {
      return TEXT;
    }
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unsupported jsonFormat '" + value + "'. Supported values: AUTO, TEXT, POSTGRES_JSONB, SQLITE_JSONB, "
              + "SMILE, CBOR", e);
    }
  }

  /// Picks a parser for a payload by consulting each format's magic / version bytes in [#DETECTION_ORDER],
  /// defaulting to text JSON when nothing matches.
  public static JsonPayloadParser detectParser(byte[] payload, int offset, int length) {
    for (JsonPayloadFormat format : DETECTION_ORDER) {
      if (format._parser.matches(payload, offset, length)) {
        return format._parser;
      }
    }
    return TEXT._parser;
  }
}
