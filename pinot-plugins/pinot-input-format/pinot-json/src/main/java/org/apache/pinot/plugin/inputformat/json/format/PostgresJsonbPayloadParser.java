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

import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;


/// Parses the PostgreSQL `jsonb` binary wire format: a single version byte (currently `1`) followed by the
/// value's **UTF-8 text** JSON representation.
///
/// Despite `jsonb`'s compact on-disk `JsonbContainer` layout, that layout never leaves the server. PostgreSQL's
/// `jsonb_send` (`src/backend/utils/adt/jsonb.c`) renders the value with `JsonbToCString` and emits
/// `pq_sendint8(version = 1)` followed by the text; `jsonb_recv` reverses it and rejects any other version.
/// Every standard binary producer — the v3 extended-query protocol, `COPY ... WITH (FORMAT binary)`, and
/// logical replication via `pgoutput` — routes through that same send function, so the version-byte + text
/// framing is what a stream actually carries.
///
/// Because the body is ordinary text JSON, values decode through [JsonUtils#bytesToMap] and therefore follow
/// exactly the same type contract as [TextJsonPayloadParser].
class PostgresJsonbPayloadParser implements JsonPayloadParser {

  /// The only version `jsonb_recv` accepts.
  private static final byte JSONB_VERSION = 1;

  @Override
  public boolean matches(byte[] payload, int offset, int length) {
    // Version byte followed by a text JSON document. Requiring the JSON start character keeps this from
    // claiming arbitrary binary payloads that merely happen to begin with 0x01.
    return length >= 2 && payload[offset] == JSONB_VERSION && TextJsonPayloadParser.startsWithJsonDocument(payload,
        offset + 1, length - 1);
  }

  @Override
  public Map<String, Object> parse(byte[] payload, int offset, int length)
      throws Exception {
    if (length < 2 || payload[offset] != JSONB_VERSION) {
      throw new IllegalArgumentException("Payload is not a version-1 PostgreSQL jsonb value");
    }
    return JsonUtils.bytesToMap(payload, offset + 1, length - 1);
  }
}
