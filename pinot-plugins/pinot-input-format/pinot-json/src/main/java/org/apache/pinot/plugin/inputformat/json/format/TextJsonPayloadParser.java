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


/// Parses UTF-8 text JSON, the historical [org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder]
/// behavior. Delegates to [JsonUtils#bytesToMap] so the produced value types exactly match the rest of Pinot.
class TextJsonPayloadParser implements JsonPayloadParser {

  @Override
  public boolean matches(byte[] payload, int offset, int length) {
    return startsWithJsonDocument(payload, offset, length);
  }

  /// Whether the region's first non-whitespace byte opens a JSON document. A decoder can only turn a top-level
  /// object into a row, but `[` is tolerated so detection does not misfire on top-level arrays.
  ///
  /// Shared with [PostgresJsonbPayloadParser], whose wire format is this same text JSON behind a version byte.
  static boolean startsWithJsonDocument(byte[] payload, int offset, int length) {
    for (int i = offset, end = offset + length; i < end; i++) {
      byte b = payload[i];
      // JSON insignificant whitespace: space, tab, LF, CR.
      if (b == ' ' || b == '\t' || b == '\n' || b == '\r') {
        continue;
      }
      return b == '{' || b == '[';
    }
    return false;
  }

  @Override
  public Map<String, Object> parse(byte[] payload, int offset, int length)
      throws Exception {
    return JsonUtils.bytesToMap(payload, offset, length);
  }
}
