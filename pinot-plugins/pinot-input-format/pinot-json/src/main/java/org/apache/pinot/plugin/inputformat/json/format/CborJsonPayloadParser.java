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

import com.fasterxml.jackson.dataformat.cbor.CBORFactory;


/// Parses <a href="https://www.rfc-editor.org/rfc/rfc8949.html">CBOR</a> (RFC 8949) via Jackson. Values decode
/// to the same Java types Jackson produces for text JSON (CBOR may additionally emit `Float` and `byte[]`
/// scalars, which Pinot handles downstream).
///
/// CBOR has no mandatory magic number, so AUTO detection only recognizes payloads that carry the optional
/// self-describe tag `0xD9D9F7` (RFC 8949 §3.4.6). CBOR streams without that tag must select the format
/// explicitly.
class CborJsonPayloadParser extends JacksonPayloadParser {

  // CBOR "Self-Described CBOR" tag 55799, encoded as the 3-byte prefix 0xD9 0xD9 0xF7 (RFC 8949 §3.4.6).
  private static final int SELF_DESCRIBE_0 = 0xD9;
  private static final int SELF_DESCRIBE_1 = 0xD9;
  private static final int SELF_DESCRIBE_2 = 0xF7;

  CborJsonPayloadParser() {
    super(new CBORFactory());
  }

  @Override
  public boolean matches(byte[] payload, int offset, int length) {
    return length >= 3
        && (payload[offset] & 0xFF) == SELF_DESCRIBE_0
        && (payload[offset + 1] & 0xFF) == SELF_DESCRIBE_1
        && (payload[offset + 2] & 0xFF) == SELF_DESCRIBE_2;
  }
}
