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

import com.fasterxml.jackson.dataformat.smile.SmileConstants;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;


/// Parses <a href="https://github.com/FasterXML/smile-format-specification">Smile</a>, Jackson's own binary
/// JSON encoding. Values decode to the same Java types Jackson produces for text JSON (Smile may additionally
/// emit `Float` and `byte[]` scalars, which Pinot handles downstream).
///
/// Detection relies on the 3-byte Smile header `:)\n` (`0x3A 0x29 0x0A`) that Jackson writes by default;
/// header-less Smile payloads must select the format explicitly rather than via AUTO detection.
class SmileJsonPayloadParser extends JacksonPayloadParser {

  SmileJsonPayloadParser() {
    super(new SmileFactory());
  }

  @Override
  public boolean matches(byte[] payload, int offset, int length) {
    return length >= 3
        && payload[offset] == SmileConstants.HEADER_BYTE_1
        && payload[offset + 1] == SmileConstants.HEADER_BYTE_2
        && payload[offset + 2] == SmileConstants.HEADER_BYTE_3;
  }
}
