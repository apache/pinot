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


/// Parses a stream payload encoded in a particular (text or binary) JSON representation into a Jackson-style
/// `Map<String, Object>` so the shared
/// [org.apache.pinot.plugin.inputformat.json.JSONRecordExtractor] can turn it into a
/// [org.apache.pinot.spi.data.readers.GenericRow].
///
/// Implementations must produce the same Java value contract Jackson produces for text JSON, so the extractor
/// can treat every format uniformly: `Boolean`, `Integer`, `Long`, `Double`, `String`, `java.math.BigInteger`
/// / `java.math.BigDecimal` for oversized / high-precision numbers, `java.util.List` for arrays, and nested
/// `Map<String, Object>` for objects. Binary formats may additionally yield `Float` and `byte[]` scalars,
/// which Pinot's downstream type conversion handles.
///
/// Implementations are stateless and must be thread-safe: a single instance is shared across all decode calls.
public interface JsonPayloadParser {

  /// Cheap, allocation-free check of the payload's leading bytes to decide whether this parser recognizes the
  /// encoding. Used only by [JsonPayloadFormat#AUTO] detection; when a format is configured explicitly the
  /// corresponding parser is used without consulting this method.
  ///
  /// @param payload backing byte array
  /// @param offset  start offset of the record within {@code payload}
  /// @param length  number of bytes belonging to the record
  /// @return {@code true} if the leading bytes match this format's magic / version signature
  boolean matches(byte[] payload, int offset, int length);

  /// Parses the {@code [offset, offset + length)} region of {@code payload} into a mutable
  /// `Map<String, Object>` following the value contract described on this interface.
  ///
  /// @throws Exception if the region is not valid for this format
  Map<String, Object> parse(byte[] payload, int offset, int length)
      throws Exception;
}
