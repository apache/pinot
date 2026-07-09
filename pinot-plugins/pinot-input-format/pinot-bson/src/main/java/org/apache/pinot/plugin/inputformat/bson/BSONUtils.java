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
package org.apache.pinot.plugin.inputformat.bson;

import java.nio.ByteBuffer;
import org.bson.BsonBinaryReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;


/// Decodes a single BSON binary document into an `org.bson.Document` (a `Map<String, Object>`). Shared by
/// [BSONRecordReader] (batch) and [BSONMessageDecoder] (streaming).
public final class BSONUtils {
  private BSONUtils() {
  }

  private static final DocumentCodec DOCUMENT_CODEC = new DocumentCodec();

  /// Decodes a whole BSON document from the given byte array.
  public static Document decodeDocument(byte[] bytes) {
    return decodeDocument(bytes, 0, bytes.length);
  }

  /// Decodes a single BSON document from the `[offset, offset + length)` sub-range of the given byte array.
  public static Document decodeDocument(byte[] bytes, int offset, int length) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length).slice();
    try (BsonBinaryReader reader = new BsonBinaryReader(buffer)) {
      return DOCUMENT_CODEC.decode(reader, DecoderContext.builder().build());
    }
  }
}
