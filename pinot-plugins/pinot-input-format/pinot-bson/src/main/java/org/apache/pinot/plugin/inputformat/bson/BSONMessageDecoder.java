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

import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.bson.Document;


/// An implementation of [StreamMessageDecoder] to read BSON records from a stream. Each message payload is
/// expected to be a single binary-encoded BSON document. The document is decoded into a [Document] (a
/// `Map<String, Object>`) and handed to a [BSONRecordExtractor].
public class BSONMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final String BSON_RECORD_EXTRACTOR_CLASS =
      "org.apache.pinot.plugin.inputformat.bson.BSONRecordExtractor";

  private RecordExtractor<Map<String, Object>> _bsonRecordExtractor;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    String recordExtractorClass = null;
    if (props != null) {
      recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
    }
    if (recordExtractorClass == null) {
      recordExtractorClass = BSON_RECORD_EXTRACTOR_CLASS;
    }
    _bsonRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
    _bsonRecordExtractor.init(fieldsToRead, null);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    return decode(payload, 0, payload.length, destination);
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    try {
      Document document = BSONUtils.decodeDocument(payload, offset, length);
      return _bsonRecordExtractor.extract(document, destination);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while decoding BSON record", e);
    }
  }
}
