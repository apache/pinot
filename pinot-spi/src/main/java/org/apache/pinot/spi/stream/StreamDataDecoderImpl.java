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
package org.apache.pinot.spi.stream;

import java.nio.charset.StandardCharsets;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamDataDecoderImpl implements StreamDataDecoder {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamDataDecoderImpl.class);

  public static final String KEY = "__key";
  public static final String HEADER_KEY_PREFIX = "__header$";
  public static final String METADATA_KEY_PREFIX = "__metadata$";

  private final StreamMessageDecoder _valueDecoder;
  private final GenericRow _reuse = new GenericRow();

  public StreamDataDecoderImpl(StreamMessageDecoder valueDecoder) {
    _valueDecoder = valueDecoder;
  }

  @Override
  public StreamDataDecoderResult decode(StreamMessage message) {
    assert message.getValue() != null;

    try {
      _reuse.clear();
      GenericRow row = _valueDecoder.decode(message.getValue(), 0, message.getLength(), _reuse);
      if (row != null) {
        if (message.getKey() != null) {
          row.putValue(KEY, new String(message.getKey(), StandardCharsets.UTF_8));
        }
        RowMetadata metadata = message.getMetadata();
        if (metadata != null) {
          if (metadata.getHeaders() != null) {
            metadata.getHeaders().getFieldToValueMap()
                    .forEach((key, value) -> row.putValue(HEADER_KEY_PREFIX + key, value));
          }
          metadata.getRecordMetadata()
                  .forEach((key, value) -> row.putValue(METADATA_KEY_PREFIX + key, value));
        }
        return new StreamDataDecoderResult(row, null);
      } else {
        return new StreamDataDecoderResult(null,
            new RuntimeException("Encountered unknown exception when decoding a Stream message"));
      }
    } catch (Exception e) {
      LOGGER.error("Failed to decode StreamMessage", e);
      return new StreamDataDecoderResult(null, e);
    }
  }
}
