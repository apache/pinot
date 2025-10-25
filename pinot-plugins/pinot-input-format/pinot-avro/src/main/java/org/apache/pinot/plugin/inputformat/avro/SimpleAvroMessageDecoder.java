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
package org.apache.pinot.plugin.inputformat.avro;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;


/**
 * An implementation of StreamMessageDecoder to read simple avro records from stream
 * NOTE: Do not use schema in the implementation, as schema will be removed from the params
 */
@NotThreadSafe
public class SimpleAvroMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final String SCHEMA = "schema";
  private static final String LEADING_BYTES_TO_STRIP = "leading.bytes.to.strip";

  private org.apache.avro.Schema _avroSchema;
  private DatumReader<GenericData.Record> _datumReader;
  private RecordExtractor<GenericData.Record> _avroRecordExtractor;
  private BinaryDecoder _binaryDecoderToReuse;
  private GenericData.Record _avroRecordToReuse;
  private int _leadingBytesToStrip = 0;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(SCHEMA), "Avro schema must be provided");
    _avroSchema = new org.apache.avro.Schema.Parser().parse(props.get(SCHEMA));
    _datumReader = new GenericDatumReader<>(_avroSchema);

    // Optional: Strip leading header bytes before decoding (e.g., magic byte + schema id)
    String leadingBytes = props.get(LEADING_BYTES_TO_STRIP);
    if (leadingBytes != null && !leadingBytes.isEmpty()) {
      try {
        _leadingBytesToStrip = Integer.parseInt(leadingBytes);
        Preconditions.checkState(_leadingBytesToStrip >= 0, "'%s' must be non-negative", LEADING_BYTES_TO_STRIP);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid integer for '" + LEADING_BYTES_TO_STRIP + "': " + leadingBytes,
            e);
      }
    }
    String recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
    String recordExtractorConfigClass = props.get(RECORD_EXTRACTOR_CONFIG_CONFIG_KEY);
    // Backward compatibility to support Avro by default
    if (recordExtractorClass == null) {
      recordExtractorClass = AvroRecordExtractor.class.getName();
      recordExtractorConfigClass = AvroRecordExtractorConfig.class.getName();
    }
    RecordExtractorConfig config = null;
    if (recordExtractorConfigClass != null) {
      config = PluginManager.get().createInstance(recordExtractorConfigClass);
      config.init(props);
    }
    _avroRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
    _avroRecordExtractor.init(fieldsToRead, config);
  }

  /**
   * {@inheritDoc}
   *
   * <p>NOTE: the payload should contain message content only (without header).
   */
  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    return decode(payload, 0, payload.length, destination);
  }

  /**
   * {@inheritDoc}
   *
   * <p>NOTE: the payload should contain message content only (without header).
   */
  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    int effectiveOffset = offset + _leadingBytesToStrip;
    int effectiveLength = length - _leadingBytesToStrip;
    if (effectiveLength < 0) {
      throw new IllegalArgumentException("Configured '" + LEADING_BYTES_TO_STRIP + "' (" + _leadingBytesToStrip
          + ") exceeds available payload length (" + length + ")");
    }
    _binaryDecoderToReuse = DecoderFactory.get().binaryDecoder(payload, effectiveOffset, effectiveLength,
        _binaryDecoderToReuse);
    try {
      _avroRecordToReuse = _datumReader.read(_avroRecordToReuse, _binaryDecoderToReuse);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while reading message using schema: " + _avroSchema, e);
    }
    return _avroRecordExtractor.extract(_avroRecordToReuse, destination);
  }
}
