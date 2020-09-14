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
import java.io.IOException;
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
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of StreamMessageDecoder to read simple avro records from stream
 * NOTE: Do not use schema in the implementation, as schema will be removed from the params
 */
@NotThreadSafe
public class SimpleAvroMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleAvroMessageDecoder.class);

  private static final String SCHEMA = "schema";

  private org.apache.avro.Schema _avroSchema;
  private DatumReader<GenericData.Record> _datumReader;
  private RecordExtractor<GenericData.Record, GenericData.Record> _avroRecordExtractor;
  private BinaryDecoder _binaryDecoderToReuse;
  private GenericData.Record _avroRecordToReuse;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(SCHEMA), "Avro schema must be provided");
    _avroSchema = new org.apache.avro.Schema.Parser().parse(props.get(SCHEMA));
    _datumReader = new GenericDatumReader<>(_avroSchema);
    String recordExtractorClass = props.get(RECORD_EXTRACTOR_CONFIG_KEY);
    // Backward compatibility to support Avro by default
    if (recordExtractorClass == null) {
      recordExtractorClass = AvroRecordExtractor.class.getName();
    }
    _avroRecordExtractor = PluginManager.get().createInstance(recordExtractorClass);
    _avroRecordExtractor.init(fieldsToRead, null);
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
    _binaryDecoderToReuse = DecoderFactory.get().binaryDecoder(payload, offset, length, _binaryDecoderToReuse);
    try {
      _avroRecordToReuse = _datumReader.read(_avroRecordToReuse, _binaryDecoderToReuse);
    } catch (IOException e) {
      LOGGER.error("Caught exception while reading message using schema: {}", _avroSchema, e);
      return null;
    }
    return _avroRecordExtractor.extract(_avroRecordToReuse, destination);
  }
}
