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
package org.apache.pinot.core.realtime.impl.fakestream;

import java.io.IOException;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.realtime.stream.AvroRecordToPinotRowGenerator;
import org.apache.pinot.core.realtime.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * StreamMessageDecoder implementation for fake stream
 */
public class FakeStreamMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(FakeStreamMessageDecoder.class);

  private org.apache.avro.Schema _avroSchema;
  private DatumReader<GenericData.Record> _datumReader;
  private AvroRecordToPinotRowGenerator _avroRecordConverter;
  private BinaryDecoder _binaryDecoderToReuse;
  private GenericData.Record _avroRecordToReuse;

  @Override
  public void init(Map props, Schema indexingSchema, String topicName) throws Exception {
    _avroSchema = new org.apache.avro.Schema.Parser().parse(FakeStreamConfigUtils.getAvroSchemaFile());
    _datumReader = new GenericDatumReader<>(_avroSchema);
    _avroRecordConverter = new AvroRecordToPinotRowGenerator(indexingSchema);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    return decode(payload, 0, payload.length, destination);
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    _binaryDecoderToReuse = DecoderFactory.get().binaryDecoder(payload, offset, length, _binaryDecoderToReuse);
    try {
      _avroRecordToReuse = _datumReader.read(_avroRecordToReuse, _binaryDecoderToReuse);
    } catch (IOException e) {
      LOGGER.error("Caught exception while reading message using schema: {}", _avroSchema, e);
      return null;
    }
    return _avroRecordConverter.transform(_avroRecordToReuse, destination);
  }
}
