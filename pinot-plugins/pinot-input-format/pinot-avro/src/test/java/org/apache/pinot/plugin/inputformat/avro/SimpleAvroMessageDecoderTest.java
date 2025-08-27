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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SimpleAvroMessageDecoderTest {

  private static final String AVRO_SCHEMA_JSON =
      "{\"type\": \"record\", \"name\": \"TestRecord\", \"fields\": ["
          + "{\"name\": \"id\", \"type\": \"long\"},"
          + "{\"name\": \"name\", \"type\": \"string\"},"
          + "{\"name\": \"value\", \"type\": \"double\"}"
          + "]}";

  @Test
  public void testInitWithValidSchema() throws Exception {
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema", AVRO_SCHEMA_JSON);
    Set<String> fieldsToRead = Set.of("id", "name", "value");

    decoder.init(props, fieldsToRead, "test-topic");

    // Test that decoder was initialized successfully
    Assert.assertNotNull(decoder);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testInitWithoutSchema() throws Exception {
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    Set<String> fieldsToRead = Set.of("id", "name");

    decoder.init(props, fieldsToRead, "test-topic");
  }

  @Test
  public void testDecodeValidMessage() throws Exception {
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema", AVRO_SCHEMA_JSON);
    Set<String> fieldsToRead = Set.of("id", "name", "value");

    decoder.init(props, fieldsToRead, "test-topic");

    // Create a test Avro record
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 123L);
    record.put("name", "test-name");
    record.put("value", 45.67);

    // Serialize the record
    byte[] payload = serializeAvroRecord(record, schema);

    // Decode the message
    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(payload, destination);

    // Verify the decoded result
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getValue("id"), 123L);
    Assert.assertEquals(result.getValue("name"), "test-name");
    Assert.assertEquals(result.getValue("value"), 45.67);
  }

  @Test
  public void testDecodeWithOffsetAndLength() throws Exception {
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema", AVRO_SCHEMA_JSON);
    Set<String> fieldsToRead = Set.of("id", "name");

    decoder.init(props, fieldsToRead, "test-topic");

    // Create a test Avro record
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 456L);
    record.put("name", "offset-test");
    record.put("value", 78.90);

    // Serialize the record
    byte[] fullPayload = serializeAvroRecord(record, schema);

    // Create a larger payload with the Avro data at an offset
    byte[] largerPayload = new byte[fullPayload.length + 10];
    System.arraycopy(fullPayload, 0, largerPayload, 5, fullPayload.length);

    // Decode with offset and length
    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(largerPayload, 5, fullPayload.length, destination);

    // Verify the decoded result
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getValue("id"), 456L);
    Assert.assertEquals(result.getValue("name"), "offset-test");
  }

  @Test
  public void testDecodeNullPayload() throws Exception {
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema", AVRO_SCHEMA_JSON);
    Set<String> fieldsToRead = Set.of("id", "name");

    decoder.init(props, fieldsToRead, "test-topic");

    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(null, destination);

    Assert.assertNull(result);
  }

  @Test
  public void testDecodeEmptyPayload() throws Exception {
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema", AVRO_SCHEMA_JSON);
    Set<String> fieldsToRead = Set.of("id", "name");

    decoder.init(props, fieldsToRead, "test-topic");

    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(new byte[0], destination);

    Assert.assertNull(result);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDecodeInvalidPayload() throws Exception {
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema", AVRO_SCHEMA_JSON);
    Set<String> fieldsToRead = Set.of("id", "name");

    decoder.init(props, fieldsToRead, "test-topic");

    // Try to decode invalid bytes
    GenericRow destination = new GenericRow();
    decoder.decode("invalid data".getBytes(StandardCharsets.UTF_8), destination);
  }

  @Test
  public void testCustomRecordExtractor() throws Exception {
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    Map<String, String> props = new HashMap<>();
    props.put("schema", AVRO_SCHEMA_JSON);
    props.put("recordExtractorClass", AvroRecordExtractor.class.getName());
    props.put("recordExtractorConfigClass", AvroRecordExtractorConfig.class.getName());
    Set<String> fieldsToRead = Set.of("id", "name");

    decoder.init(props, fieldsToRead, "test-topic");

    // Create a test Avro record
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA_JSON);
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 789L);
    record.put("name", "custom-extractor-test");
    record.put("value", 12.34);

    // Serialize the record
    byte[] payload = serializeAvroRecord(record, schema);

    // Decode the message
    GenericRow destination = new GenericRow();
    GenericRow result = decoder.decode(payload, destination);

    // Verify the decoded result
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getValue("id"), 789L);
    Assert.assertEquals(result.getValue("name"), "custom-extractor-test");
  }

  private byte[] serializeAvroRecord(GenericRecord record, Schema schema) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);

    datumWriter.write(record, encoder);
    encoder.flush();
    outputStream.close();

    return outputStream.toByteArray();
  }
}
