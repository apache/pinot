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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class SimpleAvroMessageDecoderTest {
  private Schema _schema;
  private byte[] _encodedRecord;
  private GenericRow _destination;

  @BeforeClass
  public void setUp()
      throws Exception {
    String schemaStr = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
        + "    {\"name\": \"name\", \"type\": \"string\"}\n"
        + "  ]\n"
        + "}";
    _schema = new Schema.Parser().parse(schemaStr);

    GenericData.Record record = new GenericData.Record(_schema);
    record.put("id", 42);
    record.put("name", "alice");

    GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(_schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    var encoder = EncoderFactory.get().binaryEncoder(baos, null);
    writer.write(record, encoder);
    encoder.flush();
    _encodedRecord = baos.toByteArray();

    _destination = new GenericRow();
  }

  private Map<String, String> baseProps() {
    Map<String, String> props = new HashMap<>();
    props.put("schema", _schema.toString());
    return props;
  }

  @Test
  public void testDecodeWithoutHeader()
      throws Exception {
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    decoder.init(baseProps(), Set.of(), "topic");
    GenericRow row = decoder.decode(_encodedRecord, _destination);
    Assert.assertEquals(row.getValue("id"), 42);
    Assert.assertEquals(row.getValue("name"), "alice");
  }

  @Test
  public void testDecodeWithLeadingBytesStripped()
      throws Exception {
    byte[] header = new byte[]{1, 2, 3, 4};
    byte[] payloadWithHeader = new byte[header.length + _encodedRecord.length];
    System.arraycopy(header, 0, payloadWithHeader, 0, header.length);
    System.arraycopy(_encodedRecord, 0, payloadWithHeader, header.length, _encodedRecord.length);

    Map<String, String> props = baseProps();
    props.put("leading.bytes.to.strip", String.valueOf(header.length));

    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    decoder.init(props, Set.of(), "topic");
    GenericRow row = decoder.decode(payloadWithHeader, _destination);
    Assert.assertEquals(row.getValue("id"), 42);
    Assert.assertEquals(row.getValue("name"), "alice");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testNegativeLeadingBytesRejected()
      throws Exception {
    Map<String, String> props = baseProps();
    props.put("leading.bytes.to.strip", "-1");
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    decoder.init(props, Set.of(), "topic");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testLeadingBytesExceedsLength()
      throws Exception {
    Map<String, String> props = baseProps();
    props.put("leading.bytes.to.strip", "10");
    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    decoder.init(props, Set.of(), "topic");
    decoder.decode(new byte[]{0, 1, 2, 3, 4}, new GenericRow());
  }

  @Test
  public void testDecodeWithOffsetAndLength()
      throws Exception {
    byte[] prefix = new byte[]{9, 9};
    byte[] suffix = new byte[]{8, 8, 8};
    byte[] wrapped = new byte[prefix.length + _encodedRecord.length + suffix.length];
    System.arraycopy(prefix, 0, wrapped, 0, prefix.length);
    System.arraycopy(_encodedRecord, 0, wrapped, prefix.length, _encodedRecord.length);
    System.arraycopy(suffix, 0, wrapped, prefix.length + _encodedRecord.length, suffix.length);

    SimpleAvroMessageDecoder decoder = new SimpleAvroMessageDecoder();
    decoder.init(baseProps(), Set.of(), "topic");
    GenericRow row = decoder.decode(wrapped, prefix.length, _encodedRecord.length, _destination);
    Assert.assertEquals(row.getValue("id"), 42);
    Assert.assertEquals(row.getValue("name"), "alice");
  }
}
