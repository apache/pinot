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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class MicroBatchProtocolTest {

  @Test
  public void testCreateUriMessage() throws IOException {
    String uri = "file:///tmp/test.avro";
    byte[] message = MicroBatchProtocol.createUriMessage(uri, MicroBatchPayloadV1.Format.AVRO, 100);

    assertNotNull(message);
    // First byte should be version
    assertEquals(message[0], MicroBatchProtocol.VERSION_1);
    // Rest should be JSON payload starting with '{'
    assertEquals(message[1], '{');

    // Parse and verify
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);
    assertEquals(protocol.getVersion(), MicroBatchProtocol.VERSION_1);
    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.URI);
    assertEquals(protocol.getFormat(), MicroBatchPayloadV1.Format.AVRO);
    assertEquals(protocol.getUri(), uri);
    assertEquals(protocol.getNumRecords(), 100);
  }

  @Test
  public void testCreateUriMessageWithDifferentFormats() throws IOException {
    // Test AVRO
    byte[] avroMessage = MicroBatchProtocol.createUriMessage(
        "s3://bucket/test.avro", MicroBatchPayloadV1.Format.AVRO, 100);
    MicroBatchProtocol avroProtocol = MicroBatchProtocol.parse(avroMessage);
    assertEquals(avroProtocol.getFormat(), MicroBatchPayloadV1.Format.AVRO);

    // Test PARQUET
    byte[] parquetMessage = MicroBatchProtocol.createUriMessage(
        "hdfs://cluster/test.parquet", MicroBatchPayloadV1.Format.PARQUET, 200);
    MicroBatchProtocol parquetProtocol = MicroBatchProtocol.parse(parquetMessage);
    assertEquals(parquetProtocol.getFormat(), MicroBatchPayloadV1.Format.PARQUET);

    // Test JSON
    byte[] jsonMessage = MicroBatchProtocol.createUriMessage(
        "gs://bucket/test.json", MicroBatchPayloadV1.Format.JSON, 300);
    MicroBatchProtocol jsonProtocol = MicroBatchProtocol.parse(jsonMessage);
    assertEquals(jsonProtocol.getFormat(), MicroBatchPayloadV1.Format.JSON);
  }

  @Test
  public void testCreateDataMessage() throws IOException {
    byte[] testData = "test data content".getBytes(StandardCharsets.UTF_8);
    byte[] message = MicroBatchProtocol.createDataMessage(testData, MicroBatchPayloadV1.Format.AVRO, 50);

    assertNotNull(message);
    assertEquals(message[0], MicroBatchProtocol.VERSION_1);

    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);
    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.DATA);
    assertEquals(protocol.getFormat(), MicroBatchPayloadV1.Format.AVRO);
    assertEquals(new String(protocol.getData(), StandardCharsets.UTF_8), "test data content");
    assertEquals(protocol.getNumRecords(), 50);
  }

  @Test
  public void testCreateUriMessageWithNumRecords() throws IOException {
    String uri = "s3://bucket/batch.avro";
    byte[] message = MicroBatchProtocol.createUriMessage(uri, MicroBatchPayloadV1.Format.AVRO, 1000);

    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);
    assertEquals(protocol.getNumRecords(), 1000);
    assertEquals(protocol.getUri(), uri);
  }

  @Test
  public void testCreateDataMessageWithNumRecords() throws IOException {
    byte[] testData = "test".getBytes(StandardCharsets.UTF_8);
    byte[] message = MicroBatchProtocol.createDataMessage(testData, MicroBatchPayloadV1.Format.AVRO, 500);

    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);
    assertEquals(protocol.getNumRecords(), 500);
  }

  @Test
  public void testParseUriMessage() throws IOException {
    String uri = "file:///tmp/batch-123.avro";
    byte[] message = MicroBatchProtocol.createUriMessage(uri, MicroBatchPayloadV1.Format.AVRO, 100);

    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    assertEquals(protocol.getVersion(), MicroBatchProtocol.VERSION_1);
    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.URI);
    assertEquals(protocol.getFormat(), MicroBatchPayloadV1.Format.AVRO);
    assertEquals(protocol.getUri(), uri);
    assertNull(protocol.getData());
  }

  @Test
  public void testParseDataMessage() throws IOException {
    byte[] testData = "test data".getBytes(StandardCharsets.UTF_8);
    byte[] message = MicroBatchProtocol.createDataMessage(testData, MicroBatchPayloadV1.Format.AVRO, 10);

    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    assertEquals(protocol.getVersion(), MicroBatchProtocol.VERSION_1);
    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.DATA);
    assertEquals(protocol.getFormat(), MicroBatchPayloadV1.Format.AVRO);
    assertNull(protocol.getUri());
    assertNotNull(protocol.getData());
    assertEquals(new String(protocol.getData(), StandardCharsets.UTF_8), "test data");
  }

  @Test
  public void testIsProtocol() throws IOException {
    // Valid protocol message
    byte[] validMessage = MicroBatchProtocol.createUriMessage(
        "file:///tmp/test.avro", MicroBatchPayloadV1.Format.AVRO, 100);
    assertTrue(MicroBatchProtocol.isProtocol(validMessage));

    // Invalid - no version byte (raw JSON)
    byte[] rawJson = "{\"type\":\"uri\"}".getBytes(StandardCharsets.UTF_8);
    assertFalse(MicroBatchProtocol.isProtocol(rawJson));

    // Invalid - unknown version
    byte[] unknownVersion = createRawMessage(99, "{\"type\":\"uri\"}".getBytes());
    assertFalse(MicroBatchProtocol.isProtocol(unknownVersion));

    // Invalid - too short
    assertFalse(MicroBatchProtocol.isProtocol(new byte[]{1}));
    assertFalse(MicroBatchProtocol.isProtocol(null));
  }

  @Test
  public void testParseMissingType() {
    byte[] message = createRawMessage(1, "{\"format\":\"avro\",\"uri\":\"file:///tmp/test.avro\"}".getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception for missing type");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("type"));
    }
  }

  @Test
  public void testParseMissingFormat() {
    byte[] message = createRawMessage(1, "{\"type\":\"uri\",\"uri\":\"file:///tmp/test.avro\"}".getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception for missing format");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("format"));
    }
  }

  @Test
  public void testParseUnsupportedVersion() {
    String json = "{\"type\":\"uri\",\"format\":\"avro\",\"uri\":\"file:///tmp/test.avro\"}";
    byte[] message = createRawMessage(99, json.getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception for unsupported version");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Unsupported MicroBatch protocol version: 99"));
    }
  }

  @Test
  public void testParseInvalidType() {
    String json = "{\"type\":\"invalid\",\"format\":\"avro\",\"uri\":\"file:///tmp/test.avro\"}";
    byte[] message = createRawMessage(1, json.getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception for invalid type");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid type: invalid"));
    }
  }

  @Test
  public void testParseInvalidFormat() {
    String json = "{\"type\":\"uri\",\"format\":\"xml\",\"uri\":\"file:///tmp/test.avro\"}";
    byte[] message = createRawMessage(1, json.getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception for invalid format");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Invalid format: xml"));
    }
  }

  @Test
  public void testParseUriTypeMissingUri() {
    byte[] message = createRawMessage(1,
        "{\"type\":\"uri\",\"format\":\"avro\",\"numRecords\":100}".getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception for missing uri when type=uri");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("uri"));
    }
  }

  @Test
  public void testParseDataTypeMissingData() {
    byte[] message = createRawMessage(1,
        "{\"type\":\"data\",\"format\":\"avro\",\"numRecords\":100}".getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception for missing data when type=data");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("data"));
    }
  }

  @Test
  public void testParseDataWithInvalidBase64() {
    String json = "{\"type\":\"data\",\"format\":\"avro\",\"data\":\"not-valid-base64!!!\"}";
    byte[] message = createRawMessage(1, json.getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception for invalid base64");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("base64") || e instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testParseWithS3Uri() throws IOException {
    String s3Uri = "s3://my-bucket/path/to/batch-456.avro";
    byte[] message = MicroBatchProtocol.createUriMessage(s3Uri, MicroBatchPayloadV1.Format.AVRO, 456);

    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    assertEquals(protocol.getUri(), s3Uri);
    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.URI);
  }

  @Test
  public void testParseWithHdfsUri() throws IOException {
    String hdfsUri = "hdfs://namenode:9000/user/pinot/batch-789.parquet";
    byte[] message = MicroBatchProtocol.createUriMessage(hdfsUri, MicroBatchPayloadV1.Format.PARQUET, 789);

    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    assertEquals(protocol.getUri(), hdfsUri);
    assertEquals(protocol.getFormat(), MicroBatchPayloadV1.Format.PARQUET);
  }

  @Test
  public void testParseWithGcsUri() throws IOException {
    String gcsUri = "gs://my-gcs-bucket/data/batch-999.json";
    byte[] message = MicroBatchProtocol.createUriMessage(gcsUri, MicroBatchPayloadV1.Format.JSON, 999);

    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    assertEquals(protocol.getUri(), gcsUri);
    assertEquals(protocol.getFormat(), MicroBatchPayloadV1.Format.JSON);
  }

  @Test
  public void testParseLargeBinaryData() throws IOException {
    // Test with 1MB of data
    byte[] largeData = new byte[1024 * 1024];
    for (int i = 0; i < largeData.length; i++) {
      largeData[i] = (byte) (i % 256);
    }

    byte[] message = MicroBatchProtocol.createDataMessage(largeData, MicroBatchPayloadV1.Format.AVRO, 10000);
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.DATA);
    assertEquals(protocol.getData().length, largeData.length);

    // Verify data integrity
    for (int i = 0; i < 1000; i++) {
      assertEquals(protocol.getData()[i], largeData[i]);
    }
  }

  @Test
  public void testCaseInsensitiveTypeParsing() throws IOException {
    // Type should be case-insensitive
    String json = "{\"type\":\"URI\",\"format\":\"avro\",\"uri\":\"file:///tmp/test.avro\",\"numRecords\":100}";
    byte[] message = createRawMessage(1, json.getBytes());
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);
    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.URI);

    String base64Data = Base64.getEncoder().encodeToString("test".getBytes());
    String json2 = "{\"type\":\"Data\",\"format\":\"avro\",\"data\":\"" + base64Data + "\",\"numRecords\":50}";
    message = createRawMessage(1, json2.getBytes());
    protocol = MicroBatchProtocol.parse(message);
    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.DATA);
  }

  @Test
  public void testCaseInsensitiveFormatParsing() throws IOException {
    String json = "{\"type\":\"uri\",\"format\":\"AVRO\",\"uri\":\"file:///tmp/test.avro\",\"numRecords\":100}";
    byte[] message = createRawMessage(1, json.getBytes());
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);
    assertEquals(protocol.getFormat(), MicroBatchPayloadV1.Format.AVRO);

    String json2 = "{\"type\":\"uri\",\"format\":\"Parquet\",\"uri\":\"file:///tmp/test.parquet\",\"numRecords\":200}";
    message = createRawMessage(1, json2.getBytes());
    protocol = MicroBatchProtocol.parse(message);
    assertEquals(protocol.getFormat(), MicroBatchPayloadV1.Format.PARQUET);
  }

  @Test
  public void testEmptyData() throws IOException {
    // Empty data with numRecords=1 (edge case - file exists but has no actual content)
    byte[] emptyData = new byte[0];
    byte[] message = MicroBatchProtocol.createDataMessage(emptyData, MicroBatchPayloadV1.Format.AVRO, 1);

    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.DATA);
    assertNotNull(protocol.getData());
    assertEquals(protocol.getData().length, 0);
  }

  @Test
  public void testRoundTripUriMessage() throws IOException {
    String originalUri = "abfs://container@account.dfs.core.windows.net/path/batch.avro";
    byte[] message = MicroBatchProtocol.createUriMessage(
        originalUri, MicroBatchPayloadV1.Format.AVRO, 500);
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    // Create another message from the parsed protocol
    byte[] message2 = MicroBatchProtocol.createUriMessage(
        protocol.getUri(), protocol.getFormat(), protocol.getNumRecords());
    MicroBatchProtocol protocol2 = MicroBatchProtocol.parse(message2);

    assertEquals(protocol2.getUri(), originalUri);
    assertEquals(protocol2.getType(), MicroBatchPayloadV1.Type.URI);
    assertEquals(protocol2.getFormat(), MicroBatchPayloadV1.Format.AVRO);
    assertEquals(protocol2.getNumRecords(), 500);
  }

  @Test
  public void testRoundTripDataMessage() throws IOException {
    byte[] originalData = "original test data with special chars: !@#$%^&*()".getBytes(
        StandardCharsets.UTF_8);
    byte[] message = MicroBatchProtocol.createDataMessage(
        originalData, MicroBatchPayloadV1.Format.JSON, 1);
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    // Create another message from the parsed protocol
    byte[] message2 = MicroBatchProtocol.createDataMessage(
        protocol.getData(), protocol.getFormat(), protocol.getNumRecords());
    MicroBatchProtocol protocol2 = MicroBatchProtocol.parse(message2);

    assertEquals(new String(protocol2.getData(), StandardCharsets.UTF_8),
        "original test data with special chars: !@#$%^&*()");
    assertEquals(protocol2.getType(), MicroBatchPayloadV1.Type.DATA);
    assertEquals(protocol2.getFormat(), MicroBatchPayloadV1.Format.JSON);
  }

  @Test
  public void testGetPayloadV1() throws IOException {
    byte[] message = MicroBatchProtocol.createUriMessage("s3://bucket/file.avro", MicroBatchPayloadV1.Format.AVRO, 100);
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    MicroBatchPayloadV1 payload = protocol.getPayloadV1();
    assertNotNull(payload);
    assertEquals(payload.getType(), MicroBatchPayloadV1.Type.URI);
    assertEquals(payload.getFormat(), MicroBatchPayloadV1.Format.AVRO);
    assertEquals(payload.getUri(), "s3://bucket/file.avro");
    assertEquals(payload.getNumRecords(), 100);
  }

  @Test
  public void testMessageTooShort() {
    try {
      MicroBatchProtocol.parse(new byte[]{1});
      fail("Should throw exception for message too short");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("at least 2 bytes"));
    }
  }

  @Test
  public void testNullMessage() {
    try {
      MicroBatchProtocol.parse(null);
      fail("Should throw exception for null message");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("at least 2 bytes"));
    }
  }

  @Test
  public void testUnknownFieldsIgnored() throws IOException {
    // JSON with extra unknown fields should be ignored
    String json = "{\"type\":\"uri\",\"format\":\"avro\",\"uri\":\"file:///test.avro\","
        + "\"unknownField\":\"value\",\"anotherUnknown\":123,\"numRecords\":100}";
    byte[] message = createRawMessage(1, json.getBytes());
    MicroBatchProtocol protocol = MicroBatchProtocol.parse(message);

    assertEquals(protocol.getType(), MicroBatchPayloadV1.Type.URI);
    assertEquals(protocol.getUri(), "file:///test.avro");
  }

  @Test
  public void testUriTypeWithDataFieldFails() {
    // URI type should not have 'data' field set
    String base64Data = Base64.getEncoder().encodeToString("test".getBytes());
    String json = "{\"type\":\"uri\",\"format\":\"avro\",\"uri\":\"file:///test.avro\","
        + "\"data\":\"" + base64Data + "\",\"numRecords\":100}";
    byte[] message = createRawMessage(1, json.getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception when both uri and data are set for type=uri");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("'data' must not be set when type=uri"));
    }
  }

  @Test
  public void testDataTypeWithUriFieldFails() {
    // DATA type should not have 'uri' field set
    String base64Data = Base64.getEncoder().encodeToString("test".getBytes());
    String json = "{\"type\":\"data\",\"format\":\"avro\",\"data\":\"" + base64Data + "\","
        + "\"uri\":\"file:///test.avro\",\"numRecords\":100}";
    byte[] message = createRawMessage(1, json.getBytes());
    try {
      MicroBatchProtocol.parse(message);
      fail("Should throw exception when both uri and data are set for type=data");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("'uri' must not be set when type=data"));
    }
  }

  /**
   * Helper to create raw protocol message with version byte + payload.
   */
  private byte[] createRawMessage(int version, byte[] payload) {
    try {
      ByteArrayOutputStream out = new ByteArrayOutputStream(1 + payload.length);
      out.write(version);
      out.write(payload);
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
