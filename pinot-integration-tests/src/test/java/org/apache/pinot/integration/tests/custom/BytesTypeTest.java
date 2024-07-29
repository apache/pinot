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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.UUID;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.common.function.scalar.DataTypeConversionFunctions;
import org.apache.pinot.common.function.scalar.StringFunctions;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(suiteName = "CustomClusterIntegrationTest")
public class BytesTypeTest extends CustomDataQueryClusterIntegrationTest {

  protected static final String DEFAULT_TABLE_NAME = "BytesTypeTest";
  private static final String FIXED_HEX_STRIING_VALUE = "968a3c6a5eeb42168bae0e895034a26f";

  private static final int NUM_TOTAL_DOCS = 1000;
  private static final String HEX_STR = "hexStr";
  private static final String HEX_BYTES = "hexBytes";
  private static final String UUID_STR = "uuidStr";
  private static final String UUID_BYTES = "uuidBytes";
  private static final String UTF8_STR = "utf8Str";
  private static final String UTF8_BYTES = "utf8Bytes";
  private static final String ASCII_STR = "asciiStr";
  private static final String ASCII_BYTES = "asciiBytes";
  private static final String BASE64_STR = "base64Str";
  private static final String BASE64_BYTES = "base64Bytes";
  private static final String RANDOM_STR = "randomStr";
  private static final String RANDOM_BYTES = "randomBytes";
  private static final String FIXED_STRING = "fixedString";
  private static final String FIXED_BYTES = "fixedBytes";

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(HEX_STR, FieldSpec.DataType.STRING)
        .addSingleValueDimension(HEX_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(UUID_STR, FieldSpec.DataType.STRING)
        .addSingleValueDimension(UUID_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(UTF8_STR, FieldSpec.DataType.STRING)
        .addSingleValueDimension(UTF8_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(ASCII_STR, FieldSpec.DataType.STRING)
        .addSingleValueDimension(ASCII_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(BASE64_STR, FieldSpec.DataType.STRING)
        .addSingleValueDimension(BASE64_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(RANDOM_STR, FieldSpec.DataType.STRING)
        .addSingleValueDimension(RANDOM_BYTES, FieldSpec.DataType.BYTES)
        .addSingleValueDimension(FIXED_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(FIXED_BYTES, FieldSpec.DataType.BYTES)
        .build();
  }

  @Override
  protected long getCountStarResult() {
    return NUM_TOTAL_DOCS;
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(HEX_STR, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(HEX_BYTES, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null),
        new org.apache.avro.Schema.Field(UUID_STR, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(UUID_BYTES, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null),
        new org.apache.avro.Schema.Field(UTF8_STR, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(UTF8_BYTES, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null),
        new org.apache.avro.Schema.Field(ASCII_STR, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(ASCII_BYTES, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null),
        new org.apache.avro.Schema.Field(BASE64_STR, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(BASE64_BYTES, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null),
        new org.apache.avro.Schema.Field(RANDOM_STR, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(RANDOM_BYTES, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null),
        new org.apache.avro.Schema.Field(FIXED_STRING,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(FIXED_BYTES, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.BYTES),
            null, null)

    ));

    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_TOTAL_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        byte[] bytes = newRandomBytes(RANDOM.nextInt(100) * 2 + 2);
        String hexString = Hex.encodeHexString(bytes);
        record.put(HEX_STR, hexString);
        record.put(HEX_BYTES, ByteBuffer.wrap(bytes));
        UUID uuid = java.util.UUID.randomUUID();
        record.put(UUID_STR, uuid.toString());
        record.put(UUID_BYTES, ByteBuffer.wrap(StringFunctions.toUUIDBytes(uuid.toString())));
        String utf8String = "utf8String" + i;
        record.put(UTF8_STR, utf8String);
        record.put(UTF8_BYTES, ByteBuffer.wrap(StringFunctions.toUtf8(utf8String)));
        String asciiString = "asciiString" + i;
        record.put(ASCII_STR, asciiString);
        record.put(ASCII_BYTES, ByteBuffer.wrap(StringFunctions.toAscii(asciiString)));
        String base64String = newRandomBase64String();
        record.put(BASE64_STR, base64String);
        record.put(BASE64_BYTES, ByteBuffer.wrap(StringFunctions.fromBase64(base64String)));
        byte[] randomBytes = newRandomBytes();
        record.put(RANDOM_STR, new String(randomBytes));
        record.put(RANDOM_BYTES, ByteBuffer.wrap(randomBytes));
        record.put(FIXED_STRING, FIXED_HEX_STRIING_VALUE);
        record.put(FIXED_BYTES, ByteBuffer.wrap(DataTypeConversionFunctions.hexToBytes(FIXED_HEX_STRIING_VALUE)));
        fileWriter.append(record);
      }
    }

    return avroFile;
  }

  private static String newRandomBase64String() {
    byte[] bytes = newRandomBytes(RANDOM.nextInt(100) * 2 + 2);
    return Base64.getEncoder().encodeToString(bytes);
  }

  private static byte[] newRandomBytes() {
    return newRandomBytes(RANDOM.nextInt(100));
  }

  private static byte[] newRandomBytes(int length) {
    byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (Math.random() * 256);
    }
    return bytes;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGetHexagonAddress(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("Select bytesToHex(%s), %s, hexToBytes(%s), %s from %s", HEX_BYTES, HEX_STR, HEX_STR, HEX_BYTES,
            getTableName());
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asText(), rows.get(i).get(1).asText());
      Assert.assertEquals(rows.get(i).get(2).asText(), rows.get(i).get(3).asText());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGetUUID(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("Select fromUUIDBytes(%s), %s, toUUIDBytes(%s), %s from %s", UUID_BYTES, UUID_STR, UUID_STR,
            UUID_BYTES,
            getTableName());
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asText(), rows.get(i).get(1).asText());
      Assert.assertEquals(rows.get(i).get(2).asText(), rows.get(i).get(3).asText());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGetUTF8(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("Select fromUtf8(%s), %s, toUtf8(%s), %s from %s", UTF8_BYTES, UTF8_STR, UTF8_STR, UTF8_BYTES,
            getTableName());
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asText(), rows.get(i).get(1).asText());
      Assert.assertEquals(rows.get(i).get(2).asText(), rows.get(i).get(3).asText());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGetASCII(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("Select fromAscii(%s), %s, toAscii(%s), %s from %s", ASCII_BYTES, ASCII_STR, ASCII_STR,
            ASCII_BYTES,
            getTableName());
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asText(), rows.get(i).get(1).asText());
      Assert.assertEquals(rows.get(i).get(2).asText(), rows.get(i).get(3).asText());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGetBase64(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("Select toBase64(%s), %s, fromBase64(%s), %s from %s", BASE64_BYTES, BASE64_STR, BASE64_STR,
            BASE64_BYTES,
            getTableName());
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asText(), rows.get(i).get(1).asText());
      Assert.assertEquals(rows.get(i).get(2).asText(), rows.get(i).get(3).asText());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGetRandomBytes(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("Select fromBytes(%s, 'UTF-8'), %s, toBytes(%s, 'UTF-8'), %s from %s", UTF8_BYTES, UTF8_STR,
            UTF8_STR, UTF8_BYTES, getTableName());
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asText(), rows.get(i).get(1).asText());
      Assert.assertEquals(rows.get(i).get(2).asText(), rows.get(i).get(3).asText());
    }

    query =
        String.format("Select fromBytes(%s, 'ASCII'), %s, toBytes(%s, 'ASCII'), %s from %s", ASCII_BYTES, ASCII_STR,
            ASCII_STR, ASCII_BYTES, getTableName());
    pinotResponse = postQuery(query);
    rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asText(), rows.get(i).get(1).asText());
      Assert.assertEquals(rows.get(i).get(2).asText(), rows.get(i).get(3).asText());
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testStringAndBytesInPredicate(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // String predicate
    String query =
        String.format("Select count(*) from %s WHERE %s = '%s'", getTableName(), FIXED_STRING, FIXED_HEX_STRIING_VALUE);
    JsonNode pinotResponse = postQuery(query);
    JsonNode rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asLong(), NUM_TOTAL_DOCS);
    }

    // Bytes predicate, convert literal string to bytes
    query =
        String.format("Select count(*) from %s WHERE %s = hexToBytes('%s')", getTableName(), FIXED_BYTES,
            FIXED_HEX_STRIING_VALUE);
    pinotResponse = postQuery(query);
    rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asLong(), NUM_TOTAL_DOCS);
    }

    // Bytes predicate, convert column to hex string to compare with a literal string
    query =
        String.format("Select count(*) from %s WHERE bytesToHex(%s) = '%s'", getTableName(), FIXED_BYTES,
            FIXED_HEX_STRIING_VALUE);
    pinotResponse = postQuery(query);
    rows = pinotResponse.get("resultTable").get("rows");
    for (int i = 0; i < rows.size(); i++) {
      Assert.assertEquals(rows.get(i).get(0).asLong(), NUM_TOTAL_DOCS);
    }
  }
}
