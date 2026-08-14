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
package org.apache.pinot.plugin.inputformat.parquet;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.variant.ImmutableMetadata;
import org.apache.parquet.variant.Variant;
import org.apache.parquet.variant.VariantArrayBuilder;
import org.apache.parquet.variant.VariantBuilder;
import org.apache.parquet.variant.VariantObjectBuilder;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.VariantEnvelope;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// End-to-end coverage for Parquet `VARIANT(1)` reconstruction and reader selection.
public class ParquetVariantRecordReaderTest {
  private static final String VARIANT_FIELD = "variant_col";

  private final File _tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());

  @AfterClass
  public void cleanUp() {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testUnshreddedShreddedNullProjectionAndRewind()
      throws Exception {
    Variant objectVariant = objectVariant("name", "pinot");
    File dataFile = writeScalarVariantFile(objectVariant);
    assertFalse(ParquetUtils.hasAvroSchemaInFileMetadata(new Path(dataFile.getAbsolutePath())));

    assertScalarRows(new ParquetNativeRecordReader(), dataFile);
    ParquetRecordReader autoSelectingReader = new ParquetRecordReader();
    assertScalarRows(autoSelectingReader, dataFile);
    assertFalse(autoSelectingReader.useAvroParquetRecordReader());
  }

  @Test
  public void testPartiallyShreddedObject()
      throws Exception {
    Variant expected = objectVariant("cold", "kept", "hot", 7);
    VariantBuilder baseBuilder = new VariantBuilder(new ImmutableMetadata(expected.getMetadataBuffer()));
    VariantObjectBuilder baseObject = baseBuilder.startObject();
    baseObject.appendKey("cold");
    baseObject.appendString("kept");
    baseObject.appendKey("hot");
    baseObject.appendInt(1);
    baseBuilder.endObject();
    Variant base = baseBuilder.build();

    File dataFile = writePartiallyShreddedFile(expected.getMetadataBuffer(), base.getValueBuffer());
    assertShreddedObjectRows(new ParquetNativeRecordReader(), dataFile);
  }

  @Test
  public void testUnshreddedValuePreservesEncodedBuffers() {
    Variant expected = objectVariant("name", "pinot");
    byte[] expectedMetadata = remainingBytes(expected.getMetadataBuffer());
    byte[] metadataBacking = addSentinels(expectedMetadata);
    byte[] expectedValue = remainingBytes(expected.getValueBuffer());
    byte[] valueBacking = addSentinels(expectedValue);
    MessageType schema = MessageTypeParser.parseMessageType(
        "message direct_variant {"
            + " optional group variant_col (VARIANT(1)) {"
            + "   required binary metadata;"
            + "   optional binary value;"
            + "   optional int32 typed_value;"
            + " }"
            + "}");
    Group variantGroup = new SimpleGroupFactory(schema).newGroup().addGroup(VARIANT_FIELD)
        .append("metadata", Binary.fromConstantByteArray(metadataBacking, 1, expectedMetadata.length))
        .append("value", Binary.fromConstantByteArray(valueBacking, 1, expectedValue.length));

    ParquetVariantConverter converter =
        ParquetVariantConverter.createTopLevelVariantConverters(schema)[schema.getFieldIndex(VARIANT_FIELD)];
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(converter.convert(variantGroup));
    assertEquals(remainingBytes(decoded.getMetadata()), expectedMetadata);
    assertEquals(remainingBytes(decoded.getValue()), expectedValue);
  }

  @Test
  public void testUnshreddedNonMonotonicObjectOffsetsArePreserved()
      throws Exception {
    Variant expected = nonMonotonicObjectVariant();
    File dataFile = writeScalarVariantFile(expected, "non-monotonic-object-offsets.parquet", Map.of());
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      reader.init(dataFile, null, null);
      List<GenericRow> rows = readAll(reader);
      byte[] actualEnvelope = (byte[]) rows.get(2).getValue(VARIANT_FIELD);
      byte[] expectedEnvelope = VariantEnvelope.encode(expected.getMetadataBuffer(), expected.getValueBuffer());

      assertTrue(Arrays.equals(actualEnvelope, expectedEnvelope),
          "Unshredded ingestion must preserve producer-owned metadata/value bytes exactly");
      assertEquals(VariantUtils.variantGet(actualEnvelope, "$.a", "INT"), 1);
      assertEquals(VariantUtils.variantGet(actualEnvelope, "$.b", "INT"), 2);
      assertEquals(VariantUtils.variantGet(actualEnvelope, "$.c", "INT"), 3);
    }
  }

  @Test
  public void testUnshreddedValueSupportsDirectAndReadOnlyBuffers() {
    Variant expected = objectVariant("name", "pinot");
    byte[] expectedMetadata = remainingBytes(expected.getMetadataBuffer());
    byte[] metadataBacking = addSentinels(expectedMetadata);
    byte[] expectedValue = remainingBytes(expected.getValueBuffer());
    byte[] valueBacking = addSentinels(expectedValue);
    MessageType schema = MessageTypeParser.parseMessageType(
        "message direct_variant {"
            + " optional group variant_col (VARIANT(1)) {"
            + "   required binary metadata;"
            + "   optional binary value;"
            + " }"
            + "}");

    ByteBuffer directMetadata = ByteBuffer.allocateDirect(metadataBacking.length);
    directMetadata.put(metadataBacking).flip();
    directMetadata.position(1).limit(1 + expectedMetadata.length);
    ByteBuffer directValue = ByteBuffer.allocateDirect(valueBacking.length);
    directValue.put(valueBacking).flip();
    directValue.position(1).limit(1 + expectedValue.length);
    assertUnshreddedBufferRoundTrip(schema, directMetadata, directValue, expectedMetadata, expectedValue);

    ByteBuffer readOnlyMetadata = ByteBuffer.wrap(metadataBacking).asReadOnlyBuffer();
    readOnlyMetadata.position(1).limit(1 + expectedMetadata.length);
    ByteBuffer readOnlyValue = ByteBuffer.wrap(valueBacking).asReadOnlyBuffer();
    readOnlyValue.position(1).limit(1 + expectedValue.length);
    assertUnshreddedBufferRoundTrip(schema, readOnlyMetadata, readOnlyValue, expectedMetadata, expectedValue);
  }

  @Test
  public void testStandaloneExtractorOwnsSchemaBoundVariantConverters() {
    Variant expected = objectVariant("name", "pinot");
    MessageType schema = MessageTypeParser.parseMessageType(
        "message direct_variant {"
            + " optional group variant_col (VARIANT(1)) {"
            + "   required binary metadata;"
            + "   optional binary value;"
            + " }"
            + "}");
    Group root = new SimpleGroupFactory(schema).newGroup();
    root.addGroup(VARIANT_FIELD)
        .append("metadata", Binary.fromConstantByteBuffer(expected.getMetadataBuffer()))
        .append("value", Binary.fromConstantByteBuffer(expected.getValueBuffer()));

    ParquetNativeRecordExtractorConfig config = new ParquetNativeRecordExtractorConfig();
    config.setParquetSchema(schema);
    ParquetNativeRecordExtractor extractor = new ParquetNativeRecordExtractor();
    extractor.init(Set.of(VARIANT_FIELD), config);

    GenericRow row = extractor.extract(root, new GenericRow());
    Variant actual = decode((byte[]) row.getValue(VARIANT_FIELD));
    assertEquals(actual.getFieldByKey("name").getString(), "pinot");

    ParquetNativeRecordExtractor compatibilityExtractor = new ParquetNativeRecordExtractor();
    compatibilityExtractor.init(Set.of(VARIANT_FIELD), null);
    GenericRow compatibilityRow = compatibilityExtractor.extract(root, new GenericRow());
    assertEquals(decode((byte[]) compatibilityRow.getValue(VARIANT_FIELD)).getFieldByKey("name").getString(),
        "pinot");
  }

  @Test
  public void testShreddedPrimitiveTypes()
      throws Exception {
    File dataFile = writeShreddedPrimitiveFile();
    assertShreddedPrimitiveRow(new ParquetNativeRecordReader(), dataFile);
  }

  @Test
  public void testShreddedArrayWithRepeatedAndNullElements()
      throws Exception {
    File dataFile = writeShreddedArrayFile();
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      reader.init(dataFile, null, null);
      List<GenericRow> rows = readAll(reader);
      assertEquals(rows.size(), 1);

      Variant array = decode((byte[]) rows.get(0).getValue(VARIANT_FIELD));
      assertEquals(array.getType(), Variant.Type.ARRAY);
      assertEquals(array.numArrayElements(), 3);
      assertEquals(array.getElementAtIndex(0).getInt(), 7);
      assertEquals(array.getElementAtIndex(1).getType(), Variant.Type.NULL);
      assertEquals(array.getElementAtIndex(2).getInt(), 9);
    }
  }

  @Test
  public void testAutomaticReaderReinitializesFromVariantToOrdinary()
      throws Exception {
    File variantFile = writeScalarVariantFile(objectVariant("name", "pinot"));
    File ordinaryFile = getResourceFile("data-avro.parquet");
    try (ParquetRecordReader reader = new ParquetRecordReader()) {
      reader.init(variantFile, Set.of("id", VARIANT_FIELD), null);
      assertFalse(reader.useAvroParquetRecordReader());
      assertTrue(reader.hasNext());
      assertEquals(reader.next().getValue("id"), 1);

      reader.init(ordinaryFile, null, null);
      assertTrue(reader.useAvroParquetRecordReader());
      assertTrue(reader.hasNext());
    }
  }

  @Test
  public void testAutomaticReaderReinitializesFromOrdinaryToVariant()
      throws Exception {
    File variantFile = writeScalarVariantFile(objectVariant("name", "pinot"));
    File ordinaryFile = getResourceFile("data-avro.parquet");
    try (ParquetRecordReader reader = new ParquetRecordReader()) {
      reader.init(ordinaryFile, null, null);
      assertTrue(reader.useAvroParquetRecordReader());
      assertTrue(reader.hasNext());
      reader.next();

      reader.init(variantFile, Set.of("id", VARIANT_FIELD), null);
      assertFalse(reader.useAvroParquetRecordReader());
      assertTrue(reader.hasNext());
      assertEquals(reader.next().getValue("id"), 1);
    }
  }

  @Test
  public void testNativeReaderReinitializesOnlyAfterCandidateSucceeds()
      throws Exception {
    File firstFile = writeScalarVariantFile(objectVariant("name", "first"), "native-first.parquet", Map.of());
    File secondFile = writeScalarVariantFile(objectVariant("name", "second"), "native-second.parquet", Map.of());
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      reader.init(firstFile, Set.of("id"), null);
      assertEquals(reader.next().getValue("id"), 1);

      reader.init(secondFile, Set.of(VARIANT_FIELD), null);
      List<GenericRow> rows = readAll(reader);
      assertEquals(rows.size(), 4);
      assertFalse(rows.get(2).getFieldToValueMap().containsKey("id"));
      Variant second = decode((byte[]) rows.get(2).getValue(VARIANT_FIELD));
      assertEquals(second.getFieldByKey("name").getString(), "second");
    }
  }

  @Test
  public void testNativeReaderFailedReinitializationRetainsPreviousState()
      throws Exception {
    File variantFile = writeScalarVariantFile(objectVariant("name", "pinot"));
    File invalidFile = prepareFile("invalid-native.parquet");
    FileUtils.writeByteArrayToFile(invalidFile, new byte[]{'N', 'O', 'P', 'E'});
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      reader.init(variantFile, Set.of("id", VARIANT_FIELD), null);
      assertEquals(reader.next().getValue("id"), 1);

      expectThrows(Exception.class, () -> reader.init(invalidFile, null, null));
      assertTrue(reader.hasNext());
      assertEquals(reader.next().getValue("id"), 2);
    }
  }

  @Test
  public void testNativeReaderPublishesReplacementBeforePreviousCloseFailure()
      throws Exception {
    File variantFile = writeScalarVariantFile(objectVariant("name", "replacement"));
    FailOnceOnCloseParquetFileReader previousReader = new FailOnceOnCloseParquetFileReader(variantFile);

    try {
      try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
        Field readerField = ParquetNativeRecordReader.class.getDeclaredField("_parquetFileReader");
        readerField.setAccessible(true);
        readerField.set(reader, previousReader);

        IOException exception =
            expectThrows(IOException.class, () -> reader.init(variantFile, Set.of("id", VARIANT_FIELD), null));
        assertEquals(exception.getMessage(), "previous close failed");

        assertTrue(reader.hasNext());
        assertEquals(reader.next().getValue("id"), 1);
      }
    } finally {
      previousReader.close();
    }
  }

  @Test
  public void testAvroMetadataSelectionRemainsBackwardCompatibleWithExplicitNativeOptIn()
      throws Exception {
    File variantFile = writeScalarVariantFileWithAvroMetadata(objectVariant("name", "pinot"));
    ParquetRecordReaderConfig forceAvro = new ParquetRecordReaderConfig();
    forceAvro.setUseParquetAvroRecordReader(true);

    try (ParquetRecordReader reader = new ParquetRecordReader()) {
      reader.init(variantFile, Set.of("id"), null);
      assertTrue(reader.useAvroParquetRecordReader());
      assertEquals(reader.next().getValue("id"), 1);

      reader.init(variantFile, Set.of("id"), forceAvro);
      assertTrue(reader.useAvroParquetRecordReader());
      assertEquals(reader.next().getValue("id"), 1);
    }

    ParquetRecordReaderConfig forceNative = new ParquetRecordReaderConfig();
    forceNative.setUseParquetNativeRecordReader(true);
    try (ParquetRecordReader reader = new ParquetRecordReader()) {
      reader.init(variantFile, Set.of("id", VARIANT_FIELD), forceNative);
      assertFalse(reader.useAvroParquetRecordReader());
      assertScalarRows(readAll(reader));
    }
  }

  @Test
  public void testFailedReinitializationRetainsPreviousDelegate()
      throws Exception {
    File ordinaryFile = getResourceFile("data-avro.parquet");
    File invalidFile = prepareFile("invalid.parquet");
    FileUtils.writeByteArrayToFile(invalidFile, new byte[]{'N', 'O', 'P', 'E'});
    try (ParquetRecordReader reader = new ParquetRecordReader()) {
      reader.init(ordinaryFile, null, null);
      expectThrows(Exception.class, () -> reader.init(invalidFile, null, null));
      assertTrue(reader.useAvroParquetRecordReader());
      assertTrue(reader.hasNext());
      reader.next();
    }
    try (ParquetAvroRecordReader reader = new ParquetAvroRecordReader()) {
      reader.init(ordinaryFile, null, null);
      expectThrows(Exception.class, () -> reader.init(invalidFile, null, null));
      assertTrue(reader.hasNext());
      reader.next();
    }
  }

  @Test
  public void testMalformedShreddedRowsAdvanceToNextRowAndEof()
      throws Exception {
    File dataFile = writeMalformedShreddedRows();
    try (ParquetNativeRecordReader reader = new ParquetNativeRecordReader()) {
      reader.init(dataFile, null, null);

      assertEquals(reader.next().getValue("id"), 1);
      assertTrue(reader.hasNext());
      expectThrows(RuntimeException.class, reader::next);

      assertTrue(reader.hasNext());
      assertEquals(reader.next().getValue("id"), 3);
      assertTrue(reader.hasNext());
      expectThrows(RuntimeException.class, reader::next);
      assertFalse(reader.hasNext(), "A terminal malformed row must still advance the physical reader to EOF");
    }
  }

  @Test
  public void testVariantDetectionAndUnsupportedShapes() {
    MessageType lookalike = MessageTypeParser.parseMessageType(
        "message lookalike { optional group variant_col { required binary metadata; optional binary value; } }");
    assertTrue(ParquetVariantConverter.validateAndGetTopLevelVariantFields(lookalike).isEmpty());

    MessageType unsupportedVersion = MessageTypeParser.parseMessageType(
        "message unsupported { optional group variant_col (VARIANT(2)) {"
            + " required binary metadata; optional binary value; } }");
    UnsupportedOperationException versionException = expectThrows(UnsupportedOperationException.class,
        () -> ParquetVariantConverter.validateAndGetTopLevelVariantFields(unsupportedVersion));
    assertTrue(versionException.getMessage().contains("spec version"));

    MessageType repeated = Types.buildMessage()
        .addField(Types.buildGroup(Type.Repetition.REPEATED)
            .as(LogicalTypeAnnotation.variantType((byte) 1))
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("metadata")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY)
            .named("value")
            .named(VARIANT_FIELD))
        .named("repeated_variant");
    UnsupportedOperationException repeatedException = expectThrows(UnsupportedOperationException.class,
        () -> ParquetVariantConverter.validateAndGetTopLevelVariantFields(repeated));
    assertTrue(repeatedException.getMessage().contains("Repeated"));

    MessageType missingMetadata = MessageTypeParser.parseMessageType(
        "message malformed { optional group variant_col (VARIANT(1)) { optional binary value; } }");
    IllegalArgumentException metadataException = expectThrows(IllegalArgumentException.class,
        () -> ParquetVariantConverter.validateAndGetTopLevelVariantFields(missingMetadata));
    assertTrue(metadataException.getMessage().contains("metadata"));

    MessageType nested = MessageTypeParser.parseMessageType(
        "message nested { optional group wrapper { optional group variant_col (VARIANT(1)) {"
            + " required binary metadata; optional binary value; } } }");
    UnsupportedOperationException nestedException = expectThrows(UnsupportedOperationException.class,
        () -> ParquetVariantConverter.validateAndGetTopLevelVariantFields(nested));
    assertTrue(nestedException.getMessage().contains("Nested"));

    MessageType unsupportedInt96 = MessageTypeParser.parseMessageType(
        "message unsupported_int96 { optional group variant_col (VARIANT(1)) {"
            + " required binary metadata; optional int96 typed_value; } }");
    UnsupportedOperationException int96Exception = expectThrows(UnsupportedOperationException.class,
        () -> ParquetVariantConverter.createTopLevelVariantConverters(unsupportedInt96));
    assertTrue(int96Exception.getMessage().contains("Unsupported shredded value type"));
    assertTrue(int96Exception.getMessage().matches("(?i).*int96.*"));
  }

  @Test
  public void testNestedVariantValidationHonorsSelectedFields() {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message nested {"
            + " required int32 id;"
            + " optional group wrapper {"
            + "   optional group variant_col (VARIANT(1)) {"
            + "     required binary metadata;"
            + "     optional binary value;"
            + "   }"
            + " }"
            + "}");
    ParquetNativeRecordExtractorConfig config = new ParquetNativeRecordExtractorConfig();
    config.setParquetSchema(schema);

    ParquetNativeRecordExtractor idExtractor = new ParquetNativeRecordExtractor();
    idExtractor.init(Set.of("id"), config);
    GenericRow row = idExtractor.extract(new SimpleGroupFactory(schema).newGroup().append("id", 7), new GenericRow());
    assertEquals(row.getFieldToValueMap(), Map.of("id", 7));

    ParquetNativeRecordExtractor wrapperExtractor = new ParquetNativeRecordExtractor();
    UnsupportedOperationException selectedException = expectThrows(UnsupportedOperationException.class,
        () -> wrapperExtractor.init(Set.of("wrapper"), config));
    assertTrue(selectedException.getMessage().contains("Nested"));

    ParquetNativeRecordExtractor extractAll = new ParquetNativeRecordExtractor();
    UnsupportedOperationException extractAllException = expectThrows(UnsupportedOperationException.class,
        () -> extractAll.init(null, config));
    assertTrue(extractAllException.getMessage().contains("Nested"));
  }

  private void assertScalarRows(RecordReader reader, File dataFile)
      throws IOException {
    try (reader) {
      reader.init(dataFile, Set.of("id", VARIANT_FIELD), null);
      List<GenericRow> firstPass = readAll(reader);
      assertScalarRows(firstPass);

      reader.rewind();
      List<GenericRow> secondPass = readAll(reader);
      assertScalarRows(secondPass);
    }
  }

  private void assertScalarRows(List<GenericRow> rows) {
    assertEquals(rows.size(), 4);
    for (GenericRow row : rows) {
      assertFalse(row.getFieldToValueMap().containsKey("note"));
    }

    assertEquals(rows.get(0).getValue("id"), 1);
    assertNull(rows.get(0).getValue(VARIANT_FIELD));

    assertEquals(rows.get(1).getValue("id"), 2);
    byte[] encodedNull = (byte[]) rows.get(1).getValue(VARIANT_FIELD);
    assertTrue(VariantEnvelope.isEnvelope(encodedNull));
    assertEquals(decode(encodedNull).getType(), Variant.Type.NULL);

    assertEquals(rows.get(2).getValue("id"), 3);
    Variant object = decode((byte[]) rows.get(2).getValue(VARIANT_FIELD));
    assertEquals(object.getType(), Variant.Type.OBJECT);
    assertEquals(object.getFieldByKey("name").getString(), "pinot");

    assertEquals(rows.get(3).getValue("id"), 4);
    Variant shredded = decode((byte[]) rows.get(3).getValue(VARIANT_FIELD));
    assertEquals(shredded.getType(), Variant.Type.INT);
    assertEquals(shredded.getInt(), 42);
  }

  private void assertShreddedObjectRows(RecordReader reader, File dataFile)
      throws IOException {
    try (reader) {
      reader.init(dataFile, null, null);
      assertShreddedObjectRows(readAll(reader));
      reader.rewind();
      assertShreddedObjectRows(readAll(reader));
    }
  }

  private void assertShreddedObjectRows(List<GenericRow> rows) {
    assertEquals(rows.size(), 2);

    Variant fullyShredded = decode((byte[]) rows.get(0).getValue(VARIANT_FIELD));
    assertEquals(fullyShredded.getType(), Variant.Type.OBJECT);
    assertNull(fullyShredded.getFieldByKey("cold"));
    assertEquals(fullyShredded.getFieldByKey("hot").getInt(), 9);

    Variant partiallyShredded = decode((byte[]) rows.get(1).getValue(VARIANT_FIELD));
    assertEquals(partiallyShredded.getType(), Variant.Type.OBJECT);
    assertEquals(partiallyShredded.getFieldByKey("cold").getString(), "kept");
    assertEquals(partiallyShredded.getFieldByKey("hot").getInt(), 7);
  }

  private void assertShreddedPrimitiveRow(RecordReader reader, File dataFile)
      throws IOException {
    try (reader) {
      reader.init(dataFile, null, null);
      List<GenericRow> rows = readAll(reader);
      assertEquals(rows.size(), 1);
      GenericRow row = rows.get(0);

      Variant booleanValue = decode((byte[]) row.getValue("variant_boolean"));
      assertEquals(booleanValue.getType(), Variant.Type.BOOLEAN);
      assertTrue(booleanValue.getBoolean());

      Variant longValue = decode((byte[]) row.getValue("variant_long"));
      assertEquals(longValue.getType(), Variant.Type.LONG);
      assertEquals(longValue.getLong(), 9_876_543_210L);

      Variant floatValue = decode((byte[]) row.getValue("variant_float"));
      assertEquals(floatValue.getType(), Variant.Type.FLOAT);
      assertEquals(floatValue.getFloat(), 1.25f);

      Variant doubleValue = decode((byte[]) row.getValue("variant_double"));
      assertEquals(doubleValue.getType(), Variant.Type.DOUBLE);
      assertEquals(doubleValue.getDouble(), 2.5d);

      Variant binaryValue = decode((byte[]) row.getValue("variant_binary"));
      assertEquals(binaryValue.getType(), Variant.Type.BINARY);
      assertEquals(remainingBytes(binaryValue.getBinary()), new byte[]{1, 2, 3});

      Variant decimalValue = decode((byte[]) row.getValue("variant_decimal"));
      assertEquals(decimalValue.getType(), Variant.Type.DECIMAL4);
      assertEquals(decimalValue.getDecimal(), new BigDecimal("123.45"));
    }
  }

  private List<GenericRow> readAll(RecordReader reader)
      throws IOException {
    List<GenericRow> rows = new ArrayList<>();
    while (reader.hasNext()) {
      rows.add(reader.next());
    }
    return rows;
  }

  private static File getResourceFile(String resourceName) {
    return new File(Objects.requireNonNull(
        ParquetVariantRecordReaderTest.class.getClassLoader().getResource(resourceName),
        "Missing test resource: " + resourceName).getFile());
  }

  private File writeScalarVariantFile(Variant objectVariant)
      throws IOException {
    return writeScalarVariantFile(objectVariant, "scalar-variants.parquet", Map.of());
  }

  private File writeScalarVariantFileWithAvroMetadata(Variant objectVariant)
      throws IOException {
    String avroSchema = "{\"type\":\"record\",\"name\":\"scalar_variants\",\"fields\":["
        + "{\"name\":\"id\",\"type\":\"int\"},"
        + "{\"name\":\"note\",\"type\":[\"null\",\"string\"],\"default\":null},"
        + "{\"name\":\"variant_col\",\"type\":[\"null\","
        + "{\"type\":\"record\",\"name\":\"variant_value\",\"fields\":["
        + "{\"name\":\"value\",\"type\":[\"null\",\"bytes\"],\"default\":null},"
        + "{\"name\":\"metadata\",\"type\":\"bytes\"},"
        + "{\"name\":\"typed_value\",\"type\":[\"null\",\"int\"],\"default\":null}"
        + "]}],\"default\":null}]}";
    return writeScalarVariantFile(objectVariant, "scalar-variants-with-avro-metadata.parquet",
        Map.of("parquet.avro.schema", avroSchema));
  }

  private File writeScalarVariantFile(Variant objectVariant, String fileName, Map<String, String> extraMetadata)
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message scalar_variants {"
            + " required int32 id;"
            + " optional binary note (STRING);"
            + " optional group variant_col (VARIANT(1)) {"
            + "   optional binary value;"
            + "   required binary metadata;"
            + "   optional int32 typed_value;"
            + " }"
            + "}");
    File dataFile = prepareFile(fileName);
    try (ParquetWriter<Group> writer = newWriter(dataFile, schema, extraMetadata)) {
      SimpleGroupFactory groups = new SimpleGroupFactory(schema);

      writer.write(groups.newGroup().append("id", 1).append("note", "absent"));

      VariantBuilder nullBuilder = new VariantBuilder();
      nullBuilder.appendNull();
      Variant nullVariant = nullBuilder.build();
      Group encodedNull = groups.newGroup().append("id", 2).append("note", "variant-null");
      encodedNull.addGroup(VARIANT_FIELD)
          .append("metadata", Binary.fromConstantByteBuffer(nullVariant.getMetadataBuffer()));
      writer.write(encodedNull);

      Group unshredded = groups.newGroup().append("id", 3).append("note", "unshredded");
      unshredded.addGroup(VARIANT_FIELD)
          .append("metadata", Binary.fromConstantByteBuffer(objectVariant.getMetadataBuffer()))
          .append("value", Binary.fromConstantByteBuffer(objectVariant.getValueBuffer()));
      writer.write(unshredded);

      VariantBuilder scalarBuilder = new VariantBuilder();
      scalarBuilder.appendInt(42);
      Variant scalarVariant = scalarBuilder.build();
      Group shredded = groups.newGroup().append("id", 4).append("note", "shredded");
      shredded.addGroup(VARIANT_FIELD)
          .append("metadata", Binary.fromConstantByteBuffer(scalarVariant.getMetadataBuffer()))
          .append("typed_value", 42);
      writer.write(shredded);
    }
    return dataFile;
  }

  private File writePartiallyShreddedFile(ByteBuffer metadata, ByteBuffer baseValue)
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message partial_variant {"
            + " required int32 id;"
            + " optional group variant_col (VARIANT(1)) {"
            + "   required binary metadata;"
            + "   optional binary value;"
            + "   optional group typed_value {"
            + "     required group hot {"
            + "       optional binary value;"
            + "       optional int32 typed_value;"
            + "     }"
            + "   }"
            + " }"
            + "}");
    File dataFile = prepareFile("partial-variant.parquet");
    try (ParquetWriter<Group> writer = newWriter(dataFile, schema)) {
      SimpleGroupFactory groups = new SimpleGroupFactory(schema);

      Group fullyShreddedRow = groups.newGroup().append("id", 1);
      Group fullyShredded = fullyShreddedRow.addGroup(VARIANT_FIELD)
          .append("metadata", Binary.fromConstantByteBuffer(metadata));
      fullyShredded.addGroup("typed_value").addGroup("hot").append("typed_value", 9);
      writer.write(fullyShreddedRow);

      Group partiallyShreddedRow = groups.newGroup().append("id", 2);
      Group partiallyShredded = partiallyShreddedRow.addGroup(VARIANT_FIELD)
          .append("metadata", Binary.fromConstantByteBuffer(metadata))
          .append("value", Binary.fromConstantByteBuffer(baseValue));
      partiallyShredded.addGroup("typed_value").addGroup("hot").append("typed_value", 7);
      writer.write(partiallyShreddedRow);
    }
    return dataFile;
  }

  private File writeShreddedPrimitiveFile()
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message shredded_primitives {"
            + " required int32 id;"
            + variantGroup("variant_boolean", "boolean typed_value")
            + variantGroup("variant_long", "int64 typed_value")
            + variantGroup("variant_float", "float typed_value")
            + variantGroup("variant_double", "double typed_value")
            + variantGroup("variant_binary", "binary typed_value")
            + variantGroup("variant_decimal", "fixed_len_byte_array(4) typed_value (DECIMAL(9,2))")
            + "}");
    File dataFile = prepareFile("shredded-primitives.parquet");
    try (ParquetWriter<Group> writer = newWriter(dataFile, schema)) {
      Group row = new SimpleGroupFactory(schema).newGroup().append("id", 1);
      addShreddedValue(row, "variant_boolean", variantMetadata(builder -> builder.appendBoolean(true)))
          .append("typed_value", true);
      addShreddedValue(row, "variant_long", variantMetadata(builder -> builder.appendLong(9_876_543_210L)))
          .append("typed_value", 9_876_543_210L);
      addShreddedValue(row, "variant_float", variantMetadata(builder -> builder.appendFloat(1.25f)))
          .append("typed_value", 1.25f);
      addShreddedValue(row, "variant_double", variantMetadata(builder -> builder.appendDouble(2.5d)))
          .append("typed_value", 2.5d);
      addShreddedValue(row, "variant_binary",
          variantMetadata(builder -> builder.appendBinary(ByteBuffer.wrap(new byte[]{1, 2, 3}))))
          .append("typed_value", Binary.fromConstantByteArray(new byte[]{1, 2, 3}));
      addShreddedValue(row, "variant_decimal",
          variantMetadata(builder -> builder.appendDecimal(new BigDecimal("123.45"))))
          .append("typed_value", Binary.fromConstantByteArray(fixedLengthBytes(BigInteger.valueOf(12_345), 4)));
      writer.write(row);
    }
    return dataFile;
  }

  private File writeShreddedArrayFile()
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message shredded_array {"
            + " required int32 id;"
            + " optional group variant_col (VARIANT(1)) {"
            + "   required binary metadata;"
            + "   optional group typed_value (LIST) {"
            + "     repeated group list {"
            + "       optional group element {"
            + "         optional binary value;"
            + "         optional int32 typed_value;"
            + "       }"
            + "     }"
            + "   }"
            + " }"
            + "}");
    VariantBuilder metadataBuilder = new VariantBuilder();
    VariantArrayBuilder arrayBuilder = metadataBuilder.startArray();
    arrayBuilder.appendInt(7);
    arrayBuilder.appendNull();
    arrayBuilder.appendInt(9);
    metadataBuilder.endArray();
    Variant metadataSource = metadataBuilder.build();

    File dataFile = prepareFile("shredded-array.parquet");
    try (ParquetWriter<Group> writer = newWriter(dataFile, schema)) {
      Group row = new SimpleGroupFactory(schema).newGroup().append("id", 1);
      Group variant = row.addGroup(VARIANT_FIELD)
          .append("metadata", Binary.fromConstantByteBuffer(metadataSource.getMetadataBuffer()));
      Group list = variant.addGroup("typed_value");
      list.addGroup("list").addGroup("element").append("typed_value", 7);
      list.addGroup("list");
      list.addGroup("list").addGroup("element").append("typed_value", 9);
      writer.write(row);
    }
    return dataFile;
  }

  private File writeMalformedShreddedRows()
      throws IOException {
    MessageType schema = MessageTypeParser.parseMessageType(
        "message malformed_shredded_rows {"
            + " required int32 id;"
            + " optional group variant_col (VARIANT(1)) {"
            + "   required binary metadata;"
            + "   optional binary value;"
            + "   optional int32 typed_value;"
            + " }"
            + "}");
    VariantBuilder builder = new VariantBuilder();
    builder.appendInt(7);
    Variant valid = builder.build();
    File dataFile = prepareFile("malformed-shredded-rows.parquet");
    try (ParquetWriter<Group> writer = newWriter(dataFile, schema)) {
      SimpleGroupFactory groups = new SimpleGroupFactory(schema);
      for (int id = 1; id <= 4; id++) {
        Group row = groups.newGroup().append("id", id);
        Group variant = row.addGroup(VARIANT_FIELD);
        if ((id & 1) == 0) {
          variant.append("metadata", Binary.fromConstantByteArray(new byte[0]))
              .append("typed_value", id);
        } else {
          variant.append("metadata", Binary.fromConstantByteBuffer(valid.getMetadataBuffer()))
              .append("value", Binary.fromConstantByteBuffer(valid.getValueBuffer()));
        }
        writer.write(row);
      }
    }
    return dataFile;
  }

  private File prepareFile(String name)
      throws IOException {
    FileUtils.forceMkdir(_tempDir);
    File dataFile = new File(_tempDir, name);
    FileUtils.deleteQuietly(dataFile);
    return dataFile;
  }

  private ParquetWriter<Group> newWriter(File dataFile, MessageType schema)
      throws IOException {
    return newWriter(dataFile, schema, Map.of());
  }

  private ParquetWriter<Group> newWriter(File dataFile, MessageType schema, Map<String, String> extraMetadata)
      throws IOException {
    return ExampleParquetWriter.builder(new Path(dataFile.getAbsolutePath()))
        .withType(schema)
        .withExtraMetaData(extraMetadata)
        .build();
  }

  private static String variantGroup(String fieldName, String typedValueDeclaration) {
    return " optional group " + fieldName + " (VARIANT(1)) {"
        + " required binary metadata;"
        + " optional binary value;"
        + " optional " + typedValueDeclaration + ";"
        + " }";
  }

  private static Group addShreddedValue(Group row, String fieldName, ByteBuffer metadata) {
    return row.addGroup(fieldName).append("metadata", Binary.fromConstantByteBuffer(metadata));
  }

  private static ByteBuffer variantMetadata(Consumer<VariantBuilder> appender) {
    VariantBuilder builder = new VariantBuilder();
    appender.accept(builder);
    return builder.build().getMetadataBuffer();
  }

  private static byte[] fixedLengthBytes(BigInteger value, int length) {
    byte[] source = value.toByteArray();
    byte[] result = new byte[length];
    byte signExtension = value.signum() < 0 ? (byte) 0xff : 0;
    Arrays.fill(result, signExtension);
    System.arraycopy(source, Math.max(0, source.length - length), result, Math.max(0, length - source.length),
        Math.min(source.length, length));
    return result;
  }

  /// Returns a valid object whose a/b/c offsets are `[4, 2, 0, 6]` because its int8 values are physically reversed.
  private static Variant nonMonotonicObjectVariant() {
    byte[] metadata = {0x11, 0x03, 0x00, 0x01, 0x02, 0x03, 'a', 'b', 'c'};
    byte[] value = {
        0x02, 0x03,
        0x00, 0x01, 0x02,
        0x04, 0x02, 0x00, 0x06,
        0x0C, 0x03,
        0x0C, 0x02,
        0x0C, 0x01
    };
    return new Variant(value, metadata);
  }

  private static Variant objectVariant(Object... keysAndValues) {
    VariantBuilder builder = new VariantBuilder();
    VariantObjectBuilder object = builder.startObject();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      object.appendKey((String) keysAndValues[i]);
      Object value = keysAndValues[i + 1];
      if (value instanceof String) {
        object.appendString((String) value);
      } else if (value instanceof Integer) {
        object.appendInt((Integer) value);
      } else {
        throw new IllegalArgumentException("Unsupported test Variant value: " + value);
      }
    }
    builder.endObject();
    return builder.build();
  }

  private static Variant decode(byte[] envelope) {
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(envelope);
    return new Variant(decoded.getValue(), decoded.getMetadata());
  }

  private static byte[] addSentinels(byte[] value) {
    byte[] backing = new byte[value.length + 2];
    backing[0] = 99;
    backing[backing.length - 1] = 98;
    System.arraycopy(value, 0, backing, 1, value.length);
    return backing;
  }

  private static byte[] remainingBytes(ByteBuffer buffer) {
    ByteBuffer view = buffer.duplicate();
    byte[] bytes = new byte[view.remaining()];
    view.get(bytes);
    return bytes;
  }

  private static void assertUnshreddedBufferRoundTrip(MessageType schema, ByteBuffer metadata, ByteBuffer value,
      byte[] expectedMetadata, byte[] expectedValue) {
    int metadataPosition = metadata.position();
    int metadataLimit = metadata.limit();
    int valuePosition = value.position();
    int valueLimit = value.limit();
    Group variantGroup = new SimpleGroupFactory(schema).newGroup().addGroup(VARIANT_FIELD)
        .append("metadata", Binary.fromConstantByteBuffer(metadata))
        .append("value", Binary.fromConstantByteBuffer(value));

    ParquetVariantConverter converter =
        ParquetVariantConverter.createTopLevelVariantConverters(schema)[schema.getFieldIndex(VARIANT_FIELD)];
    VariantEnvelope.Decoded decoded = VariantEnvelope.decode(converter.convert(variantGroup));
    assertEquals(remainingBytes(decoded.getMetadata()), expectedMetadata);
    assertEquals(remainingBytes(decoded.getValue()), expectedValue);
    assertEquals(metadata.position(), metadataPosition);
    assertEquals(metadata.limit(), metadataLimit);
    assertEquals(value.position(), valuePosition);
    assertEquals(value.limit(), valueLimit);
  }

  private static final class FailOnceOnCloseParquetFileReader extends ParquetFileReader {
    private boolean _failOnClose = true;

    private FailOnceOnCloseParquetFileReader(File file)
        throws IOException {
      super(HadoopInputFile.fromPath(new Path(file.getAbsolutePath()),
              ParquetUtils.getParquetHadoopConfiguration()),
          ParquetReadOptions.builder().withMetadataFilter(ParquetMetadataConverter.NO_FILTER).build());
    }

    @Override
    public void close()
        throws IOException {
      if (_failOnClose) {
        _failOnClose = false;
        throw new IOException("previous close failed");
      }
      super.close();
    }
  }
}
