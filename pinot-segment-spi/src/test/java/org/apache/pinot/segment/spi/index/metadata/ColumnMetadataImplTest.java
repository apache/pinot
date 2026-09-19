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
package org.apache.pinot.segment.spi.index.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Segment;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ColumnNameInterner;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.UuidUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Unit tests for [ColumnMetadataImpl#fromPropertiesConfiguration] focused on the
/// `FORWARD_INDEX_ENCODING` property's rolling-upgrade behavior.
///
/// `FORWARD_INDEX_ENCODING` was added in this release. Old segments built before this release won't have
/// the key in `metadata.properties`, so [ColumnMetadataImpl#fromPropertiesConfiguration] falls back to
/// deriving the encoding from `HAS_DICTIONARY`: dict means `DICTIONARY`-encoded forward, no dict means
/// `RAW`. The new "shared dictionary on RAW forward" segment shape is only representable when the key is
/// explicitly written by the new segment creator.
public class ColumnMetadataImplTest {

  /// Old-segment fallback path: no FORWARD_INDEX_ENCODING in metadata, dict present → encoding inferred as DICTIONARY.
  @Test
  public void fallsBackToDictionaryEncodingWhenKeyAbsentAndHasDictionary() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), true);
    // FORWARD_INDEX_ENCODING intentionally NOT set, simulating an old segment.

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertTrue(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.DICTIONARY,
        "Old segments without FORWARD_INDEX_ENCODING and HAS_DICTIONARY=true must infer DICTIONARY encoding");
  }

  /// Old-segment fallback path: no FORWARD_INDEX_ENCODING in metadata, no dict → encoding inferred as RAW.
  @Test
  public void fallsBackToRawEncodingWhenKeyAbsentAndNoDictionary() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), false);
    // FORWARD_INDEX_ENCODING intentionally NOT set, simulating an old raw-forward segment.

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertFalse(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.RAW,
        "Old segments without FORWARD_INDEX_ENCODING and HAS_DICTIONARY=false must infer RAW encoding");
  }

  /// New shared-dict shape: FORWARD_INDEX_ENCODING=RAW + HAS_DICTIONARY=true. The new segment creator writes both
  /// keys; the metadata loader must honor the explicit FORWARD_INDEX_ENCODING and not fall back to inference.
  @Test
  public void honorsExplicitRawEncodingEvenWhenHasDictionary() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), true);
    config.setProperty(Column.getKeyFor("col", Column.FORWARD_INDEX_ENCODING), EncodingType.RAW.name());

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertTrue(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.RAW,
        "Explicit FORWARD_INDEX_ENCODING=RAW must override inference even when HAS_DICTIONARY=true (shared-dict)");
  }

  /// New segment with explicit FORWARD_INDEX_ENCODING=DICTIONARY; verify it round-trips.
  @Test
  public void honorsExplicitDictionaryEncoding() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), true);
    config.setProperty(Column.getKeyFor("col", Column.FORWARD_INDEX_ENCODING), EncodingType.DICTIONARY.name());

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertTrue(metadata.hasDictionary());
    assertEquals(metadata.getForwardIndexEncoding(), EncodingType.DICTIONARY);
  }

  @Test
  public void parentColumnRoundtrip() {
    ColumnMetadataImpl meta = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("metrics$cpu", DataType.DOUBLE, true))
        .setParentColumn("metrics")
        .build();
    assertEquals(meta.getParentColumn(), "metrics");
    assertTrue(meta.isMaterializedChild());
  }

  /// Verify the PARENT_COLUMN key in metadata.properties round-trips through
  /// [ColumnMetadataImpl#fromPropertiesConfiguration].
  @Test
  public void parentColumnReadFromPropertiesConfig() {
    PropertiesConfiguration config = baseConfig("metrics$cpu");
    config.setProperty(Column.getKeyFor("metrics$cpu", Column.PARENT_COLUMN), "metrics");

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "metrics$cpu");

    assertEquals(metadata.getParentColumn(), "metrics");
    assertTrue(metadata.isMaterializedChild());
  }

  @Test
  public void compressionStatsPersistedAndLoaded() {
    PropertiesConfiguration config = baseConfig("rawCol");
    config.setProperty(Column.getKeyFor("rawCol", Column.HAS_DICTIONARY), false);
    config.setProperty(Column.getKeyFor("rawCol", Column.FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES), 4096L);
    config.setProperty(Column.getKeyFor("rawCol", Column.FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE), "LZ4");

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "rawCol");

    assertEquals(metadata.getRawForwardIndexUncompressedValueSizeInBytes(), 4096L);
    assertEquals(metadata.getRawForwardIndexChunkCompressionType(), ChunkCompressionType.LZ4);
  }

  @Test
  public void compressionStatsDefaultToUnavailableOnOldSegment() {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.HAS_DICTIONARY), false);
    // Neither FORWARD_INDEX_RAW_UNCOMPRESSED_VALUE_SIZE_IN_BYTES nor FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE set

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertEquals(metadata.getRawForwardIndexUncompressedValueSizeInBytes(), ColumnMetadata.UNAVAILABLE,
        "Old segments without compression stats should return UNAVAILABLE");
    assertNull(metadata.getRawForwardIndexChunkCompressionType(),
        "Old segments without a chunk compression type should return null");
  }

  @Test
  public void invalidCompressionTypeIncludesColumnContext() {
    PropertiesConfiguration config = baseConfig("badColumn");
    config.setProperty(Column.getKeyFor("badColumn", Column.FORWARD_INDEX_RAW_CHUNK_COMPRESSION_TYPE), "NOT_A_CODEC");

    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "badColumn"));
    assertTrue(exception.getMessage().contains("badColumn"));
    assertTrue(exception.getMessage().contains("NOT_A_CODEC"));
  }

  @Test
  public void compressionStatsParticipateInValueObjectMethods() {
    DimensionFieldSpec fieldSpec = new DimensionFieldSpec("col", DataType.STRING, true);
    ColumnMetadataImpl first = ColumnMetadataImpl.builder()
        .setFieldSpec(fieldSpec)
        .setRawForwardIndexUncompressedValueSizeInBytes(100)
        .setRawForwardIndexChunkCompressionType(ChunkCompressionType.LZ4)
        .build();
    ColumnMetadataImpl same = ColumnMetadataImpl.builder()
        .setFieldSpec(fieldSpec)
        .setRawForwardIndexUncompressedValueSizeInBytes(100)
        .setRawForwardIndexChunkCompressionType(ChunkCompressionType.LZ4)
        .build();
    ColumnMetadataImpl different = ColumnMetadataImpl.builder()
        .setFieldSpec(fieldSpec)
        .setRawForwardIndexUncompressedValueSizeInBytes(101)
        .setRawForwardIndexChunkCompressionType(ChunkCompressionType.LZ4)
        .build();

    assertEquals(first, same);
    assertEquals(first.hashCode(), same.hashCode());
    assertNotEquals(first, different);
    assertTrue(first.toString().contains("_compressionMetadata=CompressionMetadata{"));
  }

  @Test
  public void compressionStatsDoNotExpandExistingColumnMetadataJson() {
    ColumnMetadataImpl metadata = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("col", DataType.STRING, true))
        .setRawForwardIndexUncompressedValueSizeInBytes(100)
        .setRawForwardIndexChunkCompressionType(ChunkCompressionType.LZ4)
        .setDictionaryEncodedUncompressedValueSizeInBytes(200)
        .build();

    JsonNode json = JsonUtils.objectToJsonNode(metadata);
    assertFalse(json.has("uncompressedValueSizeInBytes"));
    assertFalse(json.has("forwardIndexChunkCompressionType"));
    assertFalse(json.has("dictionaryUncompressedValueSizeInBytes"));
  }

  // The index-size API works on numeric index ids so this module's tests need no index plugins registered.
  private static final short FORWARD_ID = 2;
  private static final short DICTIONARY_ID = 0;
  private static final short JSON_ID = 5;

  @Test
  public void indexSizesAbsentByDefault()
      throws Exception {
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(baseConfig("col"), 1, "col");
    assertEquals(metadata.getNumIndexes(), 0);
    assertTrue(metadata.getIndexSizeMap().isEmpty());
    expectThrows(IndexOutOfBoundsException.class, () -> metadata.getIndexType(-1));
    expectThrows(IndexOutOfBoundsException.class, () -> metadata.getIndexSize(-1));
    expectThrows(IndexOutOfBoundsException.class, () -> metadata.getIndexType(0));
    expectThrows(IndexOutOfBoundsException.class, () -> metadata.getIndexSize(0));
    // The REST segment-metadata payload keeps its shape: an empty object, never null.
    JsonNode indexSizeMap = JsonUtils.objectToJsonNode(metadata).get("indexSizeMap");
    assertTrue(indexSizeMap.isObject() && indexSizeMap.isEmpty(), String.valueOf(indexSizeMap));
  }

  @Test
  public void indexSizesRoundTripAfterAdd() {
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(baseConfig("col"), 1, "col");
    short[] indexTypes = {FORWARD_ID, DICTIONARY_ID, JSON_ID, 7, Short.MAX_VALUE, Short.MIN_VALUE};
    long[] indexSizes = {100, 200, 0, (1L << 48) - 1, 1L << 32, 1};
    for (int i = 0; i < indexTypes.length; i++) {
      metadata.addIndexSize(indexTypes[i], indexSizes[i]);
    }

    assertEquals(metadata.getNumIndexes(), indexTypes.length);
    for (int i = 0; i < indexTypes.length; i++) {
      assertEquals(metadata.getIndexType(i), indexTypes[i]);
      assertEquals(metadata.getIndexSize(i), indexSizes[i]);
    }
    expectThrows(IndexOutOfBoundsException.class, () -> metadata.getIndexType(-1));
    expectThrows(IndexOutOfBoundsException.class, () -> metadata.getIndexSize(-1));
    expectThrows(IndexOutOfBoundsException.class, () -> metadata.getIndexType(indexTypes.length));
    expectThrows(IndexOutOfBoundsException.class, () -> metadata.getIndexSize(indexTypes.length));
  }

  @Test
  public void indexSizesParticipateInValueObjectMethods() {
    ColumnMetadataImpl first = ColumnMetadataImpl.fromPropertiesConfiguration(baseConfig("col"), 1, "col");
    ColumnMetadataImpl second = ColumnMetadataImpl.fromPropertiesConfiguration(baseConfig("col"), 1, "col");
    ColumnMetadataImpl third = ColumnMetadataImpl.fromPropertiesConfiguration(baseConfig("col"), 1, "col");
    ColumnMetadataImpl noSizes = ColumnMetadataImpl.fromPropertiesConfiguration(baseConfig("col"), 1, "col");
    assertEquals(first, noSizes);
    assertEquals(first.hashCode(), noSizes.hashCode());
    first.addIndexSize(FORWARD_ID, 100);
    second.addIndexSize(FORWARD_ID, 100);
    third.addIndexSize(FORWARD_ID, 101);
    for (ColumnMetadataImpl metadata : new ColumnMetadataImpl[]{first, second, third}) {
      metadata.addIndexSize(DICTIONARY_ID, (1L << 48) - 1);
      metadata.addIndexSize(JSON_ID, 0);
    }

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    assertNotEquals(first, third);
    assertNotEquals(first, noSizes);
    assertEquals(first.toString(), second.toString());
    assertNotEquals(first.toString(), noSizes.toString());
  }

  @Test
  public void rejectsInvalidIndexSize() {
    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(baseConfig("col"), 1, "col");
    expectThrows(IllegalArgumentException.class, () -> metadata.addIndexSize(FORWARD_ID, -1));
    expectThrows(IllegalArgumentException.class, () -> metadata.addIndexSize(FORWARD_ID, 1L << 48));
    assertEquals(metadata.getNumIndexes(), 0, "a rejected size must not be recorded");

    metadata.addIndexSize(FORWARD_ID, 100);
    metadata.addIndexSize(DICTIONARY_ID, 200);
    expectThrows(IllegalArgumentException.class, () -> metadata.addIndexSize(JSON_ID, -1));
    expectThrows(IllegalArgumentException.class, () -> metadata.addIndexSize(JSON_ID, 1L << 48));
    assertEquals(metadata.getNumIndexes(), 2, "a rejected size must preserve existing entries");
    assertEquals(metadata.getIndexType(0), FORWARD_ID);
    assertEquals(metadata.getIndexSize(0), 100);
    assertEquals(metadata.getIndexType(1), DICTIONARY_ID);
    assertEquals(metadata.getIndexSize(1), 200);
  }

  private static final String DATETIME_FORMAT = "1:MILLISECONDS:EPOCH";
  private static final String DATETIME_GRANULARITY = "1:MILLISECONDS";

  /// The segment creator writes `defaultNullValue` for every column, so a column whose default is the type default
  /// must come back holding the shared static constant (one instance per JVM instead of a box plus the literal per
  /// segment) while staying equal to the spec the table schema would build.
  @Test
  public void typeDefaultLiteralSharesTheStaticConstant() {
    Map<DataType, Object> dimensionDefaults = Map.ofEntries(
        Map.entry(DataType.INT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT),
        Map.entry(DataType.LONG, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG),
        Map.entry(DataType.FLOAT, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT),
        Map.entry(DataType.DOUBLE, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE),
        Map.entry(DataType.BOOLEAN, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BOOLEAN),
        Map.entry(DataType.TIMESTAMP, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_TIMESTAMP),
        Map.entry(DataType.STRING, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING),
        Map.entry(DataType.JSON, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_JSON),
        Map.entry(DataType.BYTES, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES),
        Map.entry(DataType.BIG_DECIMAL, FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL));
    dimensionDefaults.forEach((dataType, constant) -> {
      FieldSpec spec = parse(FieldType.DIMENSION, dataType, writtenLiteral(dataType, constant));
      assertSame(spec.getDefaultNullValue(), constant, dataType.name());
      assertEquals(spec.getDefaultNullValueString(), dataType.toString(constant), dataType.name());
      assertEquals(spec, new DimensionFieldSpec("col", dataType, true), dataType.name());
    });

    Map<DataType, Object> metricDefaults = Map.of(
        DataType.INT, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_INT,
        DataType.LONG, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_LONG,
        DataType.FLOAT, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_FLOAT,
        DataType.DOUBLE, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_DOUBLE,
        DataType.BIG_DECIMAL, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_BIG_DECIMAL,
        DataType.STRING, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_STRING,
        DataType.BYTES, FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_BYTES);
    metricDefaults.forEach((dataType, constant) -> {
      FieldSpec spec = parse(FieldType.METRIC, dataType, writtenLiteral(dataType, constant));
      assertSame(spec.getDefaultNullValue(), constant, dataType.name());
      assertEquals(spec.getDefaultNullValueString(), dataType.toString(constant), dataType.name());
      assertEquals(spec, new MetricFieldSpec("col", dataType), dataType.name());
    });
  }

  /// The UUID default is a fresh nil-UUID array per lookup, so there is no constant to share; the literal is still
  /// recognised as the type default (it is dropped rather than retained) and the value stays equal.
  @Test
  public void uuidTypeDefaultStaysValueEqual() {
    String literal = UuidUtils.toString(UuidUtils.nullUuidBytes());
    assertNull(ColumnMetadataImpl.canonicalDefaultNullValue(FieldType.DIMENSION, DataType.UUID, literal));
    FieldSpec spec = parse(FieldType.DIMENSION, DataType.UUID, literal);
    assertEquals((byte[]) spec.getDefaultNullValue(), UuidUtils.nullUuidBytes());
    assertEquals(spec.getDefaultNullValueString(), literal);
    assertEquals(spec, new DimensionFieldSpec("col", DataType.UUID, true));
  }

  /// Custom defaults are parsed exactly as before and equal the spec a table schema builds; the literal itself is
  /// interned so the segments of a table share it.
  @Test
  public void customLiteralsRoundTrip() {
    FieldSpec intSpec = parse(FieldType.DIMENSION, DataType.INT, "-1");
    assertEquals(intSpec, new DimensionFieldSpec("col", DataType.INT, true, -1));
    assertNotSame(intSpec.getDefaultNullValue(), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertEquals(parse(FieldType.DIMENSION, DataType.STRING, "N/A"),
        new DimensionFieldSpec("col", DataType.STRING, true, "N/A"));
    assertEquals(parse(FieldType.DIMENSION, DataType.BYTES, "abcd"),
        new DimensionFieldSpec("col", DataType.BYTES, true, BytesUtils.toBytes("abcd")));
    assertEquals(parse(FieldType.METRIC, DataType.DOUBLE, "1.5"), new MetricFieldSpec("col", DataType.DOUBLE, 1.5));
    // Equality is the data type's own: a BIG_DECIMAL with another scale and a negative zero are not the type default,
    // so their string form survives the round trip.
    FieldSpec scaledZero = parse(FieldType.DIMENSION, DataType.BIG_DECIMAL, "0.0");
    assertNotSame(scaledZero.getDefaultNullValue(), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BIG_DECIMAL);
    assertEquals(scaledZero.getDefaultNullValueString(), "0.0");
    assertEquals(scaledZero, new DimensionFieldSpec("col", DataType.BIG_DECIMAL, true, new BigDecimal("0.0")));
    FieldSpec negativeZero = parse(FieldType.METRIC, DataType.FLOAT, "-0.0");
    assertNotSame(negativeZero.getDefaultNullValue(), FieldSpec.DEFAULT_METRIC_NULL_VALUE_OF_FLOAT);
    assertEquals(negativeZero.getDefaultNullValueString(), "-0.0");

    String literal = ColumnMetadataImpl.canonicalDefaultNullValue(FieldType.DIMENSION, DataType.INT, new String("-1"));
    assertEquals(literal, "-1");
    assertSame(ColumnMetadataImpl.canonicalDefaultNullValue(FieldType.DIMENSION, DataType.INT, new String("-1")),
        literal);
    assertNull(ColumnMetadataImpl.canonicalDefaultNullValue(FieldType.DIMENSION, DataType.INT, null));
  }

  @Test
  public void customStringDefaultsAreSharedAcrossParses() {
    Map<DataType, String> customDefaults = Map.of(DataType.STRING, "N/A", DataType.JSON, "{\"missing\":true}");
    customDefaults.forEach((dataType, literal) -> {
      // Separate segment metadata loads supply distinct strings; STRING and JSON retain the parsed literal itself.
      FieldSpec first = parse(FieldType.DIMENSION, dataType, new String(literal));
      PropertiesConfiguration otherColumn = configFor(FieldType.DIMENSION, dataType, new String(literal));
      otherColumn.setProperty(Column.getKeyFor("col", Column.COLUMN_NAME), "other");
      FieldSpec second = ColumnMetadataImpl.extractFieldSpec("col", otherColumn);
      assertEquals(first, new DimensionFieldSpec("col", dataType, true, literal), dataType.name());
      assertEquals(second, new DimensionFieldSpec("other", dataType, true, literal), dataType.name());
      assertNotSame(first, second, "Distinct specs must still share their custom default literal");
      assertSame(first.getDefaultNullValue(), second.getDefaultNullValue(), dataType.name());
    });
  }

  /// A STRING default with a leading/trailing space or a comma is escaped by the segment creator and recovered here
  /// before the type-default comparison, so it round-trips verbatim.
  @Test
  public void stringDefaultWithSpecialCharactersRoundTrips() {
    String custom = " a,b ";
    FieldSpec spec = parse(FieldType.DIMENSION, DataType.STRING,
        CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(custom));
    assertEquals(spec.getDefaultNullValue(), custom);
    assertEquals(spec, new DimensionFieldSpec("col", DataType.STRING, true, custom));
    FieldSpec typeDefault = parse(FieldType.DIMENSION, DataType.STRING,
        CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(
            FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING));
    assertSame(typeDefault.getDefaultNullValue(), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING);
  }

  @Test
  public void dateTimeDefaultsAndFormatStrings() {
    PropertiesConfiguration config = configFor(FieldType.DATE_TIME, DataType.LONG,
        DataType.LONG.toString(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG));
    DateTimeFieldSpec spec = (DateTimeFieldSpec) ColumnMetadataImpl.extractFieldSpec("col", config);
    assertSame(spec.getDefaultNullValue(), FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG);
    assertEquals(spec, new DateTimeFieldSpec("col", DataType.LONG, DATETIME_FORMAT, DATETIME_GRANULARITY));
    // The format and granularity are stored as fresh strings and come back as the interned instances.
    assertSame(spec.getFormat(), DATETIME_FORMAT);
    assertSame(spec.getGranularity(), DATETIME_GRANULARITY);

    DateTimeFieldSpec custom =
        (DateTimeFieldSpec) ColumnMetadataImpl.extractFieldSpec("col", configFor(FieldType.DATE_TIME, DataType.LONG,
            "0"));
    assertEquals(custom,
        new DateTimeFieldSpec("col", DataType.LONG, DATETIME_FORMAT, DATETIME_GRANULARITY, 0L, null));
    assertEquals(custom.getDefaultNullValue(), 0L);
  }

  /// `/tables/{table}/segments/{segment}/metadata` bean-serializes the FieldSpec, i.e. `getDefaultNullValue()` by
  /// value, so its payload is byte-identical to the one a spec built straight from the literal (the
  /// pre-canonicalization shape) produces, for every type including BYTES and UUID. Only `FieldSpec#toJsonObject()`,
  /// which compares the value against the type default by identity, now omits a redundant BYTES default that used to
  /// be emitted; no endpoint serializes a segment-derived schema that way.
  @Test
  public void segmentMetadataJsonUnchangedByCanonicalization()
      throws Exception {
    for (DataType dataType : new DataType[] {
        DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.BOOLEAN, DataType.TIMESTAMP,
        DataType.STRING, DataType.JSON, DataType.BYTES, DataType.UUID, DataType.BIG_DECIMAL
    }) {
      Object constant = FieldSpec.getDefaultNullValue(FieldType.DIMENSION, dataType, null);
      String literal = dataType.toString(constant);
      FieldSpec parsed = parse(FieldType.DIMENSION, dataType, writtenLiteral(dataType, constant));
      FieldSpec legacy = new DimensionFieldSpec("col", dataType, true, literal);
      assertEquals(JsonUtils.objectToString(parsed), JsonUtils.objectToString(legacy), dataType.name());
      assertEquals(parsed, legacy, dataType.name());
    }
    assertFalse(parse(FieldType.DIMENSION, DataType.BYTES, "").toJsonObject().has("defaultNullValue"));
  }

  /// A combination with no type default (rejected by schema validation, but constructible with an explicit default)
  /// keeps parsing the literal instead of failing on the type-default lookup.
  @Test
  public void literalWithoutTypeDefaultIsKept() {
    String literal =
        ColumnMetadataImpl.canonicalDefaultNullValue(FieldType.METRIC, DataType.BOOLEAN, new String("1"));
    assertEquals(literal, "1");
    assertSame(ColumnMetadataImpl.canonicalDefaultNullValue(FieldType.METRIC, DataType.BOOLEAN, new String("1")),
        literal);
    assertEquals(parse(FieldType.METRIC, DataType.BOOLEAN, "1").getDefaultNullValue(), 1);
  }

  /// Column names and parent names share the same interner across metadata loads.
  @Test
  public void columnNameAndParentColumnAreInterned() {
    PropertiesConfiguration config = baseConfig("metrics$cpu");
    config.setProperty(Column.getKeyFor("metrics$cpu", Column.COLUMN_NAME), new String("cpu"));
    config.setProperty(Column.getKeyFor("metrics$cpu", Column.PARENT_COLUMN), new String("metrics"));

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "metrics$cpu");

    assertEquals(metadata.getFieldSpec().getName(), "cpu");
    assertSame(metadata.getFieldSpec().getName(), ColumnNameInterner.intern(new String("cpu")));
    assertEquals(metadata.getParentColumn(), "metrics");
    assertSame(metadata.getParentColumn(), ColumnNameInterner.intern(new String("metrics")));
    assertNull(ColumnNameInterner.intern(null));
    // Without an explicit COLUMN_NAME the key itself is the name.
    String column = new String("plain");
    FieldSpec spec = ColumnMetadataImpl.extractFieldSpec(column, baseConfigWithoutName(column));
    assertEquals(spec.getName(), "plain");
    assertSame(spec.getName(), ColumnNameInterner.intern(new String("plain")));
  }

  @Test
  public void columnNamesAndDefaultsUseSeparateInterners() {
    String value = "metadata_" + UUID.randomUUID();
    String pooled = new String(value).intern();
    String name = ColumnNameInterner.intern(new String(value));
    String defaultValue =
        ColumnMetadataImpl.canonicalDefaultNullValue(FieldType.DIMENSION, DataType.STRING, new String(value));
    assertEquals(name, value);
    assertEquals(defaultValue, value);
    assertNotSame(name, pooled);
    assertNotSame(defaultValue, pooled);
    assertNotSame(name, defaultValue);
    assertSame(ColumnNameInterner.intern(new String(value)), name);
    assertSame(ColumnMetadataImpl.canonicalDefaultNullValue(FieldType.DIMENSION, DataType.STRING, new String(value)),
        defaultValue);
  }

  @Test
  public void columnNamesAndDefaultsAreSharedDuringConcurrentParsing()
      throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    try {
      List<Future<FieldSpec>> results = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        int maxLength = 100 + i;
        results.add(executor.submit(() -> {
          assertTrue(start.await(10, TimeUnit.SECONDS));
          PropertiesConfiguration config =
              configFor(FieldType.DIMENSION, DataType.STRING, new String("shared-default"));
          config.setProperty(Column.getKeyFor("col", Column.COLUMN_NAME), new String("shared-column"));
          config.setProperty(Column.getKeyFor("col", Column.SCHEMA_MAX_LENGTH), maxLength);
          return ColumnMetadataImpl.extractFieldSpec("col", config);
        }));
      }
      start.countDown();
      FieldSpec first = results.get(0).get(10, TimeUnit.SECONDS);
      for (int i = 1; i < results.size(); i++) {
        FieldSpec other = results.get(i).get(10, TimeUnit.SECONDS);
        assertNotSame(other, first, "Different max lengths must not share a FieldSpec");
        assertSame(other.getName(), first.getName());
        assertSame(other.getDefaultNullValue(), first.getDefaultNullValue());
      }
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
  }

  /// A server retains one FieldSpec per (segment, column) and every segment of a table parses the same column
  /// definition, so the parse path interns the spec: two loads of the same metadata, or of two segments whose column
  /// parses to an equal spec, alias one instance that is still equal to the spec the table schema builds.
  @Test
  public void equalSpecsAreInterned() {
    PropertiesConfiguration config = baseConfig("col");
    FieldSpec spec = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col").getFieldSpec();
    assertSame(ColumnMetadataImpl.fromPropertiesConfiguration(config, 2, "col").getFieldSpec(), spec);
    assertSame(ColumnMetadataImpl.fromPropertiesConfiguration(baseConfig("col"), 3, "col").getFieldSpec(), spec);
    assertEquals(spec, new DimensionFieldSpec("col", DataType.STRING, true));

    // A custom default and a max length are part of the key and are shared as well.
    FieldSpec custom = parse(FieldType.DIMENSION, DataType.INT, "-1");
    assertSame(parse(FieldType.DIMENSION, DataType.INT, "-1"), custom);
    assertEquals(custom, new DimensionFieldSpec("col", DataType.INT, true, -1));
    PropertiesConfiguration bounded = configFor(FieldType.DIMENSION, DataType.STRING, null);
    bounded.setProperty(Column.getKeyFor("col", Column.SCHEMA_MAX_LENGTH), 10);
    FieldSpec boundedSpec = ColumnMetadataImpl.extractFieldSpec("col", bounded);
    assertSame(ColumnMetadataImpl.extractFieldSpec("col", bounded), boundedSpec);
    assertEquals(boundedSpec, new DimensionFieldSpec("col", DataType.STRING, true, 10, null));
  }

  @Test
  public void sharedByteDefaultsRoundTripThroughSchemaJson()
      throws Exception {
    for (DataType dataType : List.of(DataType.BYTES, DataType.UUID)) {
      String typeDefault = dataType.toString(FieldSpec.getDefaultNullValue(FieldType.DIMENSION, dataType, null));
      String customDefault = dataType == DataType.BYTES ? "abcdef" : "123e4567-e89b-12d3-a456-426614174000";
      for (String literal : List.of(typeDefault, customDefault)) {
        FieldSpec parsed = parse(FieldType.DIMENSION, dataType, literal);
        FieldSpec expected = new DimensionFieldSpec("col", dataType, true, literal);
        Schema schema = new Schema();
        schema.addField(parsed);
        Schema copy = JsonUtils.jsonNodeToObject(schema.toJsonObject(), Schema.class);
        FieldSpec copiedSpec = copy.getFieldSpecFor("col");

        assertEquals(copiedSpec, expected, dataType.name());
        assertEquals((byte[]) copiedSpec.getDefaultNullValue(), (byte[]) dataType.convert(literal), dataType.name());
        assertEquals(copiedSpec.getDefaultNullValueString(), literal, dataType.name());
        assertEquals(JsonUtils.objectToString(parsed), JsonUtils.objectToString(expected), dataType.name());
      }
    }
  }

  @Test
  public void jsonCopyOfSharedSpecRemainsMutable()
      throws Exception {
    FieldSpec shared = parse(FieldType.DIMENSION, DataType.INT, "-17");
    int hash = shared.hashCode();
    DimensionFieldSpec copy = JsonUtils.jsonNodeToObject(shared.toJsonObject(), DimensionFieldSpec.class);
    assertEquals(copy, shared);
    assertNotSame(copy, shared);

    copy.setName("other");
    copy.setDefaultNullValue(19);

    assertEquals(copy.getName(), "other");
    assertEquals(copy.getDefaultNullValue(), 19);
    assertEquals(shared.getName(), "col");
    assertEquals(shared.getDefaultNullValue(), -17);
    assertEquals(shared.hashCode(), hash);
    assertSame(parse(FieldType.DIMENSION, DataType.INT, "-17"), shared);
  }

  @Test
  public void metricTimeAndDateTimeSpecsAreInterned() {
    FieldSpec metric = parse(FieldType.METRIC, DataType.LONG, null);
    assertSame(parse(FieldType.METRIC, DataType.LONG, null), metric);
    assertEquals(metric, new MetricFieldSpec("col", DataType.LONG));

    PropertiesConfiguration hours = configFor(FieldType.TIME, DataType.INT, null);
    hours.setProperty(Segment.TIME_UNIT, "HOURS");
    FieldSpec time = ColumnMetadataImpl.extractFieldSpec("col", hours);
    assertSame(ColumnMetadataImpl.extractFieldSpec("col", hours), time);
    assertEquals(time, new TimeFieldSpec(new TimeGranularitySpec(DataType.INT, TimeUnit.HOURS, "col")));
    // The time unit is part of the TimeFieldSpec key.
    PropertiesConfiguration days = configFor(FieldType.TIME, DataType.INT, null);
    days.setProperty(Segment.TIME_UNIT, "DAYS");
    assertNotSame(ColumnMetadataImpl.extractFieldSpec("col", days), time);

    FieldSpec dateTime = parse(FieldType.DATE_TIME, DataType.LONG, null);
    assertSame(parse(FieldType.DATE_TIME, DataType.LONG, null), dateTime);
    assertEquals(dateTime, new DateTimeFieldSpec("col", DataType.LONG, DATETIME_FORMAT, DATETIME_GRANULARITY));
  }

  /// Interning is keyed by [FieldSpec#equals], so a column whose definition changed (schema evolution) parses to a
  /// distinct canonical instance instead of aliasing the previous one.
  @Test
  public void differingSpecsAreNotInterned() {
    FieldSpec base = parse(FieldType.DIMENSION, DataType.INT, null);
    assertNotSame(parse(FieldType.DIMENSION, DataType.INT, "-1"), base, "default null value");
    assertNotSame(parse(FieldType.DIMENSION, DataType.LONG, null), base, "data type");
    assertNotSame(parse(FieldType.METRIC, DataType.INT, null), base, "field type");
    assertNotSame(ColumnMetadataImpl.extractFieldSpec("other", baseConfig("other")),
        ColumnMetadataImpl.extractFieldSpec("col", baseConfig("col")), "name");
    PropertiesConfiguration multiValue = configFor(FieldType.DIMENSION, DataType.INT, null);
    multiValue.setProperty(Column.getKeyFor("col", Column.IS_SINGLE_VALUED), false);
    assertNotSame(ColumnMetadataImpl.extractFieldSpec("col", multiValue), base, "single value");
    PropertiesConfiguration maxLength = configFor(FieldType.DIMENSION, DataType.INT, null);
    maxLength.setProperty(Column.getKeyFor("col", Column.SCHEMA_MAX_LENGTH), 10);
    assertNotSame(ColumnMetadataImpl.extractFieldSpec("col", maxLength), base, "max length");

    FieldSpec dateTime = parse(FieldType.DATE_TIME, DataType.LONG, null);
    PropertiesConfiguration otherFormat = configFor(FieldType.DATE_TIME, DataType.LONG, null);
    otherFormat.setProperty(Column.getKeyFor("col", Column.DATETIME_FORMAT), "1:SECONDS:EPOCH");
    assertNotSame(ColumnMetadataImpl.extractFieldSpec("col", otherFormat), dateTime, "format");
    PropertiesConfiguration otherGranularity = configFor(FieldType.DATE_TIME, DataType.LONG, null);
    otherGranularity.setProperty(Column.getKeyFor("col", Column.DATETIME_GRANULARITY), "1:SECONDS");
    assertNotSame(ColumnMetadataImpl.extractFieldSpec("col", otherGranularity), dateTime, "granularity");
  }

  /// Complex parents retain independent child maps, while equal child specs are shared.
  @Test
  public void complexParentIsNotInternedWhileChildrenAre() {
    PropertiesConfiguration twoChildren = complexConfig("metrics", "cpu", "host");
    ComplexFieldSpec first = (ComplexFieldSpec) ColumnMetadataImpl.extractFieldSpec("metrics", twoChildren);
    ComplexFieldSpec second = (ComplexFieldSpec) ColumnMetadataImpl.extractFieldSpec("metrics", twoChildren);
    ComplexFieldSpec narrower =
        (ComplexFieldSpec) ColumnMetadataImpl.extractFieldSpec("metrics", complexConfig("metrics", "cpu"));
    assertNotSame(second, first);
    assertNotSame(narrower, first);
    assertEquals(second, first);
    assertNotEquals(narrower, first);
    assertEquals(first.getChildFieldSpecs().keySet(), Set.of("cpu", "host"));
    assertEquals(narrower.getChildFieldSpecs().keySet(), Set.of("cpu"));
    assertSame(second.getChildFieldSpec("cpu"), first.getChildFieldSpec("cpu"));
    assertSame(second.getChildFieldSpec("host"), first.getChildFieldSpec("host"));
    assertSame(narrower.getChildFieldSpec("cpu"), first.getChildFieldSpec("cpu"));
    assertEquals(first.getChildFieldSpec("cpu"),
        new DimensionFieldSpec(ComplexFieldSpec.getFullChildName("metrics", "cpu"), DataType.DOUBLE, true));
  }

  /// A COMPLEX parent with DOUBLE children, written the way the segment creator writes it.
  private static PropertiesConfiguration complexConfig(String parent, String... children) {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setProperty(Column.getKeyFor(parent, Column.COLUMN_NAME), parent);
    config.setProperty(Column.getKeyFor(parent, Column.COLUMN_TYPE), FieldType.COMPLEX.name());
    config.setProperty(Column.getKeyFor(parent, Column.DATA_TYPE), DataType.OPEN_STRUCT.name());
    config.setProperty(Column.getKeyFor(parent, Column.IS_SINGLE_VALUED), true);
    config.setProperty(Column.getKeyFor(parent, Column.COMPLEX_CHILD_FIELD_NAMES), List.of(children));
    for (String child : children) {
      String column = ComplexFieldSpec.getFullChildName(parent, child);
      config.setProperty(Column.getKeyFor(column, Column.COLUMN_TYPE), FieldType.DIMENSION.name());
      config.setProperty(Column.getKeyFor(column, Column.DATA_TYPE), DataType.DOUBLE.name());
      config.setProperty(Column.getKeyFor(column, Column.IS_SINGLE_VALUED), true);
    }
    return config;
  }

  private static FieldSpec parse(FieldType fieldType, DataType dataType, @Nullable String defaultNullValue) {
    return ColumnMetadataImpl.extractFieldSpec("col", configFor(fieldType, dataType, defaultNullValue));
  }

  /// The literal the segment creator writes for the given default null value.
  private static String writtenLiteral(DataType dataType, Object defaultNullValue) {
    String literal = dataType.toString(defaultNullValue);
    return dataType.getStoredType() == DataType.STRING
        ? CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(literal) : literal;
  }

  private static PropertiesConfiguration configFor(FieldType fieldType, DataType dataType,
      @Nullable String defaultNullValue) {
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.COLUMN_TYPE), fieldType.name());
    config.setProperty(Column.getKeyFor("col", Column.DATA_TYPE), dataType.name());
    if (defaultNullValue != null) {
      config.setProperty(Column.getKeyFor("col", Column.DEFAULT_NULL_VALUE), defaultNullValue);
    }
    if (fieldType == FieldType.DATE_TIME) {
      config.setProperty(Column.getKeyFor("col", Column.DATETIME_FORMAT), new String(DATETIME_FORMAT));
      config.setProperty(Column.getKeyFor("col", Column.DATETIME_GRANULARITY), new String(DATETIME_GRANULARITY));
    }
    return config;
  }

  @Test
  public void transformFunctionRoundtrip() {
    String transformFunction = "Groovy({x + ',' + y}, x, y)";
    ColumnMetadataImpl metadata = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("col", DataType.STRING, true))
        .setTransformFunction(transformFunction)
        .setTransformFunctionProvenanceVersion(1)
        .build();

    assertEquals(metadata.getTransformFunction(), transformFunction);
    assertEquals(metadata.getTransformFunctionProvenanceVersion(), 1);
  }

  @Test
  public void transformFunctionReadFromBase64PropertiesConfig() {
    String transformFunction = "Groovy({x + '😀' + '${value}, \\' + y}, x, y)";
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.TRANSFORM_FUNCTION_BASE64),
        Base64.getEncoder().encodeToString(transformFunction.getBytes(StandardCharsets.UTF_8)));
    config.setProperty(Column.getKeyFor("col", Column.TRANSFORM_FUNCTION_PROVENANCE_VERSION), 1);

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertEquals(metadata.getTransformFunction(), transformFunction);
    assertEquals(metadata.getTransformFunctionProvenanceVersion(), 1);
  }

  @Test
  public void rawTransformFunctionFallbackSupportsIntermediateArtifacts() {
    String transformFunction = "Groovy({x + ',' + y}, x, y)";
    String escapedTransformFunction =
        CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(transformFunction);
    assertNotNull(escapedTransformFunction);
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.TRANSFORM_FUNCTION), escapedTransformFunction);

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertEquals(metadata.getTransformFunction(), transformFunction);
    assertEquals(metadata.getTransformFunctionProvenanceVersion(), ColumnMetadata.UNAVAILABLE);
  }

  @Test
  public void missingTransformFunctionIsBackwardCompatible() {
    PropertiesConfiguration config = baseConfig("col");

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertNull(metadata.getTransformFunction());
    assertEquals(metadata.getTransformFunctionProvenanceVersion(), ColumnMetadata.UNAVAILABLE);
  }

  private static PropertiesConfiguration baseConfig(String column) {
    PropertiesConfiguration config = baseConfigWithoutName(column);
    config.setProperty(Column.getKeyFor(column, Column.COLUMN_NAME), column);
    return config;
  }

  private static PropertiesConfiguration baseConfigWithoutName(String column) {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setProperty(Column.getKeyFor(column, Column.COLUMN_TYPE), FieldType.DIMENSION.name());
    config.setProperty(Column.getKeyFor(column, Column.DATA_TYPE), DataType.STRING.name());
    config.setProperty(Column.getKeyFor(column, Column.IS_SINGLE_VALUED), true);
    config.setProperty(Column.getKeyFor(column, Column.CARDINALITY), 1);
    return config;
  }
}
