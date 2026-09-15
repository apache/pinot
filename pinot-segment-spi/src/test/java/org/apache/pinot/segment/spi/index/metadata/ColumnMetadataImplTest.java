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
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
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
  // The index-size API works on numeric index ids so this module's tests need no index plugins registered.
  private static final short FORWARD_ID = 2;
  private static final short DICTIONARY_ID = 0;
  private static final short JSON_ID = 5;

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

  @Test
  public void transformFunctionRoundtrip() {
    String transformFunction = "Groovy({x + ',' + y}, x, y)";
    ColumnMetadataImpl meta = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("col", DataType.STRING, true))
        .setTransformFunction(transformFunction)
        .build();

    assertEquals(meta.getTransformFunction(), transformFunction);
  }

  @Test
  public void transformFunctionReadFromPropertiesConfig() {
    String transformFunction = "Groovy({x + ',' + y}, x, y)";
    String escapedTransformFunction =
        CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(transformFunction);
    assertNotNull(escapedTransformFunction);
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.TRANSFORM_FUNCTION), escapedTransformFunction);

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertEquals(metadata.getTransformFunction(), transformFunction);
  }

  @Test
  public void missingTransformFunctionIsBackwardCompatible() {
    PropertiesConfiguration config = baseConfig("col");

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertNull(metadata.getTransformFunction());
    assertNull(metadata.getTransformFunctionBackfilled());
  }

  @Test
  public void storedTransformFunctionIsNotBackfilled() {
    String transformFunction = "plus(col, 1)";
    ColumnMetadataImpl meta = ColumnMetadataImpl.builder()
        .setFieldSpec(new DimensionFieldSpec("col", DataType.INT, true))
        .setTransformFunction(transformFunction)
        .build();

    assertEquals(meta.getTransformFunction(), transformFunction);
    assertNull(meta.getTransformFunctionBackfilled());
  }

  @Test
  public void backfilledTransformFunctionIsNotAStoredTransform() {
    String transformFunction = "plus(col, 1)";
    PropertiesConfiguration config = baseConfig("col");
    String escaped = CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(transformFunction);
    assertNotNull(escaped);
    config.setProperty(Column.getKeyFor("col", Column.TRANSFORM_FUNCTION_BACKFILLED), escaped);

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertNull(metadata.getTransformFunction());
    assertEquals(metadata.getTransformFunctionBackfilled(), transformFunction);
  }

  @Test
  public void legacyBooleanBackfillMarkerReadsExpressionFromTransformFunction() {
    String transformFunction = "Groovy({x + ',' + y}, x, y)";
    String escapedTransformFunction =
        CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(transformFunction);
    assertNotNull(escapedTransformFunction);
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty(Column.getKeyFor("col", Column.TRANSFORM_FUNCTION), escapedTransformFunction);
    config.setProperty(Column.getKeyFor("col", Column.TRANSFORM_FUNCTION_BACKFILLED), "true");

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertNull(metadata.getTransformFunction());
    assertEquals(metadata.getTransformFunctionBackfilled(), transformFunction);
  }

  /// getString() interpolates `${x}` against other keys. Expressions must survive that.
  @Test
  public void transformFunctionWithDollarBraceIsNotInterpolated() {
    String transformFunction = "Groovy({ '${x}' + y }, x, y)";
    String escapedTransformFunction =
        CommonsConfigurationUtils.replaceSpecialCharacterInPropertyValue(transformFunction);
    assertNotNull(escapedTransformFunction);
    PropertiesConfiguration config = baseConfig("col");
    config.setProperty("x", "interpolated");
    config.setProperty(Column.getKeyFor("col", Column.TRANSFORM_FUNCTION), escapedTransformFunction);

    ColumnMetadataImpl metadata = ColumnMetadataImpl.fromPropertiesConfiguration(config, 1, "col");

    assertEquals(metadata.getTransformFunction(), transformFunction);
  }

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

  private static PropertiesConfiguration baseConfig(String column) {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setProperty(Column.getKeyFor(column, Column.COLUMN_NAME), column);
    config.setProperty(Column.getKeyFor(column, Column.COLUMN_TYPE), FieldType.DIMENSION.name());
    config.setProperty(Column.getKeyFor(column, Column.DATA_TYPE), DataType.STRING.name());
    config.setProperty(Column.getKeyFor(column, Column.IS_SINGLE_VALUED), true);
    config.setProperty(Column.getKeyFor(column, Column.CARDINALITY), 1);
    return config;
  }
}
