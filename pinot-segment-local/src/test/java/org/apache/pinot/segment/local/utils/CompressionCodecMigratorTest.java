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
package org.apache.pinot.segment.local.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class CompressionCodecMigratorTest {

  // -------------------------------------------------------------------------
  // toCodecSpec — mapping table
  // -------------------------------------------------------------------------

  @Test
  public void testLz4MapsToLz4() {
    assertEquals(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.LZ4), "LZ4");
  }

  @Test
  public void testZstandardMapsToZstd3() {
    assertEquals(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.ZSTANDARD), "ZSTD(3)");
  }

  @Test
  public void testDeltaMapsToCodecDeltaLz4() {
    assertEquals(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.DELTA), "CODEC(DELTA,LZ4)");
  }

  @Test
  public void testNullReturnsNull() {
    assertNull(CompressionCodecMigrator.toCodecSpec(null));
  }

  @Test
  public void testDeltaDeltaMapsToCodecDeltaDeltaLz4() {
    assertEquals(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.DELTADELTA),
        "CODEC(DELTADELTA,LZ4)");
  }

  @Test
  public void testSnappyAndGzipMigrated() {
    assertEquals(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.SNAPPY), "SNAPPY");
    assertEquals(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.GZIP), "GZIP");
  }

  @Test
  public void testNonMigratableCodecsReturnNull() {
    // These codecs have no codec-pipeline equivalent and must not be migrated
    assertNull(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.PASS_THROUGH));
    assertNull(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.MV_ENTRY_DICT));
    assertNull(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.CLP));
    assertNull(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.CLPV2));
    assertNull(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.CLPV2_ZSTD));
    assertNull(CompressionCodecMigrator.toCodecSpec(FieldConfig.CompressionCodec.CLPV2_LZ4));
  }

  // -------------------------------------------------------------------------
  // isMigratable
  // -------------------------------------------------------------------------

  @Test
  public void testIsMigratable() {
    assertTrue(CompressionCodecMigrator.isMigratable(FieldConfig.CompressionCodec.LZ4));
    assertTrue(CompressionCodecMigrator.isMigratable(FieldConfig.CompressionCodec.ZSTANDARD));
    assertTrue(CompressionCodecMigrator.isMigratable(FieldConfig.CompressionCodec.SNAPPY));
    assertTrue(CompressionCodecMigrator.isMigratable(FieldConfig.CompressionCodec.GZIP));
    assertTrue(CompressionCodecMigrator.isMigratable(FieldConfig.CompressionCodec.DELTA));
    assertTrue(CompressionCodecMigrator.isMigratable(FieldConfig.CompressionCodec.DELTADELTA));

    assertFalse(CompressionCodecMigrator.isMigratable(null));
    assertFalse(CompressionCodecMigrator.isMigratable(FieldConfig.CompressionCodec.PASS_THROUGH));
    assertFalse(CompressionCodecMigrator.isMigratable(FieldConfig.CompressionCodec.CLP));
  }

  // -------------------------------------------------------------------------
  // migrate(FieldConfig)
  // -------------------------------------------------------------------------

  @Test
  public void testMigrateZstandardFieldConfig() {
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD)
        .build();

    FieldConfig migrated = CompressionCodecMigrator.migrate(original);

    assertNull(migrated.getCompressionCodec(), "compressionCodec must be cleared");
    assertEquals(codecSpecOf(migrated), "ZSTD(3)");
    assertEquals(migrated.getName(), "col");
    assertEquals(migrated.getEncodingType(), FieldConfig.EncodingType.RAW);
  }

  @Test
  public void testMigrateLz4FieldConfig() {
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.LZ4)
        .build();

    FieldConfig migrated = CompressionCodecMigrator.migrate(original);

    assertNull(migrated.getCompressionCodec());
    assertEquals(codecSpecOf(migrated), "LZ4");
  }

  @Test
  public void testMigrateDeltaFieldConfig() {
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.DELTA)
        .build();

    FieldConfig migrated = CompressionCodecMigrator.migrate(original);

    assertNull(migrated.getCompressionCodec());
    assertEquals(codecSpecOf(migrated), "CODEC(DELTA,LZ4)");
  }

  @Test
  public void testMigrateDeltaDeltaFieldConfig() {
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.DELTADELTA)
        .build();

    FieldConfig migrated = CompressionCodecMigrator.migrate(original);

    assertNull(migrated.getCompressionCodec());
    assertEquals(codecSpecOf(migrated), "CODEC(DELTADELTA,LZ4)");
  }

  @Test
  public void testMigrateNonMigratableReturnsSameInstance() {
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.PASS_THROUGH)
        .build();

    assertSame(CompressionCodecMigrator.migrate(original), original,
        "Non-migratable FieldConfig must be returned unchanged (same instance)");
  }

  @Test
  public void testMigrateAlreadyMigratedReturnsSameInstance() {
    // A FieldConfig that already uses codecSpec (in indexes.forward) and has no legacy
    // compressionCodec should pass through unchanged.
    ObjectNode forward = JsonUtils.newObjectNode();
    forward.put("codecSpec", "ZSTD(3)");
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("forward", forward);
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();

    assertSame(CompressionCodecMigrator.migrate(original), original,
        "FieldConfig with codecSpec and no compressionCodec must be returned unchanged");
  }

  @Test
  public void testMigrateClearsInnerCompressionCodec()
      throws Exception {
    // A column with a top-level compressionCodec AND an inner indexes.forward.compressionCodec.
    // After migration the injected codecSpec and the inner compressionCodec would be mutually
    // exclusive, so the migrator must drop the inner compressionCodec to keep the result
    // deserializable on reload.
    ObjectNode forward = JsonUtils.newObjectNode();
    forward.put("compressionCodec", "SNAPPY");
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("forward", forward);
    FieldConfig original = new FieldConfig.Builder("col")
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD)
        .withIndexes(indexes)
        .build();

    FieldConfig migrated = CompressionCodecMigrator.migrate(original);

    assertNull(migrated.getCompressionCodec(), "top-level compressionCodec must be cleared");
    JsonNode migratedForward = migrated.getIndexes().get("forward");
    assertEquals(migratedForward.get("codecSpec").asText(), "ZSTD(3)");
    assertNull(migratedForward.get("compressionCodec"),
        "inner indexes.forward.compressionCodec must be dropped (mutually exclusive with codecSpec)");

    // Must round-trip through JSON (and thus ForwardIndexConfig mutual-exclusion) without throwing.
    String json = JsonUtils.objectToString(migrated);
    assertEquals(codecSpecOf(JsonUtils.stringToObject(json, FieldConfig.class)), "ZSTD(3)");
  }

  // -------------------------------------------------------------------------
  // migrateTableConfig
  // -------------------------------------------------------------------------

  @Test
  public void testMigrateTableConfigPartial() {
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig.Builder("zstdCol").withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD).build(),
        new FieldConfig.Builder("snappyCol").withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.SNAPPY).build(),
        new FieldConfig.Builder("deltaCol").withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.DELTA).build()
    );

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setFieldConfigList(fieldConfigs).build();

    TableConfig migrated = CompressionCodecMigrator.migrateTableConfig(tableConfig);

    List<FieldConfig> result = migrated.getFieldConfigList();
    assertEquals(result.size(), 3);

    // zstdCol migrated
    assertEquals(result.get(0).getName(), "zstdCol");
    assertNull(result.get(0).getCompressionCodec());
    assertEquals(codecSpecOf(result.get(0)), "ZSTD(3)");

    // snappyCol migrated (compression-only specs are supported on all raw forward-index shapes
    // that support the equivalent legacy compression codec).
    assertEquals(result.get(1).getName(), "snappyCol");
    assertNull(result.get(1).getCompressionCodec());
    assertEquals(codecSpecOf(result.get(1)), "SNAPPY");

    // deltaCol migrated
    assertEquals(result.get(2).getName(), "deltaCol");
    assertNull(result.get(2).getCompressionCodec());
    assertEquals(codecSpecOf(result.get(2)), "CODEC(DELTA,LZ4)");
  }

  @Test
  public void testMigrateTableConfigAllNonMigratableReturnsSameInstance() {
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig.Builder("col").withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.PASS_THROUGH).build()
    );

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setFieldConfigList(fieldConfigs).build();

    assertSame(CompressionCodecMigrator.migrateTableConfig(tableConfig), tableConfig,
        "TableConfig with no migratable fields must be returned unchanged");
  }

  @Test
  public void testMigrateTableConfigNullFieldConfigList() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t").build();
    assertSame(CompressionCodecMigrator.migrateTableConfig(tableConfig), tableConfig);
  }

  // -------------------------------------------------------------------------
  // isMigratableWithSchema — schema-aware transform guard
  // -------------------------------------------------------------------------

  @Test
  public void testIsMigratableWithSchemaSvIntIsMigratable() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("col", FieldSpec.DataType.INT)
        .build();
    FieldConfig fc = new FieldConfig.Builder("col").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.LZ4).build();
    assertTrue(CompressionCodecMigrator.isMigratableWithSchema(fc, schema));
  }

  @Test
  public void testIsMigratableWithSchemaSvLongIsMigratable() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("col", FieldSpec.DataType.LONG)
        .build();
    FieldConfig fc = new FieldConfig.Builder("col").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD).build();
    assertTrue(CompressionCodecMigrator.isMigratableWithSchema(fc, schema));
  }

  @Test
  public void testIsMigratableWithSchemaSvFloatCompressionIsMigratable() {
    Schema schema = new Schema.SchemaBuilder()
        .addMetric("col", FieldSpec.DataType.FLOAT)
        .build();
    FieldConfig fc = new FieldConfig.Builder("col").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.LZ4).build();
    assertTrue(CompressionCodecMigrator.isMigratableWithSchema(fc, schema));
  }

  @Test
  public void testIsMigratableWithSchemaSvDoubleCompressionIsMigratable() {
    Schema schema = new Schema.SchemaBuilder()
        .addMetric("col", FieldSpec.DataType.DOUBLE)
        .build();
    FieldConfig fc = new FieldConfig.Builder("col").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.LZ4).build();
    assertTrue(CompressionCodecMigrator.isMigratableWithSchema(fc, schema));
  }

  @Test
  public void testIsMigratableWithSchemaSvStringCompressionIsMigratable() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("col", FieldSpec.DataType.STRING)
        .build();
    FieldConfig fc = new FieldConfig.Builder("col").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD).build();
    assertTrue(CompressionCodecMigrator.isMigratableWithSchema(fc, schema));
  }

  @Test
  public void testIsMigratableWithSchemaMvCompressionIsMigratable() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("mvCol", FieldSpec.DataType.INT)
        .build();
    FieldConfig fc = new FieldConfig.Builder("mvCol").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.LZ4).build();
    assertTrue(CompressionCodecMigrator.isMigratableWithSchema(fc, schema));
  }

  @Test
  public void testIsMigratableWithSchemaMvTransformNotMigratable() {
    Schema schema = new Schema.SchemaBuilder()
        .addMultiValueDimension("mvCol", FieldSpec.DataType.INT)
        .build();
    FieldConfig fc = new FieldConfig.Builder("mvCol").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.DELTA).build();
    assertFalse(CompressionCodecMigrator.isMigratableWithSchema(fc, schema));
  }

  @Test
  public void testIsMigratableWithSchemaNullSchemaFallsBackToTypeAgnostic() {
    // When schema is null, falls back to the type-agnostic check — LZ4 is migratable
    FieldConfig fc = new FieldConfig.Builder("col").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.LZ4).build();
    assertTrue(CompressionCodecMigrator.isMigratableWithSchema(fc, null));
  }

  @Test
  public void testIsMigratableWithSchemaColumnNotInSchemaNonMigratable() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("otherCol", FieldSpec.DataType.INT)
        .build();
    FieldConfig fc = new FieldConfig.Builder("missingCol").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.LZ4).build();
    assertFalse(CompressionCodecMigrator.isMigratableWithSchema(fc, schema));
  }

  // -------------------------------------------------------------------------
  // migrateTableConfig(TableConfig, Schema)
  // -------------------------------------------------------------------------

  @Test
  public void testMigrateTableConfigWithSchemaSkipsUnsupportedTransforms() {
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("longCol", FieldSpec.DataType.LONG)
        .addMetric("floatCol", FieldSpec.DataType.FLOAT)
        .addSingleValueDimension("stringCol", FieldSpec.DataType.STRING)
        .addMultiValueDimension("mvCol", FieldSpec.DataType.INT)
        .build();

    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig.Builder("longCol").withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.LZ4).build(),
        new FieldConfig.Builder("floatCol").withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.LZ4).build(),
        new FieldConfig.Builder("stringCol").withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD).build(),
        new FieldConfig.Builder("mvCol").withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.DELTA).build()
    );

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setFieldConfigList(fieldConfigs).build();

    TableConfig migrated = CompressionCodecMigrator.migrateTableConfig(tableConfig, schema);
    List<FieldConfig> result = migrated.getFieldConfigList();
    assertEquals(result.size(), 4);

    // longCol (SV LONG) — migrated
    assertEquals(result.get(0).getName(), "longCol");
    assertNull(result.get(0).getCompressionCodec());
    assertEquals(codecSpecOf(result.get(0)), "LZ4");

    // floatCol (SV FLOAT compression-only) — migrated
    assertEquals(result.get(1).getName(), "floatCol");
    assertNull(result.get(1).getCompressionCodec());
    assertEquals(codecSpecOf(result.get(1)), "LZ4");

    // stringCol (SV STRING compression-only) — migrated
    assertEquals(result.get(2).getName(), "stringCol");
    assertNull(result.get(2).getCompressionCodec());
    assertEquals(codecSpecOf(result.get(2)), "ZSTD(3)");

    // mvCol (MV transform) — NOT migrated
    assertEquals(result.get(3).getName(), "mvCol");
    assertEquals(result.get(3).getCompressionCodec(), FieldConfig.CompressionCodec.DELTA);
    assertNull(codecSpecOf(result.get(3)));
  }

  @Test
  public void testMigrateTableConfigWithSchemaNullSchemaMatchesTypeAgnostic() {
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig.Builder("col").withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD).build()
    );
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("t")
        .setFieldConfigList(fieldConfigs).build();

    TableConfig migrated = CompressionCodecMigrator.migrateTableConfig(tableConfig, null);
    assertEquals(codecSpecOf(migrated.getFieldConfigList().get(0)), "ZSTD(3)");
  }

  /// Reads the codecSpec from the modern `indexes.forward` block (the only supported location;
  /// there is no top-level FieldConfig.codecSpec field). Returns `null` if absent.
  @Nullable
  private static String codecSpecOf(FieldConfig fieldConfig) {
    JsonNode forward = fieldConfig.getIndexes().get("forward");
    if (forward == null || forward.isNull()) {
      return null;
    }
    JsonNode codecSpec = forward.get("codecSpec");
    return codecSpec == null || codecSpec.isNull() ? null : codecSpec.asText();
  }
}
