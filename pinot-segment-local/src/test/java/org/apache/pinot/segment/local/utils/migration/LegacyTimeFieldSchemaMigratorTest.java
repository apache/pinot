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
package org.apache.pinot.segment.local.utils.migration;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.migration.ConfigMigrationRegistry;
import org.apache.pinot.spi.config.migration.MigrationResult;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.MaxLengthExceedStrategy;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies legacy TIME conversion, preservation of field attributes, and rejection of unsupported conversions.
// Intentionally exercises schemas stored with deprecated TIME fields and legacy field transforms.
@SuppressWarnings("deprecation")
public class LegacyTimeFieldSchemaMigratorTest {
  @Test
  public void testConvertsLegacyTimeWithoutMutatingOriginal() {
    TimeFieldSpec time = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "hours"));
    time.setDefaultNullValue(-7L);
    time.setFieldId(19);
    time.setAliases(List.of("eventHours"));
    time.setMetadata(Map.of("owner", "analytics"));
    time.setDescription("event time");
    time.setTags(List.of("time"));
    time.setNotNull(true);
    time.setMaxLength(123);
    time.setMaxLengthExceedStrategy(MaxLengthExceedStrategy.ERROR);
    time.setAllowTrailingZeros(true);
    Schema original = new Schema();
    original.setSchemaName("events");
    original.addField(new DimensionFieldSpec("country", DataType.STRING, true));
    original.addField(time);
    original.setPrimaryKeyColumns(List.of("hours"));

    ConfigMigrationRegistry registry = DefaultConfigMigrationRegistry.create();
    MigrationResult<Schema> result = registry.migrateSchema(original);
    assertTrue(result.isChanged());
    Schema migrated = result.getConfig();
    assertEquals(migrated.getSchemaName(), "events");
    assertNull(migrated.getTimeFieldSpec());
    DateTimeFieldSpec actual = migrated.getDateTimeSpec("hours");
    assertNotNull(actual);
    assertEquals(actual.getDataType(), DataType.LONG);
    assertEquals(actual.getFormat(), "1:HOURS:EPOCH");
    assertEquals(actual.getGranularity(), "1:HOURS");
    assertEquals(actual.getTransformFunction(), "toEpochHours(incoming)");
    assertEquals(actual.getDefaultNullValue(), -7L);
    assertEquals(actual.getFieldId(), Integer.valueOf(19));
    assertEquals(actual.getAliases(), List.of("eventHours"));
    assertEquals(actual.getMetadata(), Map.of("owner", "analytics"));
    assertEquals(actual.getDescription(), "event time");
    assertEquals(actual.getTags(), List.of("time"));
    assertTrue(actual.isNotNull());
    assertEquals(actual.getMaxLength(), Integer.valueOf(123));
    assertEquals(actual.getMaxLengthExceedStrategy(), MaxLengthExceedStrategy.ERROR);
    assertTrue(actual.isAllowTrailingZeros());
    assertEquals(migrated.getPrimaryKeyColumns(), original.getPrimaryKeyColumns());
    assertEquals(migrated.getDimensionFieldSpecs(), original.getDimensionFieldSpecs());
    assertSame(original.getTimeFieldSpec(), time);
    assertEquals(original.getConfigMigrationVersion(), 0);
    assertTrue(original.getDateTimeFieldSpecs().isEmpty());
    assertEquals(migrated.getConfigMigrationVersion(), 1);
    assertFalse(registry.migrateSchema(migrated).isChanged());
  }

  @Test
  public void testPreservesExplicitLegacyTransform() {
    TimeFieldSpec time = new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "hours"));
    time.setTransformFunction("toEpochHours(timestamp)");
    Schema original = new Schema();
    original.setSchemaName("events");
    original.addField(time);
    Schema migrated = DefaultConfigMigrationRegistry.create().migrateSchema(original).getConfig();
    assertEquals(migrated.getDateTimeSpec("hours").getTransformFunction(), "toEpochHours(timestamp)");
  }

  @Test
  public void testModernUnmarkedSchemaIsNotRecommended() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("modern")
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    MigrationResult<Schema> result = DefaultConfigMigrationRegistry.create().migrateSchema(schema);
    assertFalse(result.isChanged());
    assertSame(result.getConfig(), schema);
    assertEquals(schema.getConfigMigrationVersion(), 0);
  }

  @DataProvider
  public Object[][] unsupportedBucketConversions() {
    TimeFieldSpec emptyTransform = new TimeFieldSpec(
        new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.SECONDS, "outgoing"));
    emptyTransform.setTransformFunction("");
    return new Object[][]{
        {new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, 1000, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(DataType.LONG, TimeUnit.SECONDS, "outgoing"))},
        {new TimeFieldSpec(new TimeGranularitySpec(DataType.LONG, TimeUnit.SECONDS, "incoming"),
            new TimeGranularitySpec(DataType.LONG, 1000, TimeUnit.MILLISECONDS, "outgoing"))},
        {emptyTransform}
    };
  }

  @Test(dataProvider = "unsupportedBucketConversions")
  public void testUnsupportedBucketConversionLeavesOriginalUntouched(TimeFieldSpec time) {
    Schema schema = new Schema();
    schema.setSchemaName("nonUnitMillis");
    schema.addField(time);
    assertThrows(IllegalStateException.class, () -> DefaultConfigMigrationRegistry.create().migrateSchema(schema));
    assertSame(schema.getTimeFieldSpec(), time);
    assertEquals(schema.getConfigMigrationVersion(), 0);
    assertTrue(schema.getDateTimeFieldSpecs().isEmpty());
  }

  @Test
  public void testUnsupportedTimeConversionLeavesOriginalUntouched() {
    TimeFieldSpec time = new TimeFieldSpec(
        new TimeGranularitySpec(DataType.STRING, TimeUnit.DAYS, "SIMPLE_DATE_FORMAT:yyyyMMdd", "incoming"),
        new TimeGranularitySpec(DataType.LONG, TimeUnit.HOURS, "hours"));
    Schema schema = new Schema();
    schema.setSchemaName("unsupported");
    schema.addField(time);
    assertThrows(IllegalStateException.class, () -> DefaultConfigMigrationRegistry.create().migrateSchema(schema));
    assertSame(schema.getTimeFieldSpec(), time);
    assertEquals(schema.getConfigMigrationVersion(), 0);
    assertTrue(schema.getDateTimeFieldSpecs().isEmpty());
  }
}
