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
package org.apache.pinot.segment.local.segment.index.nullvalue;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class NullValueVectorHandlerTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "NullValueVectorHandlerTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String SV_INT_COLUMN = "svInt";
  private static final String SV_STRING_COLUMN = "svString";
  private static final String MV_INT_COLUMN = "mvInt";
  private static final String SV_LONG_COLUMN = "svLong";
  // Opted out of backfill even though it also holds the sentinel default value, to verify selectivity.
  private static final String SV_INT_NO_OPT_IN_COLUMN = "svIntNoOptIn";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(SV_INT_COLUMN, DataType.INT)
      .addSingleValueDimension(SV_STRING_COLUMN, DataType.STRING)
      .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
      .addSingleValueDimension(SV_LONG_COLUMN, DataType.LONG)
      .addSingleValueDimension(SV_INT_NO_OPT_IN_COLUMN, DataType.INT)
      .build();

  // Rows carry the default null values inline (as a pre-null-handling segment would), so the segment is built without a
  // null value vector. Dimension defaults: INT -> Integer.MIN_VALUE, STRING -> "null". A null multi-value entry is
  // stored as a single-element array holding the default null value. svLong holds no default null value.
  private static final List<GenericRow> ROWS = List.of(
      createRow(1, "a", new Object[]{10, 20}, 100L, 7),
      createRow(Integer.MIN_VALUE, "null", new Object[]{Integer.MIN_VALUE}, 200L, Integer.MIN_VALUE),
      createRow(3, "null", new Object[]{Integer.MIN_VALUE, 5}, 300L, 9)
  );

  private static GenericRow createRow(int svInt, String svString, Object[] mvInt, long svLong, int svIntNoOptIn) {
    GenericRow row = new GenericRow();
    row.putValue(SV_INT_COLUMN, svInt);
    row.putValue(SV_STRING_COLUMN, svString);
    row.putValue(MV_INT_COLUMN, mvInt);
    row.putValue(SV_LONG_COLUMN, svLong);
    row.putValue(SV_INT_NO_OPT_IN_COLUMN, svIntNoOptIn);
    return row;
  }

  /// Builds a row for the creation-path test. A `null` svInt marks that cell as null so ingestion records it in the
  /// null value vector; all other columns are non-null.
  private static GenericRow creationRow(Integer svInt, long svLong) {
    GenericRow row = new GenericRow();
    row.putValue(SV_INT_COLUMN, svInt);
    row.putValue(SV_STRING_COLUMN, "s");
    row.putValue(MV_INT_COLUMN, new Object[]{1, 2});
    row.putValue(SV_LONG_COLUMN, svLong);
    row.putValue(SV_INT_NO_OPT_IN_COLUMN, 5);
    return row;
  }

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    // Build the segment without null handling, so no null value vector is created.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(ROWS));
    driver.build();
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private static FieldConfig backfillOptIn(String column)
      throws Exception {
    // Opt into backfill via the null value vector index config: indexes.null.backfill = true.
    return new FieldConfig.Builder(column).withIndexes(JsonUtils.stringToJsonNode("{\"null\": {\"backfill\": true}}"))
        .build();
  }

  @Test
  public void testBackfillNullVectorForOptedInColumns()
      throws Exception {
    // Opt in every column except SV_INT_NO_OPT_IN_COLUMN.
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(
            List.of(backfillOptIn(SV_INT_COLUMN), backfillOptIn(SV_STRING_COLUMN), backfillOptIn(MV_INT_COLUMN),
                backfillOptIn(SV_LONG_COLUMN)))
        .build();

    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(TEMP_DIR, SEGMENT_NAME), new IndexLoadingConfig(tableConfig, SCHEMA));
    try {
      // svInt: only the Integer.MIN_VALUE row is null.
      assertNullDocIds(segment, SV_INT_COLUMN, false, true, false);
      // svString: both "null" rows are null.
      assertNullDocIds(segment, SV_STRING_COLUMN, false, true, true);
      // mvInt: only the single-element {MIN_VALUE} row is null; multi-element rows are not.
      assertNullDocIds(segment, MV_INT_COLUMN, false, true, false);
      // svLong: no value equals the default null value, so no bitmap file is written (matching segment creation).
      assertNullDocIds(segment, SV_LONG_COLUMN, false, false, false);

      // Columns with null values carry a bitmap file and are not flagged non-null; the null-free column is flagged.
      for (String column : List.of(SV_INT_COLUMN, SV_STRING_COLUMN, MV_INT_COLUMN)) {
        assertNotNull(segment.getDataSource(column).getNullValueVector(),
            "Column with null values should have a bitmap file: " + column);
        assertFalse(segment.getSegmentMetadata().getColumnMetadataFor(column).isNonNull(),
            "Column with null values should not be flagged non-null: " + column);
      }
      assertTrue(segment.getSegmentMetadata().getColumnMetadataFor(SV_LONG_COLUMN).isNonNull(),
          "Null-free column should be flagged non-null (idempotency signal)");

      // The un-opted column holds the sentinel default value too, but must be left untouched.
      assertNullDocIds(segment, SV_INT_NO_OPT_IN_COLUMN, false, false, false);
      assertNull(segment.getDataSource(SV_INT_NO_OPT_IN_COLUMN).getNullValueVector(),
          "Column not opted into backfill must not get a null value vector");
      assertFalse(segment.getSegmentMetadata().getColumnMetadataFor(SV_INT_NO_OPT_IN_COLUMN).isNonNull(),
          "Column not opted into backfill must not be flagged");
    } finally {
      segment.destroy();
    }
  }

  /// A non-default value per data type, in the form ingestion accepts. Covers every backfill-supported stored type
  /// (`INT`, `LONG`, `FLOAT`, `DOUBLE`, `BIG_DECIMAL`, `STRING`, `BYTES`) plus every logical type that maps onto one of
  /// them (`BOOLEAN`->`INT`, `TIMESTAMP`->`LONG`, `JSON`->`STRING`, `UUID`->`BYTES`), so both the per-type comparison
  /// branches and the default-null-value round-trip through segment metadata are exercised.
  private static Map<DataType, Object> nonDefaultValues() {
    Map<DataType, Object> values = new LinkedHashMap<>();
    values.put(DataType.INT, 1);
    values.put(DataType.LONG, 1L);
    values.put(DataType.FLOAT, 1.5f);
    values.put(DataType.DOUBLE, 2.5d);
    values.put(DataType.BIG_DECIMAL, BigDecimal.ONE);
    values.put(DataType.BOOLEAN, true);
    values.put(DataType.TIMESTAMP, 1000L);
    values.put(DataType.STRING, "a");
    values.put(DataType.JSON, "{\"a\":1}");
    values.put(DataType.BYTES, new byte[]{1, 2});
    values.put(DataType.UUID, "11111111-2222-3333-4444-555555555555");
    return values;
  }

  /// Data types allowed for a metric field. Their default null values differ from the dimension defaults for
  /// `INT`/`LONG`/`FLOAT`/`DOUBLE` (`0`/`0.0` instead of `MIN_VALUE`/`-Infinity`), so they are covered separately.
  private static final List<DataType> METRIC_TYPES =
      List.of(DataType.INT, DataType.LONG, DataType.FLOAT, DataType.DOUBLE, DataType.BIG_DECIMAL, DataType.BYTES);

  /// `JSON` is single-value only; `SchemaUtils.validateMultiValueCompatibility` rejects an MV JSON column.
  private static final Set<DataType> MV_UNSUPPORTED_TYPES = Set.of(DataType.JSON);

  private static String svColumn(DataType dataType) {
    return "sv" + dataType.name();
  }

  private static String mvColumn(DataType dataType) {
    return "mv" + dataType.name();
  }

  private static String metricColumn(DataType dataType) {
    return "me" + dataType.name();
  }

  @Test
  public void testBackfillAllSupportedStoredTypes()
      throws Exception {
    // One SV and one MV dimension column per data type, plus one metric column per metric-allowed type (metric
    // defaults differ), so every per-type comparison branch is exercised against every default null value. Row 0 holds
    // a non-default value; row 1 leaves every cell null so the ingestion pipeline substitutes the column's default null
    // value (and, for MV, the single-element array holding it) exactly as it would for a pre-null-handling segment.
    // Row 1's MV entries are therefore single-element, so the element comparison (not just the length check) is
    // exercised in both directions.
    Map<DataType, Object> nonDefaultValues = nonDefaultValues();
    Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME);
    for (DataType dataType : nonDefaultValues.keySet()) {
      schemaBuilder.addSingleValueDimension(svColumn(dataType), dataType);
      if (!MV_UNSUPPORTED_TYPES.contains(dataType)) {
        schemaBuilder.addMultiValueDimension(mvColumn(dataType), dataType);
      }
    }
    // Metric columns are single-value only, and carry different default null values than dimensions.
    for (DataType dataType : METRIC_TYPES) {
      schemaBuilder.addMetric(metricColumn(dataType), dataType);
    }
    Schema schema = schemaBuilder.build();

    GenericRow nonNullRow = new GenericRow();
    GenericRow nullRow = new GenericRow();
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    for (Map.Entry<DataType, Object> entry : nonDefaultValues.entrySet()) {
      DataType dataType = entry.getKey();
      Object nonDefaultValue = entry.getValue();
      String svColumn = svColumn(dataType);
      nonNullRow.putValue(svColumn, nonDefaultValue);
      nullRow.putValue(svColumn, null);
      fieldConfigs.add(backfillOptIn(svColumn));
      if (!MV_UNSUPPORTED_TYPES.contains(dataType)) {
        String mvColumn = mvColumn(dataType);
        nonNullRow.putValue(mvColumn, new Object[]{nonDefaultValue});
        nullRow.putValue(mvColumn, null);
        fieldConfigs.add(backfillOptIn(mvColumn));
      }
    }
    for (DataType dataType : METRIC_TYPES) {
      String metricColumn = metricColumn(dataType);
      nonNullRow.putValue(metricColumn, nonDefaultValues.get(dataType));
      nullRow.putValue(metricColumn, null);
      fieldConfigs.add(backfillOptIn(metricColumn));
    }

    // Build without null handling, so no null value vector exists.
    String segmentName = "allTypesSegment";
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build(), schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(nonNullRow, nullRow)));
    driver.build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(fieldConfigs)
        .build();
    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), new IndexLoadingConfig(tableConfig, schema));
    try {
      for (DataType dataType : nonDefaultValues.keySet()) {
        assertNullDocIds(segment, svColumn(dataType), false, true);
        if (!MV_UNSUPPORTED_TYPES.contains(dataType)) {
          assertNullDocIds(segment, mvColumn(dataType), false, true);
        }
      }
      for (DataType dataType : METRIC_TYPES) {
        assertNullDocIds(segment, metricColumn(dataType), false, true);
      }
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testBackfillWithConfiguredDefaultNullValues()
      throws Exception {
    // Columns with an explicit (non-default) default null value in the schema — the recommended way to use backfill,
    // since the sentinel can be chosen so it does not occur in the data. This takes a different resolution path than
    // the built-in defaults: the configured value is persisted as the column's DEFAULT_NULL_VALUE and parsed back via
    // DataType.convert when the segment metadata is read. One column per distinct comparison mechanism, plus MV.
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension("cfgInt", DataType.INT, -1)
        .addMultiValueDimension("cfgIntMv", DataType.INT, -1)
        .addSingleValueDimension("cfgDouble", DataType.DOUBLE, -1.5d)
        // The configured scale (-1.50) differs from the stored scale (-1.5, trailing zeros stripped at ingestion), so
        // this only matches because the comparison uses BigDecimal.compareTo rather than equals.
        .addSingleValueDimension("cfgBigDecimal", DataType.BIG_DECIMAL, new BigDecimal("-1.50"))
        .addSingleValueDimension("cfgString", DataType.STRING, "N/A")
        .addSingleValueDimension("cfgBytes", DataType.BYTES, new byte[]{9})
        .addSingleValueDimension("cfgTimestamp", DataType.TIMESTAMP, 1600000000000L)
        .build();
    List<String> columns =
        List.of("cfgInt", "cfgIntMv", "cfgDouble", "cfgBigDecimal", "cfgString", "cfgBytes", "cfgTimestamp");

    GenericRow nonNullRow = new GenericRow();
    nonNullRow.putValue("cfgInt", 1);
    nonNullRow.putValue("cfgIntMv", new Object[]{1});
    nonNullRow.putValue("cfgDouble", 2.5d);
    nonNullRow.putValue("cfgBigDecimal", BigDecimal.ONE);
    nonNullRow.putValue("cfgString", "a");
    nonNullRow.putValue("cfgBytes", new byte[]{1, 2});
    nonNullRow.putValue("cfgTimestamp", 1700000000000L);
    // Leave every cell null so ingestion substitutes each column's configured default null value.
    GenericRow nullRow = new GenericRow();
    for (String column : columns) {
      nullRow.putValue(column, null);
    }

    String segmentName = "configuredDefaultsSegment";
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build(), schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(nonNullRow, nullRow)));
    driver.build();

    List<FieldConfig> fieldConfigs = new ArrayList<>();
    for (String column : columns) {
      fieldConfigs.add(backfillOptIn(column));
    }
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(fieldConfigs)
        .build();
    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), new IndexLoadingConfig(tableConfig, schema));
    try {
      for (String column : columns) {
        assertNullDocIds(segment, column, false, true);
      }
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testBackfillOnTimeColumn()
      throws Exception {
    // The time column is the one column whose stored null value is not always its default: ingestion substitutes the
    // current time when the configured default is outside the valid time range (see NullValueTransformerUtils), which
    // is why TableConfigUtils rejects a backfill opt-in in that case. Here the default is deliberately in range, so
    // ingestion stores it as-is and backfill can match it -- the only time-column configuration backfill supports.
    String timeColumn = "ts";
    long inRangeDefault = 1600000000000L;
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addDateTime(timeColumn, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS", inRangeDefault, null)
        .build();

    GenericRow nonNullRow = new GenericRow();
    nonNullRow.putValue(timeColumn, 1700000000000L);
    GenericRow nullRow = new GenericRow();
    nullRow.putValue(timeColumn, null);

    // Build without null handling, so no null value vector exists and the null row stores the configured default.
    String segmentName = "timeColumnSegment";
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
            .setTimeColumnName(timeColumn)
            .build(), schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(nonNullRow, nullRow)));
    driver.build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setTimeColumnName(timeColumn)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(backfillOptIn(timeColumn)))
        .build();
    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), new IndexLoadingConfig(tableConfig, schema));
    try {
      assertNullDocIds(segment, timeColumn, false, true);
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testNonNullFlagWrittenAtCreation()
      throws Exception {
    // Build a fresh segment WITH null handling enabled (no backfill opt-in): svInt has a null value, the others do not.
    String segmentName = "creationSegment";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNullHandlingEnabled(true).build();
    List<GenericRow> rows = List.of(creationRow(1, 100L), creationRow(null, 200L), creationRow(3, 300L));
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(TEMP_DIR, segmentName), new IndexLoadingConfig(tableConfig, SCHEMA));
    try {
      // svInt has a null -> a bitmap file is written at creation and the column is not flagged non-null.
      assertNotNull(segment.getDataSource(SV_INT_COLUMN).getNullValueVector());
      assertTrue(segment.getDataSource(SV_INT_COLUMN).getNullValueVector().isNull(1));
      assertFalse(segment.getSegmentMetadata().getColumnMetadataFor(SV_INT_COLUMN).isNonNull());
      // svLong has no nulls -> no bitmap file, and the column is flagged non-null at creation.
      assertNull(segment.getDataSource(SV_LONG_COLUMN).getNullValueVector());
      assertTrue(segment.getSegmentMetadata().getColumnMetadataFor(SV_LONG_COLUMN).isNonNull());
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testBackfillIsIdempotent()
      throws Exception {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(backfillOptIn(SV_INT_COLUMN), backfillOptIn(SV_LONG_COLUMN)))
        .build();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
    File segmentDir = new File(TEMP_DIR, SEGMENT_NAME);

    // First reload backfills: svInt gets a bitmap file (has the sentinel value), svLong gets the non-null flag.
    ImmutableSegmentLoader.load(segmentDir, indexLoadingConfig).destroy();

    // A second reload must find nothing left to backfill for either column.
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      NullValueVectorHandler handler = new NullValueVectorHandler(segmentDirectory,
          indexLoadingConfig.getFieldIndexConfigByColName(), tableConfig, SCHEMA);
      assertFalse(handler.needUpdateIndices(reader));
    }
  }

  @Test
  public void testColumnMissingFromSegmentIsSkipped()
      throws Exception {
    // A column present in the schema but not yet in the segment (normally materialized by DefaultColumnHandler before
    // the index handlers run) must be skipped rather than fail when the handler is invoked on its own.
    String missingColumn = "svIntMissing";
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(SV_INT_COLUMN, DataType.INT)
        .addSingleValueDimension(SV_STRING_COLUMN, DataType.STRING)
        .addMultiValueDimension(MV_INT_COLUMN, DataType.INT)
        .addSingleValueDimension(SV_LONG_COLUMN, DataType.LONG)
        .addSingleValueDimension(SV_INT_NO_OPT_IN_COLUMN, DataType.INT)
        .addSingleValueDimension(missingColumn, DataType.INT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNullHandlingEnabled(true)
        .setFieldConfigList(List.of(backfillOptIn(missingColumn)))
        .build();
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, schema);

    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(new File(TEMP_DIR, SEGMENT_NAME),
        ReadMode.mmap); SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      NullValueVectorHandler handler = new NullValueVectorHandler(segmentDirectory,
          indexLoadingConfig.getFieldIndexConfigByColName(), tableConfig, schema);
      assertFalse(handler.needUpdateIndices(reader),
          "A column missing from the segment must not be reported as needing a backfill");
    }
  }

  @Test
  public void testNoBackfillWithoutOptIn()
      throws Exception {
    // Null handling on, but no column opts into backfill: no null value vector should be generated.
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNullHandlingEnabled(true).build();

    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(TEMP_DIR, SEGMENT_NAME), new IndexLoadingConfig(tableConfig, SCHEMA));
    try {
      assertNullDocIds(segment, SV_INT_COLUMN, false, false, false);
      assertNullDocIds(segment, SV_STRING_COLUMN, false, false, false);
      assertNullDocIds(segment, MV_INT_COLUMN, false, false, false);
      assertNull(segment.getDataSource(SV_INT_COLUMN).getNullValueVector(),
          "No null value vector should be generated without opt-in");
    } finally {
      segment.destroy();
    }
  }

  private static void assertNullDocIds(ImmutableSegment segment, String column, boolean... expectedNull)
      throws Exception {
    try (PinotSegmentColumnReader columnReader = new PinotSegmentColumnReader(segment, column)) {
      assertEquals(segment.getSegmentMetadata().getTotalDocs(), expectedNull.length);
      for (int docId = 0; docId < expectedNull.length; docId++) {
        assertEquals(columnReader.isNull(docId), expectedNull[docId],
            "Unexpected null status for column: " + column + ", docId: " + docId);
      }
    }
  }
}
