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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReaderV7;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Integration test for forward-index `codecSpec` handling.
///
/// Builds an offline table where representative pipelines covering all eight built-in codecs are
/// applied to INT and LONG raw columns. All codec specs, including single-stage compression, use the
/// V7 forward-index format. All INT/LONG columns are populated with identical values
/// (intVal = i, longVal = i * 1_000_000_000L), so every codec must read back the same aggregates,
/// filter counts, and point lookups. A STRING dictionary column verifies that codec-spec raw columns
/// and dictionary-encoded columns coexist in the same segment.
///
/// Codec specs covered (each as its own column):
///
/// - `LZ4`, `ZSTD(3)`, `SNAPPY`, `GZIP` — single-stage compression
/// - `DELTA,LZ4`, `DELTA,ZSTD(3)`, `DELTA,SNAPPY`, `DELTA,GZIP` — DELTA transform + compression
/// - `DELTADELTA,LZ4` — second-order DELTA transform + compression
/// - `T64`, `GORILLA` — packing transforms without compression
/// - `T64,LZ4`, `GORILLA,ZSTD(3)` — packing transform + compression
@Test(suiteName = "CustomClusterIntegrationTest")
public class CodecPipelineIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  private static final String TABLE_NAME = "CodecPipelineIntegrationTest";
  private static final int NUM_DOCS = 1000;
  private static final int V7_TARGET_DOCS_PER_CHUNK = 256;
  private static final int[] POINT_LOOKUP_IDS = {0, 1, 510, 511, 512, 513, 999};
  private static final String POINT_LOOKUP_ID_LIST = "0, 1, 510, 511, 512, 513, 999";

  private static final String STR_COL = "strVal";
  private static final String TIME_COL = "ts";

  // Expected aggregates: SUM(0..999) = 499_500
  private static final long EXPECTED_INT_SUM = 499_500L;
  private static final long EXPECTED_LONG_SUM = 499_500L * 1_000_000_000L;

  /// Codec spec → column-name suffix. Each codec spec gets its own INT and LONG column
  /// (`int<suffix>` / `long<suffix>`). Order matters only for diagnostic output.
  /// LinkedHashMap preserves declaration order so the data provider is stable.
  private static final Map<String, String> CODEC_SPECS;
  static {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("LZ4", "Lz4");
    m.put("ZSTD(3)", "Zstd");
    m.put("SNAPPY", "Snappy");
    m.put("GZIP", "Gzip");
    m.put("DELTA,LZ4", "DeltaLz4");
    m.put("DELTA,ZSTD(3)", "DeltaZstd");
    m.put("DELTA,SNAPPY", "DeltaSnappy");
    m.put("DELTA,GZIP", "DeltaGzip");
    m.put("DELTADELTA,LZ4", "DeltadeltaLz4");
    m.put("T64", "T64");
    m.put("GORILLA", "Gorilla");
    m.put("T64,LZ4", "T64Lz4");
    m.put("GORILLA,ZSTD(3)", "GorillaZstd");
    CODEC_SPECS = m;
  }

  private static String intColFor(String suffix) {
    return "int" + suffix;
  }

  private static String longColFor(String suffix) {
    return "long" + suffix;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder().setSchemaName(getTableName());
    for (String suffix : CODEC_SPECS.values()) {
      builder.addMetric(intColFor(suffix), FieldSpec.DataType.INT);
      builder.addMetric(longColFor(suffix), FieldSpec.DataType.LONG);
    }
    builder.addSingleValueDimension(STR_COL, FieldSpec.DataType.STRING);
    builder.addDateTimeField(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
    return builder.build();
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("codecRecord", null, null, false);
    List<Field> fields = new ArrayList<>();
    for (String suffix : CODEC_SPECS.values()) {
      fields.add(new Field(intColFor(suffix), org.apache.avro.Schema.create(Type.INT), null, null));
      fields.add(new Field(longColFor(suffix), org.apache.avro.Schema.create(Type.LONG), null, null));
    }
    fields.add(new Field(STR_COL, org.apache.avro.Schema.create(Type.STRING), null, null));
    fields.add(new Field(TIME_COL, org.apache.avro.Schema.create(Type.LONG), null, null));
    avroSchema.setFields(fields);

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        for (String suffix : CODEC_SPECS.values()) {
          record.put(intColFor(suffix), i);
          record.put(longColFor(suffix), (long) i * 1_000_000_000L);
        }
        record.put(STR_COL, "str_" + i);
        record.put(TIME_COL, (long) i);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Override
  public String getTimeColumnName() {
    return TIME_COL;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setFieldConfigList(getFieldConfigs())
        .build();
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    // STR_COL uses a dictionary (default), so it is intentionally NOT in this list.
    List<String> noDict = new ArrayList<>();
    for (String suffix : CODEC_SPECS.values()) {
      noDict.add(intColFor(suffix));
      noDict.add(longColFor(suffix));
    }
    return noDict;
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    for (Map.Entry<String, String> entry : CODEC_SPECS.entrySet()) {
      String spec = entry.getKey();
      String suffix = entry.getValue();
      fieldConfigs.add(rawFieldConfigWithCodecSpec(intColFor(suffix), spec));
      fieldConfigs.add(rawFieldConfigWithCodecSpec(longColFor(suffix), spec));
    }
    // STR_COL with dictionary encoding — verifies codec-pipeline and dict columns coexist.
    fieldConfigs.add(new FieldConfig.Builder(STR_COL)
        .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
        .build());
    return fieldConfigs;
  }

  /// Builds a RAW FieldConfig whose codecSpec is configured via the modern `indexes.forward` block
  /// (the only supported path; there is no top-level FieldConfig.codecSpec field).
  private static FieldConfig rawFieldConfigWithCodecSpec(String column, String codecSpec) {
    ObjectNode forward = JsonUtils.newObjectNode();
    forward.put("codecSpec", codecSpec);
    // Each input file contains 500 rows. Keep V7 chunks smaller than that so point lookups
    // exercise both sides of a real chunk boundary within each generated segment.
    forward.put("targetDocsPerChunk", V7_TARGET_DOCS_PER_CHUNK);
    ObjectNode indexes = JsonUtils.newObjectNode();
    indexes.set("forward", forward);
    return new FieldConfig.Builder(column)
        .withEncodingType(FieldConfig.EncodingType.RAW)
        .withIndexes(indexes)
        .build();
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Nullable
  @Override
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  /// Cartesian product of (codec spec, query engine) so every codec is exercised on both engines.
  @DataProvider(name = "codecAndEngine")
  public Object[][] codecAndEngine() {
    List<Object[]> rows = new ArrayList<>(CODEC_SPECS.size() * 2);
    for (Map.Entry<String, String> entry : CODEC_SPECS.entrySet()) {
      String spec = entry.getKey();
      String suffix = entry.getValue();
      rows.add(new Object[]{spec, suffix, false});
      rows.add(new Object[]{spec, suffix, true});
    }
    return rows.toArray(new Object[0][]);
  }

  @Test(dataProvider = "codecAndEngine")
  public void testSumPerCodec(String codecSpec, String suffix, boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String intCol = intColFor(suffix);
    String longCol = longColFor(suffix);

    JsonNode intSum = postQuery("SELECT SUM(" + intCol + ") FROM " + getTableName());
    assertEquals(intSum.get("resultTable").get("rows").get(0).get(0).asLong(), EXPECTED_INT_SUM,
        "Unexpected SUM(" + intCol + ") for codec " + codecSpec);

    JsonNode longSum = postQuery("SELECT SUM(" + longCol + ") FROM " + getTableName());
    assertEquals(longSum.get("resultTable").get("rows").get(0).get(0).asLong(), EXPECTED_LONG_SUM,
        "Unexpected SUM(" + longCol + ") for codec " + codecSpec);
  }

  @Test(dataProvider = "codecAndEngine")
  public void testFilterPerCodec(String codecSpec, String suffix, boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String intCol = intColFor(suffix);
    String longCol = longColFor(suffix);

    // intVal < 100 → 100 rows (values 0..99)
    JsonNode intFilter = postQuery("SELECT COUNT(*) FROM " + getTableName() + " WHERE " + intCol + " < 100");
    assertEquals(intFilter.get("resultTable").get("rows").get(0).get(0).asLong(), 100L,
        "Unexpected count for " + intCol + " < 100, codec " + codecSpec);

    // longVal < 100_000_000_000L → 100 rows
    JsonNode longFilter =
        postQuery("SELECT COUNT(*) FROM " + getTableName() + " WHERE " + longCol + " < 100000000000");
    assertEquals(longFilter.get("resultTable").get("rows").get(0).get(0).asLong(), 100L,
        "Unexpected count for " + longCol + " < 100B, codec " + codecSpec);
  }

  /// Per-codec point lookups across multiple chunk boundaries. Aggregate queries can mask per-doc
  /// decoding errors that average out — point lookups force the reader to materialize specific
  /// values, including chunk-boundary docs.
  @Test(dataProvider = "codecAndEngine")
  public void testPointLookupsPerCodec(String codecSpec, String suffix, boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String intCol = intColFor(suffix);
    String longCol = longColFor(suffix);

    // Avro records are distributed round-robin across two files. IDs 510/512 are local rows
    // 255/256 in one segment and 511/513 are rows 255/256 in the other segment.
    JsonNode result = postQuery(
        "SELECT " + TIME_COL + ", " + intCol + ", " + longCol + " FROM " + getTableName()
            + " WHERE " + TIME_COL + " IN (" + POINT_LOOKUP_ID_LIST + ") ORDER BY " + TIME_COL);
    JsonNode rows = result.get("resultTable").get("rows");
    assertEquals(rows.size(), POINT_LOOKUP_IDS.length, "Unexpected point-lookup row count for " + codecSpec);
    for (int rowId = 0; rowId < POINT_LOOKUP_IDS.length; rowId++) {
      int id = POINT_LOOKUP_IDS[rowId];
      JsonNode row = rows.get(rowId);
      assertEquals(row.get(0).asInt(), id, "Unexpected point-lookup order for codec " + codecSpec);
      assertEquals(row.get(1).asInt(), id, "Wrong " + intCol + " for ts=" + id + ", codec " + codecSpec);
      assertEquals(row.get(2).asLong(), (long) id * 1_000_000_000L,
          "Wrong " + longCol + " for ts=" + id + ", codec " + codecSpec);
    }
  }

  /// Verifies that a single SELECT touching multiple codec-encoded columns returns consistent values
  /// across codecs in the same row — catches any chunk-state cross-talk between readers.
  @Test(dataProvider = "useBothQueryEngines")
  public void testCrossCodecConsistency(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    List<String> suffixes = new ArrayList<>(CODEC_SPECS.values());
    String selectList = String.join(", ", Stream.concat(
            suffixes.stream().map(CodecPipelineIntegrationTest::intColFor),
            suffixes.stream().map(CodecPipelineIntegrationTest::longColFor))
        .toArray(String[]::new));

    int[] spotCheckIds = {0, 510, 511, 512, 513, 999};
    for (int id : spotCheckIds) {
      JsonNode result = postQuery("SELECT " + selectList + " FROM " + getTableName() + " WHERE ts = " + id);
      JsonNode row = result.get("resultTable").get("rows").get(0);
      // First N columns are int; next N are long.
      for (int i = 0; i < suffixes.size(); i++) {
        assertEquals(row.get(i).asInt(), id,
            "Cross-codec int mismatch at suffix " + suffixes.get(i) + " for ts=" + id);
      }
      for (int i = 0; i < suffixes.size(); i++) {
        assertEquals(row.get(suffixes.size() + i).asLong(), (long) id * 1_000_000_000L,
            "Cross-codec long mismatch at suffix " + suffixes.get(i) + " for ts=" + id);
      }
    }
  }

  /// Verifies that a STRING column stored with dictionary encoding (not codec pipeline) reads back
  /// correctly alongside codec-pipeline columns, confirming both can coexist in the same segment.
  @Test(dataProvider = "useBothQueryEngines")
  public void testStringColumnWithDictEncoding(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    int[] spotCheckIds = {0, 42, 500, 999};
    for (int id : spotCheckIds) {
      JsonNode result = postQuery("SELECT strVal FROM " + getTableName() + " WHERE ts = " + id);
      assertEquals(result.get("resultTable").get("rows").get(0).get(0).asText(), "str_" + id,
          "Wrong strVal for ts=" + id);
    }

    JsonNode countDistinctResult = postQuery("SELECT COUNT(DISTINCT strVal) FROM " + getTableName());
    assertEquals(countDistinctResult.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS,
        "Expected all " + NUM_DOCS + " distinct string values");
  }

  /// Verify COUNT(*) reads through every codec column path consistently via a join-style test.
  @Test(dataProvider = "useBothQueryEngines")
  public void testCountAcrossCodecs(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // COUNT with no WHERE: should be NUM_DOCS regardless of column choice.
    JsonNode count = postQuery("SELECT COUNT(*) FROM " + getTableName());
    assertEquals(count.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS,
        "Unexpected total row count");

    // COUNT WHERE intLz4 = intZstd (every row should match: same values across codecs).
    JsonNode crossCount = postQuery(
        "SELECT COUNT(*) FROM " + getTableName() + " WHERE intLz4 = intZstd AND longSnappy = longGzip");
    assertEquals(crossCount.get("resultTable").get("rows").get(0).get(0).asLong(), NUM_DOCS,
        "Cross-codec equality must hold for every row");
  }

  /// Proves that codecSpec is not merely accepted by config and ignored by segment generation.
  /// Every configured codec pipeline must produce a self-describing V7 reader with the exact
  /// canonical spec.
  @Test
  public void testGeneratedSegmentsUseConfiguredForwardIndexFormats()
      throws Exception {
    File[] segmentDirs = _segmentDir.listFiles(File::isDirectory);
    assertNotNull(segmentDirs, "Segment output directory must be readable: " + _segmentDir);
    assertTrue(segmentDirs.length > 0, "Expected generated segments under " + _segmentDir);

    FieldIndexConfigs rawForwardConfig = new FieldIndexConfigs.Builder()
        .add(StandardIndexes.forward(), new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW).build())
        .build();
    for (File segmentDir : segmentDirs) {
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentDirectory.Reader segmentReader = directory.createReader()) {
        ColumnMetadata dictionaryMetadata = directory.getSegmentMetadata().getColumnMetadataFor(STR_COL);
        assertNotNull(dictionaryMetadata, "Missing metadata for " + STR_COL + " in " + segmentDir);
        assertTrue(dictionaryMetadata.hasDictionary(), STR_COL + " must remain dictionary encoded");
        assertEquals(dictionaryMetadata.getForwardIndexEncoding(), FieldConfig.EncodingType.DICTIONARY);

        for (Map.Entry<String, String> entry : CODEC_SPECS.entrySet()) {
          String codecSpec = entry.getKey();
          String suffix = entry.getValue();
          assertNumericForwardIndexFormat(directory, segmentReader, rawForwardConfig, intColFor(suffix), codecSpec);
          assertNumericForwardIndexFormat(directory, segmentReader, rawForwardConfig, longColFor(suffix), codecSpec);
        }
      }
    }
  }

  private static void assertNumericForwardIndexFormat(SegmentDirectory directory,
      SegmentDirectory.Reader segmentReader, FieldIndexConfigs rawForwardConfig, String column, String codecSpec)
      throws Exception {
    ColumnMetadata metadata = directory.getSegmentMetadata().getColumnMetadataFor(column);
    assertNotNull(metadata, "Missing metadata for " + column + " in " + directory.getPath());
    assertEquals(metadata.getForwardIndexEncoding(), FieldConfig.EncodingType.RAW);
    assertFalse(metadata.hasDictionary());
    PinotDataBuffer forwardIndexBuffer = segmentReader.getIndexFor(column, StandardIndexes.forward());
    try (ForwardIndexReader<?> forwardReader = StandardIndexes.forward().getReaderFactory()
        .createIndexReader(segmentReader, rawForwardConfig, metadata)) {
      assertTrue(forwardReader instanceof FixedByteChunkSVForwardIndexReaderV7,
          column + " with " + codecSpec + " should use V7, got " + forwardReader.getClass());
      assertEquals(FixedByteChunkSVForwardIndexReaderV7.readCodecSpec(forwardIndexBuffer), codecSpec);
      assertNull(forwardReader.getCompressionType());
    }
  }

  /// Sanity check: the codec spec list stays in sync with the rest of the test setup.
  @Test
  public void testAllCodecSpecsRegisteredInTableConfig() {
    List<String> expectedColumns = new ArrayList<>();
    for (String suffix : CODEC_SPECS.values()) {
      expectedColumns.add(intColFor(suffix));
      expectedColumns.add(longColFor(suffix));
    }
    List<String> noDict = getNoDictionaryColumns();
    assertEquals(noDict.size(), expectedColumns.size(),
        "noDictionaryColumns size must match the codec-spec matrix");
    for (String col : expectedColumns) {
      if (!noDict.contains(col)) {
        throw new AssertionError("Expected " + col + " in noDictionaryColumns; got " + noDict);
      }
    }
  }
}
