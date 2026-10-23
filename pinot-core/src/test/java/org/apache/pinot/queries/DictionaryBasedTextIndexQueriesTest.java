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
package org.apache.pinot.queries;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.FieldConfig.IndexType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/// Verifies that a dictionary-based TEXT index (`buildOnDictionary = true`) returns the same `TEXT_MATCH`
/// results as the default per-row TEXT index, for both single-value and multi-value STRING columns. The
/// dictionary-based index returns matching dictIds that are resolved to docIds by the standard dictionary-based
/// filter operators; this test confirms that resolution is correct end-to-end.
public class DictionaryBasedTextIndexQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), DictionaryBasedTextIndexQueriesTest.class.getSimpleName());
  private static final String TABLE_NAME = "testTable";
  private static final String SV_COL = "SV_TEXT";
  private static final String MV_COL = "MV_TEXT";
  private static final String INT_COL = "INT_COL";
  private static final int NUM_ROWS = 1000;

  // SV value cycles over 5 distinct tokens; MV value pairs a constant token with a cycling token.
  private static final String[] SV_TOKENS = {"apple", "banana", "cherry", "date", "elderberry"};
  private static final String[] MV_TOKENS = {"red", "green", "blue"};

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(SV_COL, FieldSpec.DataType.STRING)
      .addMultiValueDimension(MV_COL, FieldSpec.DataType.STRING)
      .addMetric(INT_COL, FieldSpec.DataType.INT)
      .build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @AfterClass
  public void tearDown() {
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private static List<GenericRow> createTestData() {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(SV_COL, SV_TOKENS[i % SV_TOKENS.length]);
      row.putValue(MV_COL, new String[]{"fruit", MV_TOKENS[i % MV_TOKENS.length]});
      row.putValue(INT_COL, i);
      rows.add(row);
    }
    return rows;
  }

  private TableConfig tableConfig(boolean buildOnDictionary) {
    return tableConfig(buildOnDictionary, false);
  }

  private TableConfig tableConfig(boolean buildOnDictionary, boolean storeInSegmentFile) {
    Map<String, String> props = Map.of(
        FieldConfig.TEXT_INDEX_BUILD_ON_DICTIONARY, Boolean.toString(buildOnDictionary),
        "storeInSegmentFile", Boolean.toString(storeInSegmentFile));
    List<FieldConfig> fieldConfigs = List.of(
        new FieldConfig(SV_COL, EncodingType.DICTIONARY, List.of(IndexType.TEXT), null, props),
        new FieldConfig(MV_COL, EncodingType.DICTIONARY, List.of(IndexType.TEXT), null, props));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setFieldConfigList(fieldConfigs).build();
  }

  private ImmutableSegment buildAndLoad(String segmentName, boolean buildOnDictionary)
      throws Exception {
    return buildAndLoad(segmentName, buildOnDictionary, false);
  }

  private ImmutableSegment buildAndLoad(String segmentName, boolean buildOnDictionary, boolean storeInSegmentFile)
      throws Exception {
    TableConfig tableConfig = tableConfig(buildOnDictionary, storeInSegmentFile);
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(createTestData())) {
      driver.init(config, recordReader);
      driver.build();
    }
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName),
        new IndexLoadingConfig(tableConfig, SCHEMA));
  }

  private long count(String query) {
    AggregationResultsBlock block = (AggregationResultsBlock) ((Operator) getOperator(query)).nextBlock();
    return ((Number) block.getResults().get(0)).longValue();
  }

  @Test
  public void dictionaryBasedTextMatchEqualsPerRow()
      throws Exception {
    String[] queries = {
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + SV_COL + ", 'apple')",
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + SV_COL + ", 'cherry')",
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + SV_COL + ", 'missing')",
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + MV_COL + ", 'fruit')",
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + MV_COL + ", 'green')",
        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + MV_COL + ", 'red OR blue')"
    };
    // Expected counts derived from the deterministic data generator.
    long[] expected = {
        NUM_ROWS / SV_TOKENS.length,          // 'apple' -> i % 5 == 0
        NUM_ROWS / SV_TOKENS.length,          // 'cherry' -> i % 5 == 2
        0,                                    // not present
        NUM_ROWS,                             // 'fruit' present in every MV row
        countMv(1),                           // 'green' -> i % 3 == 1
        countMv(0) + countMv(2)               // 'red' (i%3==0) OR 'blue' (i%3==2)
    };

    // Per-row index results (the reference) ...
    ImmutableSegment perRow = buildAndLoad("perRow", false);
    _indexSegment = perRow;
    _indexSegments = List.of(perRow);
    long[] perRowCounts = new long[queries.length];
    for (int i = 0; i < queries.length; i++) {
      perRowCounts[i] = count(queries[i]);
      assertEquals(perRowCounts[i], expected[i], "Per-row count mismatch for: " + queries[i]);
    }
    perRow.destroy();

    // ... must equal the dictionary-based index results.
    ImmutableSegment dictBased = buildAndLoad("dictBased", true);
    _indexSegment = dictBased;
    _indexSegments = List.of(dictBased);
    for (int i = 0; i < queries.length; i++) {
      assertEquals(count(queries[i]), perRowCounts[i], "Dictionary-based count mismatch for: " + queries[i]);
    }
    dictBased.destroy();
    _indexSegment = null;
  }

  @Test
  public void dictionaryBasedIndexIsSmallerForLowCardinality()
      throws Exception {
    // The SV column has only 5 distinct values across 1000 rows, so the dictionary-based text index (5 Lucene docs)
    // must be smaller than the per-row text index (1000 Lucene docs).
    ImmutableSegment perRow = buildAndLoad("perRowSize", false);
    long perRowSize = FileUtils.sizeOfDirectory(new File(INDEX_DIR, "perRowSize"));
    perRow.destroy();
    ImmutableSegment dictBased = buildAndLoad("dictSize", true);
    long dictSize = FileUtils.sizeOfDirectory(new File(INDEX_DIR, "dictSize"));
    dictBased.destroy();
    _indexSegment = null;
    assertTrue(dictSize < perRowSize,
        "Expected dictionary-based segment (" + dictSize + ") < per-row segment (" + perRowSize + ")");
  }

  @Test
  public void reloadBuildsDictionaryBasedTextIndex()
      throws Exception {
    // Build a segment with NO text index (dictionary-encoded columns only) ...
    TableConfig noTextConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(noTextConfig, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName("reload");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(createTestData())) {
      driver.init(config, recordReader);
      driver.build();
    }

    // ... then reload with a buildOnDictionary text index, which the SegmentPreProcessor (TextIndexHandler) builds
    // by looping the dictionary.
    ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, "reload"),
        new IndexLoadingConfig(tableConfig(true), SCHEMA));
    _indexSegment = segment;
    _indexSegments = List.of(segment);

    assertTrue(segment.getDataSource(SV_COL).getTextIndex().isBuildOnDictionary(),
        "Reloaded text index should be dictionary-based");
    assertEquals(count("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + SV_COL + ", 'apple')"),
        NUM_ROWS / SV_TOKENS.length);
    assertEquals(count("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + MV_COL + ", 'green')"),
        countMv(1));
    assertEquals(count("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + MV_COL + ", 'fruit')"), NUM_ROWS);

    segment.destroy();
    _indexSegment = null;
  }

  @Test
  public void buildOnDictionaryRequiresDictionaryEncoding()
      throws Exception {
    // A raw (non-dictionary) column with buildOnDictionary=true must fail the build, protecting the docId==dictId
    // invariant.
    Map<String, String> props = Map.of(FieldConfig.TEXT_INDEX_BUILD_ON_DICTIONARY, "true");
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(SV_COL))
        .setFieldConfigList(List.of(new FieldConfig(SV_COL, EncodingType.RAW, List.of(IndexType.TEXT), null, props)))
        .build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName("raw");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(createTestData())) {
      driver.init(config, recordReader);
      driver.build();
      fail("Expected build to fail for buildOnDictionary on a raw-encoded column");
    } catch (Exception e) {
      assertTrue(ExceptionUtils.getStackTrace(e).contains("buildOnDictionary"),
          "Expected a buildOnDictionary dictionary-required failure, got: " + e);
    }
  }

  @Test
  public void dictionaryBasedTextMatchRejectsOptions()
      throws Exception {
    ImmutableSegment dictBased = buildAndLoad("dictOptions", true);
    _indexSegment = dictBased;
    _indexSegments = List.of(dictBased);
    try {
      // TEXT_MATCH options are not supported by the dictionary-based path (getDictIds takes only the query value).
      getOperator(
          "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + SV_COL + ", 'apple', 'parser=CLASSIC')");
      fail("Expected TEXT_MATCH with options to be rejected for a dictionary-based text index");
    } catch (Exception e) {
      assertTrue(ExceptionUtils.getStackTrace(e).contains("options are not supported"),
          "Expected an options-not-supported failure, got: " + e);
    } finally {
      dictBased.destroy();
      _indexSegment = null;
    }
  }

  @Test
  public void dictionaryBasedTextMatchWithStoreInSegmentFile()
      throws Exception {
    // storeInSegmentFile packs the Lucene index into the segment's single file, exercising the buffer-based reader
    // path (LuceneTextIndexReader buffer constructor + buffer property overlay) in dictionary mode.
    ImmutableSegment perRow = buildAndLoad("perRowSif", false, true);
    _indexSegment = perRow;
    _indexSegments = List.of(perRow);
    long svCount = count("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + SV_COL + ", 'apple')");
    long mvCount = count("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + MV_COL + ", 'green')");
    perRow.destroy();

    ImmutableSegment dictBased = buildAndLoad("dictSif", true, true);
    _indexSegment = dictBased;
    _indexSegments = List.of(dictBased);
    assertTrue(dictBased.getDataSource(SV_COL).getTextIndex().isBuildOnDictionary(),
        "store-in-segment-file segment should still self-describe as dictionary-based");
    assertEquals(count("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + SV_COL + ", 'apple')"), svCount);
    assertEquals(count("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + MV_COL + ", 'green')"), mvCount);
    dictBased.destroy();
    _indexSegment = null;
  }

  @Test
  public void dictionaryBasedTextMatchMatchingAllValues()
      throws Exception {
    // Matching every distinct dictionary value exercises the always-true short-circuit in the dict-id evaluator.
    ImmutableSegment dictBased = buildAndLoad("dictAll", true);
    _indexSegment = dictBased;
    _indexSegments = List.of(dictBased);
    // MV distinct values are {fruit, red, green, blue}; OR-ing all of them matches every distinct dictId, so every
    // row matches.
    assertEquals(count("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + MV_COL
        + ", 'fruit OR red OR green OR blue')"), NUM_ROWS);
    dictBased.destroy();
    _indexSegment = null;
  }

  @Test
  public void segmentWithoutPersistedFlagReadsAsPerRow()
      throws Exception {
    // Build a per-row segment, then strip the buildOnDictionary key from its persisted properties to simulate a
    // segment written before this feature existed.
    buildAndLoad("preFeature", false).destroy();
    for (File propsFile : FileUtils.listFiles(new File(INDEX_DIR, "preFeature"), new String[]{"properties"}, true)) {
      if (propsFile.getName().equals("lucene.properties")) {
        List<String> kept = new ArrayList<>();
        for (String line : FileUtils.readLines(propsFile, StandardCharsets.UTF_8)) {
          if (!line.contains(FieldConfig.TEXT_INDEX_BUILD_ON_DICTIONARY)) {
            kept.add(line);
          }
        }
        FileUtils.writeLines(propsFile, kept);
      }
    }

    // Reload under a table config that turns buildOnDictionary ON. The segment self-describes, so it must still be
    // read as per-row (the build mode comes from the segment, not the live table config).
    ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, "preFeature"),
        new IndexLoadingConfig(tableConfig(true), SCHEMA));
    _indexSegment = segment;
    _indexSegments = List.of(segment);
    assertFalse(segment.getDataSource(SV_COL).getTextIndex().isBuildOnDictionary(),
        "A segment whose persisted properties lack the flag must be read as per-row, regardless of table config");
    assertEquals(count("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + SV_COL + ", 'apple')"),
        NUM_ROWS / SV_TOKENS.length);
    segment.destroy();
    _indexSegment = null;
  }

  private static long countMv(int mod) {
    long count = 0;
    for (int i = 0; i < NUM_ROWS; i++) {
      if (i % MV_TOKENS.length == mod) {
        count++;
      }
    }
    return count;
  }
}
