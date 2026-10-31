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

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.TextScoreStats;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests global (cross-segment) BM25 statistics: `TEXT_SCORE_STATS` collects them and `TEXT_SCORE`'s 4th argument
/// scores with them.
///
/// Two segments, each served by its own instance, have deliberately different term distributions: in segment A
/// `usb` is in 5 of 6 short titles (low IDF, short average length), in segment B it is in 1 of 6 longer titles. Both
/// contain the title `usb charger` (ids 0 and 100). A third segment holding the union of both corpora is the
/// reference: with global statistics, every document must score exactly what it scores in that single index.
public class TextScoreGlobalStatsQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "TextScoreGlobalStatsQueriesTest-" + UUID.randomUUID());
  private static final String TABLE_NAME = "products";
  private static final String ID_COL = "id";
  private static final String TITLE_COL = "title";
  private static final String DESCR_COL = "descr";
  private static final List<String> TEXT_COLUMNS = List.of(TITLE_COL, DESCR_COL);

  private static final String[] TITLES_A = {
      "usb charger", "usb cable", "usb hub", "usb usb adapter", "usb mouse", "keyboard"
  };
  private static final String[] TITLES_B = {
      "usb charger", "wireless mouse pad large desk", "hdmi monitor cable long braided",
      "laptop stand aluminium adjustable", "charger brick fast", "webcam ring light"
  };
  private static final int ID_OFFSET_B = 100;

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(ID_COL, DataType.INT)
      .addSingleValueDimension(TITLE_COL, DataType.STRING)
      .addSingleValueDimension(DESCR_COL, DataType.STRING)
      .build();

  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
      .setNoDictionaryColumns(List.of(TITLE_COL, DESCR_COL))
      .addFieldConfig(new FieldConfig.Builder(TITLE_COL).withEncodingType(FieldConfig.EncodingType.RAW)
          .withIndexTypes(List.of(FieldConfig.IndexType.TEXT)).build())
      .setMultiColumnTextIndexConfig(new MultiColumnTextIndexConfig(List.of(DESCR_COL)))
      .build();

  private ImmutableSegment _segmentA;
  private ImmutableSegment _segmentB;
  /// All documents of A followed by all documents of B, in one index
  private ImmutableSegment _unionSegment;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _segmentA;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return List.of(_segmentA, _segmentB);
  }

  @Override
  protected List<List<IndexSegment>> getDistinctInstances() {
    return List.of(List.of(_segmentA), List.of(_segmentB));
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    List<GenericRow> rowsA = rows(TITLES_A, 0);
    List<GenericRow> rowsB = rows(TITLES_B, ID_OFFSET_B);
    List<GenericRow> unionRows = new ArrayList<>(rowsA);
    unionRows.addAll(rowsB);
    _segmentA = buildSegment("segmentA", rowsA);
    _segmentB = buildSegment("segmentB", rowsB);
    _unionSegment = buildSegment("unionSegment", unionRows);
  }

  private static List<GenericRow> rows(String[] titles, int idOffset) {
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < titles.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue(ID_COL, idOffset + i);
      row.putValue(TITLE_COL, titles[i]);
      row.putValue(DESCR_COL, titles[i]);
      rows.add(row);
    }
    return rows;
  }

  private static ImmutableSegment buildSegment(String segmentName, List<GenericRow> rows)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), new IndexLoadingConfig(TABLE_CONFIG, SCHEMA));
  }

  @AfterClass
  public void tearDown() {
    _segmentA.destroy();
    _segmentB.destroy();
    _unionSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testPerSegmentScoresDifferForIdenticalDocuments() {
    for (String column : TEXT_COLUMNS) {
      for (String query : List.of("usb", "usb charger")) {
        Map<Integer, Double> scores = scoresById(column, query, null);
        assertTrue(scores.get(0) > 0, column);
        // Same text, different segment statistics: `usb` is common in A (low IDF) and rare in B (high IDF)
        assertNotEquals(scores.get(0), scores.get(ID_OFFSET_B), column + " " + query);
        assertTrue(scores.get(ID_OFFSET_B) > scores.get(0), column + " " + query);
      }
    }
  }

  @Test
  public void testStatsAreSummedAcrossSegmentsAndInstances() {
    for (String column : TEXT_COLUMNS) {
      TextScoreStats stats = TextScoreStats.fromJson(getStats(column, "usb charger", null));
      TextScoreStats.FieldStats fieldStats = stats.getField(column);
      assertNotNull(fieldStats, stats.toJson());
      assertEquals(stats.getFields().size(), 1);
      assertEquals(fieldStats.getDocCount(), TITLES_A.length + TITLES_B.length);
      assertEquals(fieldStats.getMaxDoc(), TITLES_A.length + TITLES_B.length);
      // Token counts: A = 2+2+2+3+2+1, B = 2+5+5+4+3+3
      assertEquals(fieldStats.getSumTotalTermFreq(), 12 + 22);
      assertEquals(fieldStats.getTerms().keySet(), Set.of("charger", "usb"));
      // usb: 5 docs / 6 occurrences in A, 1 doc in B; charger: 1 doc in A, 2 docs in B
      assertEquals(fieldStats.getTermStats("usb"), new long[]{6, 7});
      assertEquals(fieldStats.getTermStats("charger"), new long[]{3, 3});
      // Equal to the statistics of the single union index
      assertEquals(stats, unionStats(column, "usb charger", null));
    }
  }

  @Test
  public void testGlobalStatsMakeIdenticalDocumentsScoreIdentically() {
    for (String column : TEXT_COLUMNS) {
      for (String query : List.of("usb", "usb charger", "charger OR cable", "\"usb charger\"")) {
        String stats = getStats(column, query, null);
        Map<Integer, Double> scores = scoresById(column, query, stats);
        assertTrue(scores.get(0) > 0, column + " " + query);
        assertEquals(scores.get(0), scores.get(ID_OFFSET_B), column + " " + query);
      }
    }
  }

  @Test
  public void testGlobalStatsScoresEqualSingleIndexScores() {
    for (String column : TEXT_COLUMNS) {
      for (String query : List.of("usb", "usb charger", "charger OR cable", "\"usb charger\"", "mouse AND usb")) {
        Map<Integer, Double> scores = scoresById(column, query, getStats(column, query, null));
        Int2FloatMap unionScores = unionScores(column, query);
        assertEquals(scores.size(), TITLES_A.length + TITLES_B.length);
        int numMatches = 0;
        for (int unionDocId = 0; unionDocId < TITLES_A.length + TITLES_B.length; unionDocId++) {
          int id = unionDocId < TITLES_A.length ? unionDocId : ID_OFFSET_B + unionDocId - TITLES_A.length;
          double expected = unionScores.get(unionDocId);
          assertEquals(scores.get(id), expected, 1e-5, column + " " + query + " id " + id);
          if (expected > 0) {
            numMatches++;
          }
        }
        assertTrue(numMatches > 0, column + " " + query);
      }
    }
  }

  @Test
  public void testGlobalStatsWithFilterAndOrderBy() {
    String stats = getStats(TITLE_COL, "usb charger", null);
    BrokerResponseNative response = getBrokerResponse(
        "SELECT id, TEXT_SCORE(title, 'usb charger', '', '" + stats + "') AS s FROM products "
            + "WHERE TEXT_MATCH(title, 'usb charger') ORDER BY s DESC, id LIMIT 3");
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    List<Object[]> rows = response.getResultTable().getRows();
    // The two `usb charger` titles tie for first place with global statistics
    assertEquals(rows.get(0)[0], 0);
    assertEquals(rows.get(1)[0], ID_OFFSET_B);
    assertEquals(rows.get(0)[1], rows.get(1)[1]);
  }

  @Test
  public void testStatsWithParserOptions() {
    // The options string must be passed to both steps; it changes how the query is parsed
    String options = "parser=CLASSIC,defaultOperator=AND";
    String stats = getStats(TITLE_COL, "usb charger", options);
    assertEquals(TextScoreStats.fromJson(stats), unionStats(TITLE_COL, "usb charger", options));
    Map<Integer, Double> scores = scoresById(TITLE_COL, "usb charger", options, stats);
    // AND semantics: only the two `usb charger` titles match, with equal scores
    assertEquals(scores.get(0), scores.get(ID_OFFSET_B));
    assertTrue(scores.get(0) > 0);
    assertEquals(scores.get(1), 0.0);
  }

  @Test
  public void testStatsIgnoreFilter() {
    // The statistics describe the corpus, not the filtered documents
    BrokerResponseNative response = getBrokerResponse(
        "SELECT TEXT_SCORE_STATS(title, 'usb') FROM products WHERE id = 0");
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    TextScoreStats stats = TextScoreStats.fromJson((String) response.getResultTable().getRows().get(0)[0]);
    assertEquals(stats.getField(TITLE_COL).getDocCount(), TITLES_A.length + TITLES_B.length);
  }

  @Test
  public void testEmptyStatsFallBackToPerSegmentScores() {
    assertEquals(scoresById(TITLE_COL, "usb", ""), scoresById(TITLE_COL, "usb", null));
    assertEquals(scoresById(TITLE_COL, "usb", new TextScoreStats().toJson()), scoresById(TITLE_COL, "usb", null));
  }

  @Test
  public void testStatsCannotBeCombinedWithOtherAggregations() {
    // Rejected while planning the segment query
    RuntimeException e = expectThrows(RuntimeException.class,
        () -> getBrokerResponse("SELECT TEXT_SCORE_STATS(title, 'usb'), COUNT(*) FROM products"));
    assertTrue(e.getMessage().contains("TEXT_SCORE_STATS cannot be combined with other aggregation functions"),
        e.getMessage());
  }

  @Test
  public void testStatsOnColumnWithoutTextIndexIsRejected() {
    BrokerResponseNative response = getBrokerResponse("SELECT TEXT_SCORE_STATS(id, 'usb') FROM products");
    assertFalse(response.getExceptions().isEmpty());
    assertTrue(response.getExceptions().get(0).getMessage().contains("without text index"),
        response.getExceptions().toString());
  }

  private String getStats(String column, String query, String options) {
    String optionsArg = options != null ? ", '" + options + "'" : "";
    BrokerResponseNative response = getBrokerResponse(
        "SELECT TEXT_SCORE_STATS(" + column + ", '" + query.replace("'", "''") + "'" + optionsArg
            + ") FROM products");
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    assertEquals(response.getResultTable().getDataSchema().getColumnDataType(0), ColumnDataType.STRING);
    return (String) response.getResultTable().getRows().get(0)[0];
  }

  private Map<Integer, Double> scoresById(String column, String query, String stats) {
    return scoresById(column, query, "", stats);
  }

  /// Returns `id -> TEXT_SCORE(column, query, options[, stats])` for every document of both segments.
  private Map<Integer, Double> scoresById(String column, String query, String options, String stats) {
    String extraArgs = stats != null ? ", '" + options + "', '" + stats + "'" : "";
    BrokerResponseNative response = getBrokerResponse(
        "SELECT id, TEXT_SCORE(" + column + ", '" + query.replace("'", "''") + "'" + extraArgs
            + ") FROM products LIMIT 1000");
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    Map<Integer, Double> scores = new HashMap<>();
    for (Object[] row : response.getResultTable().getRows()) {
      scores.put((Integer) row[0], (Double) row[1]);
    }
    return scores;
  }

  /// Per-segment scores of the union index, keyed by union doc id; non-matches are absent (0)
  private Int2FloatMap unionScores(String column, String query) {
    DataSource dataSource = _unionSegment.getDataSource(column, SCHEMA);
    return dataSource.getTextIndex() != null ? dataSource.getTextIndex().getDocScores(query, (String) null)
        : dataSource.getMultiColumnTextIndex().getDocScores(column, query, (String) null);
  }

  private TextScoreStats unionStats(String column, String query, String options) {
    DataSource dataSource = _unionSegment.getDataSource(column, SCHEMA);
    return dataSource.getTextIndex() != null ? dataSource.getTextIndex().getTextScoreStats(query, options)
        : dataSource.getMultiColumnTextIndex().getTextScoreStats(column, query, options);
  }
}
