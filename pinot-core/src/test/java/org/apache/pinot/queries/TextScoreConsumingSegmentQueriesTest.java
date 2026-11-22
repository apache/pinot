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
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.indexsegment.mutable.MutableSegmentImpl;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentConfig;
import org.apache.pinot.segment.local.realtime.impl.RealtimeSegmentStatsHistory;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.MultiColumnRealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneIndexRefreshManager;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndex;
import org.apache.pinot.segment.local.realtime.impl.invertedindex.RealtimeLuceneTextIndexSearcherPool;
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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// `TEXT_SCORE` and `TEXT_SCORE_STATS` on a consuming (mutable) segment, for a column with its own realtime text
/// index (`title`) and for a column served by the realtime multi-column text index (`descr`).
///
/// The same rows are also built into an immutable segment, the reference: a consuming segment must return exactly
/// the scores and statistics of the committed segment holding the same documents. Queries run on two server
/// instances serving the same consuming segment, so `TEXT_SCORE_STATS` counts every document twice.
public class TextScoreConsumingSegmentQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "TextScoreConsumingSegmentQueriesTest-" + UUID.randomUUID());
  private static final String RAW_TABLE_NAME = "products";
  private static final String SEGMENT_NAME = RAW_TABLE_NAME + "__0__0__20240601T1818Z";
  private static final String ID_COL = "id";
  private static final String TITLE_COL = "title";
  private static final String DESCR_COL = "descr";
  private static final List<String> TEXT_COLUMNS = List.of(TITLE_COL, DESCR_COL);

  private static final String[] TITLES = {
      "usb charger",
      "usb charger braided cable wall adapter travel kit pouch",
      "usb usb usb hub",
      "usb power bank hub",
      "hdmi monitor cable",
      "wireless mouse"
  };

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(ID_COL, DataType.INT)
      .addSingleValueDimension(TITLE_COL, DataType.STRING)
      .addSingleValueDimension(DESCR_COL, DataType.STRING)
      .build();

  private static final MultiColumnTextIndexConfig MULTI_COLUMN_TEXT_INDEX_CONFIG =
      new MultiColumnTextIndexConfig(List.of(DESCR_COL));

  private MutableSegmentImpl _mutableSegment;
  private ImmutableSegment _referenceSegment;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _mutableSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return List.of(_mutableSegment);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    ServerMetrics.register(mock(ServerMetrics.class));
    RealtimeLuceneTextIndexSearcherPool.init(1);
    RealtimeLuceneIndexRefreshManager.init(1, 10);

    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < TITLES.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue(ID_COL, i);
      row.putValue(TITLE_COL, TITLES[i]);
      row.putValue(DESCR_COL, TITLES[i]);
      rows.add(row);
    }

    _mutableSegment = buildMutableSegment(rows);
    _referenceSegment = buildImmutableSegment(rows);
  }

  private static TableConfig tableConfig(TableType tableType) {
    return new TableConfigBuilder(tableType).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of(TITLE_COL, DESCR_COL))
        .addFieldConfig(new FieldConfig.Builder(TITLE_COL).withEncodingType(FieldConfig.EncodingType.RAW)
            .withIndexTypes(List.of(FieldConfig.IndexType.TEXT)).build())
        .setMultiColumnTextIndexConfig(MULTI_COLUMN_TEXT_INDEX_CONFIG)
        .build();
  }

  private static MutableSegmentImpl buildMutableSegment(List<GenericRow> rows)
      throws Exception {
    RealtimeSegmentStatsHistory statsHistory = mock(RealtimeSegmentStatsHistory.class);
    when(statsHistory.getEstimatedCardinality(anyString())).thenReturn(200);
    when(statsHistory.getEstimatedAvgColSize(anyString())).thenReturn(32);
    RealtimeSegmentConfig config = new RealtimeSegmentConfig.Builder(tableConfig(TableType.REALTIME), SCHEMA)
        .setTableNameWithType(RAW_TABLE_NAME + "_REALTIME")
        .setSegmentName(SEGMENT_NAME)
        .setStreamName(RAW_TABLE_NAME)
        .setSchema(SCHEMA)
        .setCapacity(1000)
        .setTextIndexConfig(MULTI_COLUMN_TEXT_INDEX_CONFIG)
        .setSegmentZKMetadata(new SegmentZKMetadata(SEGMENT_NAME))
        .setMemoryManager(new DirectMemoryManager(SEGMENT_NAME))
        .setStatsHistory(statsHistory)
        .setConsumerDir(new File(INDEX_DIR, "consumerDir").getAbsolutePath())
        .build();
    MutableSegmentImpl segment = new MutableSegmentImpl(config, null);
    for (GenericRow row : rows) {
      segment.index(row, null);
    }
    // Realtime text indexes are refreshed asynchronously; make every indexed row searchable before querying
    ((RealtimeLuceneTextIndex) segment.getDataSource(TITLE_COL, SCHEMA).getTextIndex()).getSearcherManager()
        .maybeRefreshBlocking();
    ((MultiColumnRealtimeLuceneTextIndex) segment.getDataSource(DESCR_COL, SCHEMA).getMultiColumnTextIndex())
        .getSearcherManager().maybeRefreshBlocking();
    return segment;
  }

  private static ImmutableSegment buildImmutableSegment(List<GenericRow> rows)
      throws Exception {
    TableConfig tableConfig = tableConfig(TableType.OFFLINE);
    String segmentName = "referenceSegment";
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), new IndexLoadingConfig(tableConfig, SCHEMA));
  }

  @AfterClass
  public void tearDown() {
    _mutableSegment.destroy();
    _referenceSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testScoresMatchCommittedSegment() {
    for (String column : TEXT_COLUMNS) {
      for (String query : List.of("usb", "usb charger", "charger OR cable", "\"usb charger\"")) {
        Map<Integer, Double> scores = scoresById(column, query, null);
        Int2FloatMap referenceScores = referenceScores(column, query, null);
        assertEquals(scores.size(), TITLES.length, column + " " + query);
        assertTrue(!referenceScores.isEmpty(), column + " " + query);
        for (int id = 0; id < TITLES.length; id++) {
          assertEquals(scores.get(id), referenceScores.get(id), 1e-5, column + " " + query + " id " + id);
        }
      }
    }
  }

  @Test
  public void testOrderByScore() {
    for (String column : TEXT_COLUMNS) {
      BrokerResponseNative response = getBrokerResponse(
          "SELECT id, TEXT_SCORE(" + column + ", 'usb') AS s FROM products WHERE TEXT_MATCH(" + column
              + ", 'usb') ORDER BY s DESC, id LIMIT 100");
      assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
      // Both instances return every match: collapse the repeated ids
      List<Integer> ids = new ArrayList<>();
      for (Object[] row : response.getResultTable().getRows()) {
        int id = (Integer) row[0];
        if (ids.isEmpty() || ids.get(ids.size() - 1) != id) {
          ids.add(id);
        }
      }
      assertEquals(ids, List.of(2, 0, 3, 1), column);
    }
  }

  @Test
  public void testStatsMatchCommittedSegment() {
    for (String column : TEXT_COLUMNS) {
      for (String options : new String[]{null, "parser=CLASSIC,defaultOperator=AND"}) {
        TextScoreStats stats = TextScoreStats.fromJson(getStats(column, "usb charger", options));
        TextScoreStats.FieldStats fieldStats = stats.getField(column);
        assertNotNull(fieldStats, stats.toJson());
        assertEquals(fieldStats.getDocCount(), 2 * TITLES.length);
        assertEquals(fieldStats.getTermStats("usb"), new long[]{2 * 4, 2 * 6});
        TextScoreStats referenceStats = referenceStats(column, "usb charger", options);
        assertEquals(stats, referenceStats.merge(referenceStats(column, "usb charger", options)),
            column + " " + options);
      }
    }
  }

  @Test
  public void testScoresWithGlobalStatsMatchCommittedSegment() {
    for (String column : TEXT_COLUMNS) {
      String stats = getStats(column, "usb charger", null);
      Map<Integer, Double> scores = scoresById(column, "usb charger", stats);
      Int2FloatMap referenceScores = referenceScores(column, "usb charger", TextScoreStats.fromJson(stats));
      for (int id = 0; id < TITLES.length; id++) {
        assertEquals(scores.get(id), referenceScores.get(id), 1e-5, column + " id " + id);
      }
      // The doubled corpus changes the statistics, hence the scores
      assertNotEquals(scores.get(0), scoresById(column, "usb charger", null).get(0), column);
    }
  }

  private String getStats(String column, String query, String options) {
    String optionsArg = options != null ? ", '" + options + "'" : "";
    BrokerResponseNative response = getBrokerResponse(
        "SELECT TEXT_SCORE_STATS(" + column + ", '" + query + "'" + optionsArg + ") FROM products");
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    return (String) response.getResultTable().getRows().get(0)[0];
  }

  /// Returns `id -> TEXT_SCORE(column, query[, '', stats])` for every document of the consuming segment.
  private Map<Integer, Double> scoresById(String column, String query, String stats) {
    String extraArgs = stats != null ? ", '', '" + stats + "'" : "";
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

  /// Scores of the committed reference segment, keyed by doc id (equal to `id`); non-matches are absent (0)
  private Int2FloatMap referenceScores(String column, String query, @Nullable TextScoreStats stats) {
    DataSource dataSource = _referenceSegment.getDataSource(column, SCHEMA);
    return dataSource.getTextIndex() != null ? dataSource.getTextIndex().getDocScores(query, null, stats)
        : dataSource.getMultiColumnTextIndex().getDocScores(column, query, null, stats);
  }

  private TextScoreStats referenceStats(String column, String query, String options) {
    DataSource dataSource = _referenceSegment.getDataSource(column, SCHEMA);
    return dataSource.getTextIndex() != null ? dataSource.getTextIndex().getTextScoreStats(query, options)
        : dataSource.getMultiColumnTextIndex().getTextScoreStats(column, query, options);
  }
}
