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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests for `TEXT_SCORE(column, 'query')`: BM25 relevance from the Lucene text index, for a column with its own
/// text index (`title`) and for a column served by the multi-column text index (`descr`).
///
/// The corpus avoids stop words so that Lucene's field lengths equal the visible token counts, which lets
/// [#testScoreMatchesBm25Formula] check the exact BM25 value.
public class TextScoreQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), "TextScoreQueriesTest-" + UUID.randomUUID());
  private static final String TABLE_NAME = "products";
  private static final String SEGMENT_NAME = "productsSegment";
  private static final String ID_COL = "id";
  private static final String TITLE_COL = "title";
  private static final String DESCR_COL = "descr";

  /// id -> title. Ids 2/3 have the same length but different `usb` frequencies; ids 0/1 have the same `usb`
  /// frequency but different lengths; ids 4/5 do not mention `usb`.
  private static final String[] TITLES = {
      "usb charger",
      "usb charger braided cable wall adapter travel kit pouch",
      "usb usb usb hub",
      "usb power bank hub",
      "hdmi monitor cable",
      "wireless mouse"
  };

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
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < TITLES.length; i++) {
      GenericRow row = new GenericRow();
      row.putValue(ID_COL, i);
      row.putValue(TITLE_COL, TITLES[i]);
      row.putValue(DESCR_COL, TITLES[i]);
      rows.add(row);
    }
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), new IndexLoadingConfig(TABLE_CONFIG, SCHEMA));
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testScoresArePositiveForMatchesAndZeroOtherwise() {
    for (String column : List.of(TITLE_COL, DESCR_COL)) {
      Map<Integer, Double> scores = scoresById(column, "usb");
      assertEquals(scores.size(), TITLES.length);
      for (int id = 0; id < 4; id++) {
        assertTrue(scores.get(id) > 0, column + " id " + id);
      }
      assertEquals(scores.get(4), 0.0, column);
      assertEquals(scores.get(5), 0.0, column);
    }
  }

  @Test
  public void testHigherTermFrequencyScoresHigher() {
    for (String column : List.of(TITLE_COL, DESCR_COL)) {
      Map<Integer, Double> scores = scoresById(column, "usb");
      // Same length (4 tokens); 3 occurrences vs 1
      assertTrue(scores.get(2) > scores.get(3), column);
    }
  }

  @Test
  public void testShorterDocumentScoresHigher() {
    for (String column : List.of(TITLE_COL, DESCR_COL)) {
      Map<Integer, Double> scores = scoresById(column, "usb");
      // One occurrence each; 2 tokens vs 9 tokens
      assertTrue(scores.get(0) > scores.get(1), column);
    }
  }

  @Test
  public void testScoreMatchesBm25Formula() {
    // Lucene BM25: idf * tf / (tf + k1 * (1 - b + b * dl / avgdl)), idf = ln(1 + (N - n + 0.5) / (n + 0.5))
    double k1 = 1.2;
    double b = 0.75;
    int numDocs = TITLES.length;
    int docFreq = 4;
    double avgdl = (2 + 9 + 4 + 4 + 3 + 2) / (double) numDocs;
    double idf = Math.log(1 + (numDocs - docFreq + 0.5) / (docFreq + 0.5));
    double expected = idf * 1 / (1 + k1 * (1 - b + b * 2 / avgdl));
    Map<Integer, Double> scores = scoresById(TITLE_COL, "usb");
    assertEquals(scores.get(0), expected, 1e-5);
  }

  @Test
  public void testOrderByScore() {
    for (String column : List.of(TITLE_COL, DESCR_COL)) {
      BrokerResponseNative response = getBrokerResponse(
          "SELECT id, TEXT_SCORE(" + column + ", 'usb') AS s FROM products WHERE TEXT_MATCH(" + column
              + ", 'usb') ORDER BY s DESC, id LIMIT 100");
      assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
      assertEquals(response.getResultTable().getDataSchema().getColumnDataType(1), ColumnDataType.DOUBLE);
      // 4 identical segments: collapse the repeated ids
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
  public void testMultiTermQueryRanksDocumentWithAllTermsFirst() {
    BrokerResponseNative response = getBrokerResponse(
        "SELECT id, TEXT_SCORE(title, 'usb charger') AS s FROM products WHERE TEXT_MATCH(title, 'usb charger') "
            + "ORDER BY s DESC LIMIT 1");
    assertEquals(response.getResultTable().getRows().get(0)[0], 0);
  }

  @Test
  public void testScoreFilterAgreesWithTextMatch() {
    long viaScore = (long) getBrokerResponse(
        "SELECT COUNT(*) FROM products WHERE TEXT_SCORE(title, 'usb OR hdmi') > 0").getResultTable().getRows()
        .get(0)[0];
    long viaMatch = (long) getBrokerResponse(
        "SELECT COUNT(*) FROM products WHERE TEXT_MATCH(title, 'usb OR hdmi')").getResultTable().getRows()
        .get(0)[0];
    assertEquals(viaScore, viaMatch);
    assertEquals(viaMatch, 5L * 4);
  }

  @Test
  public void testColumnWithoutTextIndexIsRejected() {
    BadQueryRequestException e = expectThrows(BadQueryRequestException.class,
        () -> getBrokerResponse("SELECT TEXT_SCORE(id, 'usb') FROM products"));
    assertEquals(e.getCause().getMessage(), "Cannot apply TEXT_SCORE on column: id without text index");
  }

  /// Returns `id -> TEXT_SCORE(column, query)` for every document (no filter).
  private Map<Integer, Double> scoresById(String column, String query) {
    BrokerResponseNative response =
        getBrokerResponse("SELECT id, TEXT_SCORE(" + column + ", '" + query + "') FROM products LIMIT 1000");
    assertTrue(response.getExceptions().isEmpty(), response.getExceptions().toString());
    Map<Integer, Double> scores = new HashMap<>();
    for (Object[] row : response.getResultTable().getRows()) {
      Double previous = scores.put((Integer) row[0], (Double) row[1]);
      // Every copy of the segment must produce the same score
      assertTrue(previous == null || previous.equals(row[1]));
    }
    return scores;
  }
}
