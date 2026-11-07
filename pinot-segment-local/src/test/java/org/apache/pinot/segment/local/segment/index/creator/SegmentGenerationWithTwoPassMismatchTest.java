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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.ColumnarSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.DefaultValueColumnReader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


// The segment build reads its record reader twice: a stats pass that sizes the dictionaries and writes total docs,
// and an index pass that writes the rows. When the index pass drops the only row of a sorted column value, that
// dictionary id owns no document and the sorted index used to seal with an invalid (MAX_VALUE, MIN_VALUE) range.
// The build must fail instead, and so must every other way the two passes can disagree.
public class SegmentGenerationWithTwoPassMismatchTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), SegmentGenerationWithTwoPassMismatchTest.class.getSimpleName());
  private static final String TABLE_NAME = "mismatchTest";
  private static final String SEGMENT_NAME = "mismatchSegment";
  private static final String ORG_ID = "orgId";
  private static final String BUCKET = "bucket";
  private static final String TS = "ts";
  private static final int NUM_ROWS_IN_STATS_PASS = 10;

  private Schema _schema;
  private TableConfig _tableConfig;
  private List<GenericRow> _rows;

  @BeforeMethod
  public void setUp() {
    FileUtils.deleteQuietly(TEMP_DIR);
    _schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(ORG_ID, DataType.STRING)
        .addSingleValueDimension(BUCKET, DataType.INT)
        .addDateTime(TS, DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setTimeColumnName(TS).build();

    // orgId is sorted, and its second value owns exactly the last row.
    _rows = new ArrayList<>(NUM_ROWS_IN_STATS_PASS);
    for (int i = 0; i < NUM_ROWS_IN_STATS_PASS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(ORG_ID, i < NUM_ROWS_IN_STATS_PASS - 1 ? "orgA" : "orgB");
      row.putValue(BUCKET, i < 5 ? 0 : 1);
      row.putValue(TS, 1_600_000_000_000L);
      _rows.add(row);
    }
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testBuildFailsWhenIndexPassReadsFewerRows()
      throws Exception {
    TwoPassRecordReader recordReader = new TwoPassRecordReader(_rows, _rows.subList(0, NUM_ROWS_IN_STATS_PASS - 1));
    IllegalStateException e = expectThrows(IllegalStateException.class, () -> build(recordReader));

    // The injected condition really happened: 10 rows read for stats, 9 rows read for the index.
    assertEquals(recordReader.getRowsReadPerPass(), List.of(NUM_ROWS_IN_STATS_PASS, NUM_ROWS_IN_STATS_PASS - 1));
    assertTrue(e.getMessage().contains("Segment: " + SEGMENT_NAME), e.getMessage());
    assertTrue(e.getMessage().contains(
        "indexed " + (NUM_ROWS_IN_STATS_PASS - 1) + " rows but the stats pass counted " + NUM_ROWS_IN_STATS_PASS
            + " rows"), e.getMessage());

    // Closing the half-built sorted index also fails on the empty dictionary id. That stays attached to the build
    // failure instead of replacing it.
    assertEquals(e.getSuppressed().length, 1);
    assertTrue(e.getSuppressed()[0] instanceof IOException);
    assertTrue(e.getSuppressed()[0].getMessage().contains("Dictionary id: 1 of sorted column: " + ORG_ID),
        e.getSuppressed()[0].getMessage());

    assertFalse(new File(TEMP_DIR, SEGMENT_NAME).exists());
  }

  // An extra row would write past every index creator sized for the stats-pass total, so the build stops before
  // indexing it.
  @Test
  public void testBuildFailsWhenIndexPassReadsMoreRows()
      throws Exception {
    List<GenericRow> indexPassRows = new ArrayList<>(_rows);
    indexPassRows.add(_rows.get(NUM_ROWS_IN_STATS_PASS - 1));
    TwoPassRecordReader recordReader = new TwoPassRecordReader(_rows, indexPassRows);
    IllegalStateException e = expectThrows(IllegalStateException.class, () -> build(recordReader));

    assertEquals(recordReader.getRowsReadPerPass(), List.of(NUM_ROWS_IN_STATS_PASS, NUM_ROWS_IN_STATS_PASS + 1));
    assertTrue(e.getMessage().contains(
        "indexed more than " + NUM_ROWS_IN_STATS_PASS + " rows but the stats pass counted " + NUM_ROWS_IN_STATS_PASS
            + " rows"), e.getMessage());
    assertEquals(e.getSuppressed().length, 0);
    assertFalse(new File(TEMP_DIR, SEGMENT_NAME).exists());
  }

  // The same rows in a different order keep the count and every value, but a sorted column would get overlapping
  // doc id ranges.
  @Test
  public void testBuildFailsWhenIndexPassReordersRows()
      throws Exception {
    List<GenericRow> indexPassRows = new ArrayList<>(_rows);
    indexPassRows.add(0, indexPassRows.remove(NUM_ROWS_IN_STATS_PASS - 1));
    TwoPassRecordReader recordReader = new TwoPassRecordReader(_rows, indexPassRows);
    IllegalStateException e = expectThrows(IllegalStateException.class, () -> build(recordReader));

    assertTrue(e.getMessage().contains("after dictionary id: 1"), e.getMessage());
    assertFalse(new File(TEMP_DIR, SEGMENT_NAME).exists());
  }

  // now() runs once per pass, so the index pass computes values the stats pass never put in the dictionary.
  @Test
  public void testBuildFailsOnVolatileTransform() {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig("ingestTs", "now()")));
    _tableConfig.setIngestionConfig(ingestionConfig);
    _schema.addField(new DimensionFieldSpec("ingestTs", DataType.LONG, true));
    // Sleeping between the passes keeps every index-pass now() value later than every stats-pass value.
    GenericRowRecordReader recordReader = new GenericRowRecordReader(_rows) {
      @Override
      public void rewind() {
        try {
          Thread.sleep(20);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        super.rewind();
      }
    };
    IllegalStateException e = expectThrows(IllegalStateException.class, () -> build(recordReader));

    assertTrue(e.getMessage().contains("is not in the dictionary for column: ingestTs"), e.getMessage());
    assertTrue(e.getMessage().contains("volatile ingestion transform"), e.getMessage());
  }

  // The column-major build has no record reader to count, so each column checks its own count. A column reader that
  // reports one doc fewer on the index pass would otherwise leave that column's indexes a doc short.
  @Test
  public void testColumnarBuildFailsWhenColumnShrinks()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(ORG_ID, DataType.STRING)
        .addSingleValueDimension(BUCKET, DataType.INT)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
    Map<String, ColumnReader> columnReaders = new HashMap<>();
    columnReaders.put(BUCKET,
        new DefaultValueColumnReader(BUCKET, NUM_ROWS_IN_STATS_PASS, schema.getFieldSpecFor(BUCKET)));
    columnReaders.put(ORG_ID,
        new DefaultValueColumnReader(ORG_ID, NUM_ROWS_IN_STATS_PASS, schema.getFieldSpecFor(ORG_ID)) {
          private boolean _rewound;

          @Override
          public void rewind() {
            _rewound = true;
          }

          @Override
          public int getTotalDocs() {
            return _rewound ? NUM_ROWS_IN_STATS_PASS - 1 : NUM_ROWS_IN_STATS_PASS;
          }
        });

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new ColumnarSegmentCreationDataSource(columnReaders), null);
    IllegalStateException e = expectThrows(IllegalStateException.class, driver::build);

    assertEquals(e.getMessage(),
        "Column: " + ORG_ID + " indexed " + (NUM_ROWS_IN_STATS_PASS - 1) + " docs but the stats pass counted "
            + NUM_ROWS_IN_STATS_PASS + " docs");
    assertFalse(new File(TEMP_DIR, SEGMENT_NAME).exists());
  }

  // The same rows through a reader that yields all of them on every pass build a healthy segment.
  @Test
  public void testBuildSucceedsWhenPassesAgree()
      throws Exception {
    build(new GenericRowRecordReader(_rows));
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(new File(TEMP_DIR, SEGMENT_NAME));
    assertEquals(metadata.getTotalDocs(), NUM_ROWS_IN_STATS_PASS);
    assertTrue(metadata.getColumnMetadataFor(ORG_ID).isSorted());
  }

  private void build(RecordReader recordReader)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _schema);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();
  }

  // Yields the stats-pass rows until its second rewind and the index-pass rows after that. The driver rewinds once
  // inside init() before the stats pass, then again before the index pass.
  private static class TwoPassRecordReader implements RecordReader {
    private final List<GenericRow> _statsPassRows;
    private final List<GenericRow> _indexPassRows;
    private final List<Integer> _rowsReadPerPass = new ArrayList<>();

    private List<GenericRow> _rows;
    private int _numRewinds;
    private int _nextRowId;
    private int _rowsReadThisPass;

    TwoPassRecordReader(List<GenericRow> statsPassRows, List<GenericRow> indexPassRows) {
      _statsPassRows = statsPassRows;
      _indexPassRows = indexPassRows;
      _rows = statsPassRows;
    }

    List<Integer> getRowsReadPerPass() {
      List<Integer> counts = new ArrayList<>(_rowsReadPerPass);
      if (_rowsReadThisPass > 0) {
        counts.add(_rowsReadThisPass);
      }
      return counts;
    }

    @Override
    public void init(File dataFile, @Nullable Set<String> fieldsToRead,
        @Nullable RecordReaderConfig recordReaderConfig) {
    }

    @Override
    public boolean hasNext() {
      return _nextRowId < _rows.size();
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      reuse.init(_rows.get(_nextRowId++));
      _rowsReadThisPass++;
      return reuse;
    }

    @Override
    public void rewind() {
      if (_rowsReadThisPass > 0) {
        _rowsReadPerPass.add(_rowsReadThisPass);
      }
      _rowsReadThisPass = 0;
      _nextRowId = 0;
      _numRewinds++;
      _rows = _numRewinds >= 2 ? _indexPassRows : _statsPassRows;
    }

    @Override
    public void close() {
    }
  }
}
