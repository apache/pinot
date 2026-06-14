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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.RegexpPatternConverterUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.FieldConfig.IndexType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Empirical comparison of the FST index versus the TEXT (Lucene full-text) index on a single STRING column of
 * {@value #NUM_ROWS} rows, across a range of value cardinalities.
 *
 * For each cardinality the test builds three segments that are identical except for the index on the text column:
 *   <ul>
 *     <li>a baseline segment (dictionary + forward index only),</li>
 *     <li>a segment with an additional FST index, and</li>
 *     <li>a segment with an additional TEXT index.</li>
 *   </ul>
 * The on-disk size attributable to each index is computed as (segment size - baseline size), which isolates the
 * index from the shared dictionary/forward index. Query latency is measured at the single-segment operator level for
 * an equivalent predicate on each index (case-sensitive {@code REGEXP_LIKE} for FST, {@code TEXT_MATCH} for TEXT)
 * that matches the same set of rows.
 *
 * This is a benchmark, not a correctness gate. It is disabled by default ({@code @Test(enabled = false)}) so it does
 * not run in CI; flip it to enabled (or run the single method from an IDE) to reproduce the numbers. It prints two
 * report tables (index size and end-to-end latency) to stdout.
 */
public class FstVsTextSizePerfBenchmarkTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), FstVsTextSizePerfBenchmarkTest.class.getSimpleName());
  private static final String TABLE_NAME = "testTable";
  private static final String TEXT_COL = "TEXT_COL";
  private static final String INT_COL = "INT_COL";

  private static final int NUM_ROWS = 1_000_000;
  private static final int[] CARDINALITIES = {10, 1_000, 100_000, 1_000_000};
  // A standalone token present in exactly the values whose index is a multiple of 10 (~10% of rows).
  private static final String NEEDLE = "needle";
  private static final String[] VOCAB =
      {"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet"};

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension(TEXT_COL, FieldSpec.DataType.STRING)
      .addMetric(INT_COL, FieldSpec.DataType.INT)
      .build();

  private static final int WARMUP_ITERS = 2;
  private static final int MEASURE_ITERS = 9;

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

  /** Deterministic, unique-per-{@code v} text value; carries the NEEDLE token when {@code v % 10 == 0}. */
  private static String value(int v) {
    String w1 = VOCAB[v % VOCAB.length];
    String w2 = VOCAB[(v / 10) % VOCAB.length];
    String w3 = (v % 10 == 0) ? NEEDLE : VOCAB[(v / 7) % VOCAB.length];
    return w1 + ' ' + w2 + ' ' + w3 + " id" + v;
  }

  /** Streams {@link #NUM_ROWS} rows, cycling through {@code cardinality} distinct values, without materializing. */
  private static final class GeneratingRecordReader implements RecordReader {
    private final int _numRows;
    private final int _cardinality;
    private int _next;

    GeneratingRecordReader(int numRows, int cardinality) {
      _numRows = numRows;
      _cardinality = cardinality;
    }

    @Override
    public void init(File dataFile, @Nullable Set<String> fieldsToRead, @Nullable RecordReaderConfig config) {
    }

    @Override
    public boolean hasNext() {
      return _next < _numRows;
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      reuse.clear();
      reuse.putValue(TEXT_COL, value(_next % _cardinality));
      reuse.putValue(INT_COL, _next);
      _next++;
      return reuse;
    }

    @Override
    public void rewind() {
      _next = 0;
    }

    @Override
    public void close() {
    }
  }

  /** Builds a segment with the given index (or none when {@code indexType == null}) and returns its directory. */
  private File buildSegment(String segmentName, int cardinality, @Nullable IndexType indexType)
      throws Exception {
    List<FieldConfig> fieldConfigs = indexType == null ? List.of()
        : List.of(new FieldConfig(TEXT_COL, EncodingType.DICTIONARY, List.of(indexType), null, null));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setFieldConfigList(fieldConfigs).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GeneratingRecordReader(NUM_ROWS, cardinality)) {
      driver.init(config, recordReader);
      driver.build();
    }
    return new File(INDEX_DIR, segmentName);
  }

  /**
   * Builds a standalone Lucene text index over the {@code cardinality} distinct values only (one document per
   * dictionary entry, not per row, with Lucene docId == dictId) and returns its directory.
   */
  private File buildTextOnDictionaryIndex(int cardinality)
      throws Exception {
    File dir = new File(INDEX_DIR, "textdict_" + cardinality);
    FileUtils.deleteQuietly(dir);
    FileUtils.forceMkdir(dir);
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    try (LuceneTextIndexCreator creator =
        new LuceneTextIndexCreator(TEXT_COL, dir, true, false, null, null, config)) {
      for (int v = 0; v < cardinality; v++) {
        creator.add(value(v));
      }
      creator.seal();
    }
    return dir;
  }

  /**
   * Resolves a set of matching dictionary ids to row docIds the way Pinot's scan filter does when no inverted index
   * exists: scan the forward index of every row and test membership. Returns the matched row count.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static long resolveByForwardScan(ImmutableRoaringBitmap matchedDictIds, ForwardIndexReader forwardIndex,
      int numDocs) {
    ForwardIndexReaderContext context = forwardIndex.createContext();
    long count = 0;
    for (int docId = 0; docId < numDocs; docId++) {
      if (matchedDictIds.contains(forwardIndex.getDictId(docId, context))) {
        count++;
      }
    }
    return count;
  }

  /** Median latency (ms) of an index-level lookup over {@link #MEASURE_ITERS} runs after warmup. */
  private static double medianMs(Runnable lookup) {
    for (int i = 0; i < WARMUP_ITERS; i++) {
      lookup.run();
    }
    double[] samples = new double[MEASURE_ITERS];
    for (int i = 0; i < MEASURE_ITERS; i++) {
      long start = System.nanoTime();
      lookup.run();
      samples[i] = (System.nanoTime() - start) / 1_000_000.0;
    }
    Arrays.sort(samples);
    return samples[MEASURE_ITERS / 2];
  }

  private TableConfig tableConfig(@Nullable IndexType indexType) {
    List<FieldConfig> fieldConfigs = indexType == null ? List.of()
        : List.of(new FieldConfig(TEXT_COL, EncodingType.DICTIONARY, List.of(indexType), null, null));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setFieldConfigList(fieldConfigs).build();
  }

  private long countQuery(String query) {
    AggregationResultsBlock block = (AggregationResultsBlock) ((Operator) getOperator(query)).nextBlock();
    return ((Number) block.getResults().get(0)).longValue();
  }

  /** Median wall-clock latency (ms) over {@link #MEASURE_ITERS} runs of the single-segment COUNT(*) query. */
  private double medianLatencyMs(String query) {
    for (int i = 0; i < WARMUP_ITERS; i++) {
      ((Operator) getOperator(query)).nextBlock();
    }
    double[] samples = new double[MEASURE_ITERS];
    for (int i = 0; i < MEASURE_ITERS; i++) {
      long start = System.nanoTime();
      ((Operator) getOperator(query)).nextBlock();
      samples[i] = (System.nanoTime() - start) / 1_000_000.0;
    }
    Arrays.sort(samples);
    return samples[MEASURE_ITERS / 2];
  }

  // Disabled by default: heavy (builds 1M-row segments, ~30-60s). Enable locally to reproduce the numbers.
  @Test(enabled = false)
  public void benchmarkFstVsText()
      throws Exception {
    String fstQuery = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE REGEXP_LIKE(" + TEXT_COL + ", '.*" + NEEDLE
        + ".*')";
    String textQuery = "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE TEXT_MATCH(" + TEXT_COL + ", '" + NEEDLE + "')";

    // The Lucene RegExp that FST.getDictIds receives for REGEXP_LIKE(col, '.*needle.*').
    String fstLuceneRegex = RegexpPatternConverterUtils.regexpLikeToLuceneRegExp(".*" + NEEDLE + ".*");

    StringBuilder sizes = new StringBuilder();
    sizes.append(String.format("%nINDEX SIZE on a %,d-row STRING column%n", NUM_ROWS));
    sizes.append("=".repeat(72)).append('\n');
    sizes.append(String.format("%-12s | %-10s | %-11s | %-11s | %-11s%n",
        "cardinality", "baseline", "FST/dict", "TEXT/dict", "TEXT/row"));
    sizes.append("-".repeat(72)).append('\n');

    StringBuilder latencies = new StringBuilder();
    latencies.append(String.format("%nEND-TO-END LATENCY = index lookup + dictId->docId forward scan, median ms%n"));
    latencies.append("(FST/dict and TEXT/dict need the scan; TEXT/row returns docIds directly)%n");
    latencies.append("=".repeat(92)).append('\n');
    latencies.append(String.format("%-12s | %-14s | %-14s | %-18s | %-14s%n",
        "cardinality", "FST (engine)", "FST (manual)", "TEXT/dict (manual)", "TEXT/row (engine)"));
    latencies.append("-".repeat(92)).append('\n');

    for (int cardinality : CARDINALITIES) {
      File baseDir = buildSegment("base_" + cardinality, cardinality, null);
      File fstDir = buildSegment("fst_" + cardinality, cardinality, IndexType.FST);
      File textDir = buildSegment("text_" + cardinality, cardinality, IndexType.TEXT);

      long baseSize = FileUtils.sizeOfDirectory(baseDir);
      long fstIndexSize = FileUtils.sizeOfDirectory(fstDir) - baseSize;
      long textIndexSize = FileUtils.sizeOfDirectory(textDir) - baseSize;

      // TEXT index over the distinct dictionary values only (one Lucene doc per distinct value, docId == dictId).
      File textDictDir = buildTextOnDictionaryIndex(cardinality);
      long textOnDictSize = FileUtils.sizeOfDirectory(textDictDir);

      // FST: real engine e2e, plus manual e2e (getDictIds + forward-scan resolution) as a cross-check.
      ImmutableSegment fstSegment =
          ImmutableSegmentLoader.load(fstDir, new IndexLoadingConfig(tableConfig(IndexType.FST), SCHEMA));
      _indexSegment = fstSegment;
      _indexSegments = List.of(fstSegment);
      ForwardIndexReader fwd = (ForwardIndexReader) fstSegment.getDataSource(TEXT_COL).getForwardIndex();
      TextIndexReader fstReader = fstSegment.getDataSource(TEXT_COL).getFSTIndex();
      long fstEngineCount = countQuery(fstQuery);
      double fstEngineMs = medianLatencyMs(fstQuery);
      double fstManualMs = medianMs(() -> resolveByForwardScan(fstReader.getDictIds(fstLuceneRegex), fwd, NUM_ROWS));
      long fstResolvedCount = resolveByForwardScan(fstReader.getDictIds(fstLuceneRegex), fwd, NUM_ROWS);

      // TEXT/dict: getDocIds (returns dictIds) + the SAME forward-scan resolution as FST (column data is identical).
      double textDictManualMs;
      long textDictResolvedCount;
      try (LuceneTextIndexReader textDictReader =
          new LuceneTextIndexReader(TEXT_COL, textDictDir, cardinality, new TextIndexConfigBuilder().build())) {
        textDictManualMs = medianMs(() -> resolveByForwardScan(textDictReader.getDocIds(NEEDLE, null), fwd, NUM_ROWS));
        textDictResolvedCount = resolveByForwardScan(textDictReader.getDocIds(NEEDLE, null), fwd, NUM_ROWS);
      }
      fstSegment.destroy();
      FileUtils.deleteQuietly(textDictDir);

      // TEXT/row: real engine e2e (getDocIds returns final docIds, no resolution step).
      ImmutableSegment textSegment =
          ImmutableSegmentLoader.load(textDir, new IndexLoadingConfig(tableConfig(IndexType.TEXT), SCHEMA));
      _indexSegment = textSegment;
      _indexSegments = List.of(textSegment);
      long textEngineCount = countQuery(textQuery);
      double textRowEngineMs = medianLatencyMs(textQuery);
      textSegment.destroy();
      _indexSegment = null;

      // All four paths must select the same rows for the comparison to be meaningful.
      assertEquals(textDictResolvedCount, fstResolvedCount,
          "TEXT/dict and FST resolved different counts at cardinality " + cardinality);
      assertEquals(fstResolvedCount, fstEngineCount,
          "Manual resolution disagrees with the FST query engine at cardinality " + cardinality);
      assertEquals(fstEngineCount, textEngineCount,
          "FST and TEXT engine counts differ at cardinality " + cardinality);
      // Guard against a vacuously-consistent run (all paths returning 0): NEEDLE is seeded in exactly 10% of rows.
      // Exact because NUM_ROWS and every configured cardinality are multiples of 10.
      assertEquals(fstEngineCount, NUM_ROWS / 10L,
          "Expected ~10% of rows to carry the NEEDLE token at cardinality " + cardinality);

      sizes.append(String.format("%-12s | %10s | %11s | %11s | %11s%n", String.format("%,d", cardinality),
          FileUtils.byteCountToDisplaySize(baseSize), FileUtils.byteCountToDisplaySize(fstIndexSize),
          FileUtils.byteCountToDisplaySize(textOnDictSize), FileUtils.byteCountToDisplaySize(textIndexSize)));
      latencies.append(String.format("%-12s | %14.2f | %14.2f | %18.2f | %14.2f%n",
          String.format("%,d", cardinality), fstEngineMs, fstManualMs, textDictManualMs, textRowEngineMs));

      FileUtils.deleteQuietly(baseDir);
      FileUtils.deleteQuietly(fstDir);
      FileUtils.deleteQuietly(textDir);
    }
    sizes.append("=".repeat(72)).append('\n');
    latencies.append("=".repeat(92)).append('\n');
    StringBuilder report = sizes.append(latencies);
    System.out.println(report);
  }
}
