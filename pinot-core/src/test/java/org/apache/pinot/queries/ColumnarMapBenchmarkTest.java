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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Micro-benchmark comparing COLUMNAR_MAP (two-tier) vs flattened columns.
 * Ingests 5M synthetic docs into two segments and compares query latency and storage cost.
 *
 * <p>Disabled by default — run manually:
 * <pre>
 *   mvn test -pl pinot-core -Dtest=ColumnarMapBenchmarkTest -DenableBenchmark=true
 * </pre>
 */
public class ColumnarMapBenchmarkTest extends BaseQueriesTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarMapBenchmarkTest.class);

  private static final int NUM_SEGMENTS = 10;
  private static final int DOCS_PER_SEGMENT = 2_000_000;
  private static final int WARMUP_RUNS = 5;
  private static final int MEASURED_RUNS = 5;

  private static final String MAP_TABLE = "benchmark_columnar_map";
  private static final String FLAT_TABLE = "benchmark_flattened";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "ColumnarMapBenchmark");
  private static final File CACHE_DIR = new File(System.getProperty("user.home"), ".pinot-benchmark-cache");

  // Map key definitions: name → fill rate
  private static final String[] KEY_NAMES = {
      "tenancy", "country", "device_os", "event_type", "session_id",
      "discount_code", "referral", "error_code", "debug_trace", "experiment_id"
  };
  private static final double[] FILL_RATES = {
      1.0, 1.0, 0.95, 0.80, 0.60,
      0.30, 0.15, 0.05, 0.02, 0.01
  };

  // Value pools for each key
  private static final String[][] VALUE_POOLS = {
      {"uber/production", "uber/testing", "uber_eats", "uber_freight", "uber_connect"},
      generateCountries(),
      {"ios", "android", "web", "unknown"},
      {"page_view", "click", "purchase", "signup", "logout", "search", "add_to_cart", "checkout",
          "payment", "refund", "review", "share", "bookmark", "notify", "subscribe",
          "unsubscribe", "report", "upload", "download", "export"},
      null, // session_id generated dynamically (high cardinality)
      generateCodes("SAVE", 100),
      {"google", "facebook", "twitter", "email", "instagram", "tiktok", "youtube", "reddit",
          "linkedin", "snapchat", "pinterest", "whatsapp", "telegram", "discord", "slack",
          "organic", "direct", "affiliate", "partner", "other",
          "blog", "podcast", "newsletter", "press", "event",
          "referral_program", "word_of_mouth", "app_store", "play_store", "sms"},
      {"E001", "E002", "E003", "E004", "E005", "E006", "E007", "E008", "E009", "E010"},
      null, // debug_trace generated dynamically (high cardinality)
      generateCodes("exp-", 50)
  };

  // Inverted index keys (must match between MAP and flattened)
  private static final Set<String> INVERTED_INDEX_KEYS = Set.of("tenancy", "country");

  private final List<IndexSegment> _mapSegments = new ArrayList<>();
  private final List<IndexSegment> _flatSegments = new ArrayList<>();
  private List<IndexSegment> _activeSegments;  // toggled per query
  private long _mapSegmentSize;
  private long _flatSegmentSize;
  private final List<BenchmarkResult> _results = new ArrayList<>();

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _activeSegments.get(0);
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _activeSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    if (!"true".equals(System.getProperty("enableBenchmark"))) {
      LOGGER.info("Benchmark disabled. Set -DenableBenchmark=true to run.");
      return;
    }

    FileUtils.deleteDirectory(INDEX_DIR);
    INDEX_DIR.mkdirs();
    CACHE_DIR.mkdirs();

    int totalDocs = NUM_SEGMENTS * DOCS_PER_SEGMENT;
    LOGGER.info("Setting up {} segments × {} docs = {} total docs...", NUM_SEGMENTS, DOCS_PER_SEGMENT, totalDocs);

    // Create or reuse cached template segments (one MAP + one Flat with DOCS_PER_SEGMENT docs)
    String cacheKey = "docs_" + DOCS_PER_SEGMENT;
    File cachedMapTemplate = new File(CACHE_DIR, "mapTemplate_" + cacheKey);
    File cachedFlatTemplate = new File(CACHE_DIR, "flatTemplate_" + cacheKey);

    if (!cachedMapTemplate.exists() || !cachedFlatTemplate.exists()) {
      LOGGER.info("Cache miss — creating template segments with {} docs...", DOCS_PER_SEGMENT);
      File tempBuildDir = new File(CACHE_DIR, "build_tmp");
      FileUtils.deleteDirectory(tempBuildDir);
      tempBuildDir.mkdirs();

      List<GenericRow> rows = generateRows(0);
      createMapSegment(rows, "mapTemplate_" + cacheKey, tempBuildDir);
      createFlattenedSegment(rows, "flatTemplate_" + cacheKey, tempBuildDir);
      rows.clear();

      // Move to cache
      File builtMap = new File(tempBuildDir, "mapTemplate_" + cacheKey);
      File builtFlat = new File(tempBuildDir, "flatTemplate_" + cacheKey);
      if (cachedMapTemplate.exists()) {
        FileUtils.deleteDirectory(cachedMapTemplate);
      }
      if (cachedFlatTemplate.exists()) {
        FileUtils.deleteDirectory(cachedFlatTemplate);
      }
      FileUtils.moveDirectory(builtMap, cachedMapTemplate);
      FileUtils.moveDirectory(builtFlat, cachedFlatTemplate);
      FileUtils.deleteDirectory(tempBuildDir);
      LOGGER.info("Template segments cached to {}", CACHE_DIR);
    } else {
      LOGGER.info("Cache hit — reusing template segments from {}", CACHE_DIR);
    }

    // Copy template segments N times and load them
    _mapSegmentSize = 0;
    _flatSegmentSize = 0;

    for (int seg = 0; seg < NUM_SEGMENTS; seg++) {
      String mapSegName = "mapSegment_" + seg;
      String flatSegName = "flatSegment_" + seg;
      File mapDir = new File(INDEX_DIR, mapSegName);
      File flatDir = new File(INDEX_DIR, flatSegName);

      FileUtils.copyDirectory(cachedMapTemplate, mapDir);
      FileUtils.copyDirectory(cachedFlatTemplate, flatDir);

      _mapSegments.add(ImmutableSegmentLoader.load(mapDir, ReadMode.mmap));
      _flatSegments.add(ImmutableSegmentLoader.load(flatDir, ReadMode.mmap));
      _mapSegmentSize += FileUtils.sizeOfDirectory(mapDir);
      _flatSegmentSize += FileUtils.sizeOfDirectory(flatDir);
    }

    LOGGER.info("All {} segments loaded. MAP total: {} bytes ({} B/doc), Flat total: {} bytes ({} B/doc)",
        NUM_SEGMENTS, _mapSegmentSize, _mapSegmentSize / totalDocs,
        _flatSegmentSize, _flatSegmentSize / totalDocs);
  }

  @Test(enabled = false,
      description = "Manual benchmark — run with -Dtest=ColumnarMapBenchmarkTest -DenableBenchmark=true and "
          + "the @Test-method-level enabled flag flipped to true")
  public void testBenchmark()
      throws Exception {
    if (_mapSegments.isEmpty()) {
      LOGGER.info("Skipping benchmark (not enabled). Set -DenableBenchmark=true to run.");
      return;
    }

    // Q1: Full-scan COUNT with filter (dense key, inverted index)
    benchmark("Q1: COUNT + filter (dense)",
        "SELECT count(*) FROM " + MAP_TABLE + " WHERE props['country'] = 'US'",
        "SELECT count(*) FROM " + FLAT_TABLE + " WHERE props__country = 'US'");

    // Q2: SUM aggregation (full scan, dense key)
    benchmark("Q2: SUM (full scan)",
        "SELECT sum(amount) FROM " + MAP_TABLE + " WHERE props['tenancy'] = 'uber/production'",
        "SELECT sum(amount) FROM " + FLAT_TABLE + " WHERE props__tenancy = 'uber/production'");

    // Q3: COUNT aggregation on dense key (full scan, no group by — just touch the column)
    benchmark("Q3: COUNT DISTINCT (dense)",
        "SELECT count(distinct props['country']) FROM " + MAP_TABLE,
        "SELECT count(distinct props__country) FROM " + FLAT_TABLE);

    // Q4: GROUP BY single dense key (full scan)
    benchmark("Q4: GROUP BY 1 key",
        "SELECT props['country'], count(*) FROM " + MAP_TABLE + " GROUP BY props['country']",
        "SELECT props__country, count(*) FROM " + FLAT_TABLE + " GROUP BY props__country");

    // Q5: GROUP BY two dense keys (full scan)
    benchmark("Q5: GROUP BY 2 keys",
        "SELECT props['country'], props['tenancy'], count(*) FROM " + MAP_TABLE
            + " GROUP BY props['country'], props['tenancy']",
        "SELECT props__country, props__tenancy, count(*) FROM " + FLAT_TABLE
            + " GROUP BY props__country, props__tenancy");

    // Q6: Filtered GROUP BY (inverted index filter + GROUP BY on different key)
    benchmark("Q6: Filtered GROUP BY",
        "SELECT props['country'], count(*) FROM " + MAP_TABLE
            + " WHERE props['tenancy'] = 'uber/production' GROUP BY props['country']",
        "SELECT props__country, count(*) FROM " + FLAT_TABLE
            + " WHERE props__tenancy = 'uber/production' GROUP BY props__country");

    // Q7: DISTINCT count on dense key (full scan)
    benchmark("Q7: DISTINCT COUNT",
        "SELECT distinctcount(props['country']) FROM " + MAP_TABLE,
        "SELECT distinctcount(props__country) FROM " + FLAT_TABLE);

    // Q8: Multi-filter AND (two dense keys with inverted indexes)
    benchmark("Q8: AND filter (2 keys)",
        "SELECT count(*) FROM " + MAP_TABLE
            + " WHERE props['country'] = 'US' AND props['tenancy'] = 'uber/production'",
        "SELECT count(*) FROM " + FLAT_TABLE
            + " WHERE props__country = 'US' AND props__tenancy = 'uber/production'");

    // Write report
    writeReport();
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    for (IndexSegment seg : _mapSegments) {
      seg.destroy();
    }
    for (IndexSegment seg : _flatSegments) {
      seg.destroy();
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  // ---- Data Generation ----

  private List<GenericRow> generateRows(int segmentIndex) {
    Random rng = new Random(42 + segmentIndex);  // deterministic per segment
    List<GenericRow> rows = new ArrayList<>(DOCS_PER_SEGMENT);
    for (int i = 0; i < DOCS_PER_SEGMENT; i++) {
      GenericRow row = new GenericRow();
      row.putValue("userId", "user_" + (rng.nextInt(500_000)));
      row.putValue("timestamp", System.currentTimeMillis() - rng.nextInt(86400_000));
      row.putValue("amount", Math.round(rng.nextDouble() * 100.0) / 100.0 * 10.0);  // ~1000 distinct values

      Map<String, Object> props = new HashMap<>();
      for (int k = 0; k < KEY_NAMES.length; k++) {
        if (rng.nextDouble() < FILL_RATES[k]) {
          String value;
          if (VALUE_POOLS[k] != null) {
            value = VALUE_POOLS[k][rng.nextInt(VALUE_POOLS[k].length)];
          } else if (KEY_NAMES[k].equals("session_id")) {
            value = "sess_" + rng.nextInt(10_000);  // cap cardinality
          } else {
            // debug_trace: high cardinality
            value = Long.toHexString(rng.nextLong());
          }
          props.put(KEY_NAMES[k], value);
        }
      }
      row.putValue("props", props);
      rows.add(row);
    }
    return rows;
  }

  private void createMapSegment(List<GenericRow> rows, String mapSegmentName)
      throws Exception {
    createMapSegment(rows, mapSegmentName, INDEX_DIR);
  }

  private void createMapSegment(List<GenericRow> rows, String mapSegmentName, File outDir)
      throws Exception {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName(MAP_TABLE)
        .addSingleValueDimension("userId", FieldSpec.DataType.STRING)
        .addSingleValueDimension("timestamp", FieldSpec.DataType.LONG)
        .addMetric("amount", FieldSpec.DataType.DOUBLE)
        .addField(new ComplexFieldSpec("props", FieldSpec.DataType.MAP, true,
            Map.of("key", new DimensionFieldSpec("key", FieldSpec.DataType.STRING, true),
                "value", new DimensionFieldSpec("value", FieldSpec.DataType.STRING, true))))
        .build();

    ObjectNode columnarMapNode = JsonUtils.newObjectNode();
    columnarMapNode.put("enabled", true);
    columnarMapNode.put("maxKeys", 1000);
    columnarMapNode.put("enableInvertedIndexForAll", false);
    columnarMapNode.put("denseKeyThreshold", 0.5);
    columnarMapNode.set("invertedIndexKeys",
        JsonUtils.newObjectNode().arrayNode().add("tenancy").add("country"));
    ObjectNode indexesNode = JsonUtils.newObjectNode();
    indexesNode.set("columnar_map", columnarMapNode);
    FieldConfig propsFieldConfig = new FieldConfig.Builder("props")
        .withIndexes(indexesNode)
        .withProperties(Map.of(
            "denseKeyThreshold", "0.5",
            "invertedIndexKeys", "tenancy,country"
        ))
        .build();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MAP_TABLE)
        .setFieldConfigList(List.of(propsFieldConfig))
        .build();

    buildSegment(schema, tableConfig, mapSegmentName, rows, outDir);
  }

  private void createFlattenedSegment(List<GenericRow> rows, String flatSegmentName)
      throws Exception {
    createFlattenedSegment(rows, flatSegmentName, INDEX_DIR);
  }

  private void createFlattenedSegment(List<GenericRow> rows, String flatSegmentName, File outDir)
      throws Exception {
    Schema.SchemaBuilder sb = new Schema.SchemaBuilder()
        .setSchemaName(FLAT_TABLE)
        .addSingleValueDimension("userId", FieldSpec.DataType.STRING)
        .addSingleValueDimension("timestamp", FieldSpec.DataType.LONG)
        .addMetric("amount", FieldSpec.DataType.DOUBLE);
    for (String key : KEY_NAMES) {
      sb.addSingleValueDimension("props__" + key, FieldSpec.DataType.STRING);
    }
    Schema schema = sb.build();

    // Inverted index on matching keys
    List<String> invertedCols = new ArrayList<>();
    for (String key : INVERTED_INDEX_KEYS) {
      invertedCols.add("props__" + key);
    }

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(FLAT_TABLE)
        .setInvertedIndexColumns(invertedCols)
        .build();

    // Convert rows: extract map keys into top-level columns
    List<GenericRow> flatRows = new ArrayList<>(rows.size());
    for (GenericRow row : rows) {
      GenericRow flatRow = new GenericRow();
      flatRow.putValue("userId", row.getValue("userId"));
      flatRow.putValue("timestamp", row.getValue("timestamp"));
      flatRow.putValue("amount", row.getValue("amount"));

      @SuppressWarnings("unchecked")
      Map<String, Object> props = (Map<String, Object>) row.getValue("props");
      for (String key : KEY_NAMES) {
        Object val = props != null ? props.get(key) : null;
        flatRow.putValue("props__" + key, val != null ? val : "");
      }
      flatRows.add(flatRow);
    }

    buildSegment(schema, tableConfig, flatSegmentName, flatRows, outDir);
  }

  private void buildSegment(Schema schema, TableConfig tableConfig, String segmentName,
      List<GenericRow> rows)
      throws Exception {
    buildSegment(schema, tableConfig, segmentName, rows, INDEX_DIR);
  }

  private void buildSegment(Schema schema, TableConfig tableConfig, String segmentName,
      List<GenericRow> rows, File outDir)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outDir.getAbsolutePath());
    config.setTableName(schema.getSchemaName());
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
  }

  // ---- Benchmarking ----

  private void benchmark(String label, String mapQuery, String flatQuery) {
    LOGGER.info("Benchmarking: {}", label);

    // Warmup + measure on MAP segments
    _activeSegments = _mapSegments;
    long[] mapTimes = runQuery(mapQuery, WARMUP_RUNS, MEASURED_RUNS);

    // Warmup + measure on Flattened segments
    _activeSegments = _flatSegments;
    long[] flatTimes = runQuery(flatQuery, WARMUP_RUNS, MEASURED_RUNS);

    long mapMedian = median(mapTimes);
    long flatMedian = median(flatTimes);
    double ratio = flatMedian > 0 ? (double) mapMedian / flatMedian : 0;

    LOGGER.info("  MAP: {}ms (median), Flat: {}ms (median), Ratio: {}",
        mapMedian, flatMedian, String.format("%.2fx", ratio));

    _results.add(new BenchmarkResult(label, mapTimes, flatTimes));
  }

  private long[] runQuery(String query, int warmup, int measured) {
    // Warmup
    for (int i = 0; i < warmup; i++) {
      try {
        BrokerResponseNative response = getBrokerResponse(query);
        if (response.getExceptionsSize() > 0) {
          LOGGER.warn("Query exception during warmup: {}", response.getExceptions());
        }
      } catch (Exception e) {
        LOGGER.warn("Warmup query failed: {}", e.getMessage());
      }
    }

    // Measured runs
    long[] times = new long[measured];
    for (int i = 0; i < measured; i++) {
      long start = System.nanoTime();
      try {
        getBrokerResponse(query);
      } catch (Exception e) {
        LOGGER.warn("Measured query failed: {}", e.getMessage());
      }
      times[i] = (System.nanoTime() - start) / 1_000_000;  // convert to ms
    }
    return times;
  }

  private static long median(long[] values) {
    long[] sorted = values.clone();
    Arrays.sort(sorted);
    return sorted[sorted.length / 2];
  }

  // ---- Report Generation ----

  private void writeReport()
      throws IOException {
    File reportFile = new File(INDEX_DIR.getParentFile(), "benchmark-results-columnar-map-vs-flattened.md");
    try (PrintWriter pw = new PrintWriter(new FileWriter(reportFile))) {
      pw.println("# COLUMNAR_MAP vs Flattened Benchmark Results");
      pw.println();
      int totalDocs = NUM_SEGMENTS * DOCS_PER_SEGMENT;
      pw.printf("**Date:** %s%n", Instant.now());
      pw.printf("**Segments:** %d × %,d docs = **%,d total docs**%n", NUM_SEGMENTS, DOCS_PER_SEGMENT, totalDocs);
      pw.printf("**Warmup:** %d runs, **Measured:** %d runs%n", WARMUP_RUNS, MEASURED_RUNS);
      pw.printf("**Dense keys (>50%% fill):** tenancy, country, device_os, event_type, session_id%n");
      pw.printf("**Sparse keys (<50%% fill):** discount_code, referral, error_code, debug_trace, experiment_id%n");
      pw.println();

      // Storage
      pw.println("## Storage");
      pw.println();
      pw.println("| Table | Total Size | Bytes/Doc |");
      pw.println("|---|---|---|");
      pw.printf("| COLUMNAR_MAP | %.1f MB | %.1f |%n",
          _mapSegmentSize / (1024.0 * 1024.0), (double) _mapSegmentSize / totalDocs);
      pw.printf("| Flattened | %.1f MB | %.1f |%n",
          _flatSegmentSize / (1024.0 * 1024.0), (double) _flatSegmentSize / totalDocs);
      pw.printf("| **Ratio** | **%.2fx** | |%n",
          (double) _mapSegmentSize / _flatSegmentSize);
      pw.println();

      // Query Latency
      pw.println("## Query Latency (median of " + MEASURED_RUNS + " runs, ms)");
      pw.println();
      pw.println("| Query | MAP (ms) | Flat (ms) | Ratio | MAP range | Flat range |");
      pw.println("|---|---|---|---|---|---|");
      for (BenchmarkResult r : _results) {
        long mapMedian = median(r._mapTimes);
        long flatMedian = median(r._flatTimes);
        double ratio = flatMedian > 0 ? (double) mapMedian / flatMedian : 0;
        pw.printf("| %s | %d | %d | %.2fx | %d-%d | %d-%d |%n",
            r._label, mapMedian, flatMedian, ratio,
            min(r._mapTimes), max(r._mapTimes),
            min(r._flatTimes), max(r._flatTimes));
      }
      pw.println();

      // Raw data
      pw.println("## Raw Data (all runs, ms)");
      pw.println();
      for (BenchmarkResult r : _results) {
        pw.printf("**%s**%n", r._label);
        pw.printf("- MAP: %s%n", Arrays.toString(r._mapTimes));
        pw.printf("- Flat: %s%n%n", Arrays.toString(r._flatTimes));
      }
    }
    LOGGER.info("Report written to: {}", reportFile.getAbsolutePath());
  }

  private static long min(long[] values) {
    long m = Long.MAX_VALUE;
    for (long v : values) {
      m = Math.min(m, v);
    }
    return m;
  }

  private static long max(long[] values) {
    long m = Long.MIN_VALUE;
    for (long v : values) {
      m = Math.max(m, v);
    }
    return m;
  }

  // ---- Helpers ----

  private static String[] generateCountries() {
    return new String[]{
        "US", "MX", "BR", "CA", "GB", "FR", "DE", "ES", "IT", "NL",
        "AU", "JP", "KR", "IN", "ID", "TH", "VN", "PH", "MY", "SG",
        "AE", "SA", "EG", "ZA", "NG", "KE", "GH", "CO", "AR", "CL",
        "PE", "EC", "VE", "UY", "PY", "BO", "CR", "PA", "DO", "GT",
        "HN", "SV", "NI", "CU", "JM", "TT", "BZ", "HT", "PR", "BB"
    };
  }

  private static String[] generateCodes(String prefix, int count) {
    String[] codes = new String[count];
    for (int i = 0; i < count; i++) {
      codes[i] = prefix + String.format("%03d", i + 1);
    }
    return codes;
  }

  private static class BenchmarkResult {
    final String _label;
    final long[] _mapTimes;
    final long[] _flatTimes;

    BenchmarkResult(String label, long[] mapTimes, long[] flatTimes) {
      _label = label;
      _mapTimes = mapTimes;
      _flatTimes = flatTimes;
    }
  }
}
