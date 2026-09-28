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
package org.apache.pinot.segment.local.segment.creator.impl.openstruct;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.segment.index.openstruct.MutableOpenStructIndex;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.creator.OpenStructColumnarSource;
import org.apache.pinot.segment.spi.memory.PinotDataBufferMemoryManager;
import org.apache.pinot.spi.config.table.OpenStructIndexConfig;
import org.apache.pinot.spi.data.ComplexFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.OpenStructNaming;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Asserts the columnar seal hand-off produces the same accumulated state, and the same written
/// column metadata, as feeding the identical data one document map at a time.
public class OpenStructColumnarHandoffTest {
  private static final int NUM_DOCS = 500;
  private PinotDataBufferMemoryManager _memMgr;
  private File _tempDir;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _memMgr = new DirectMemoryManager(OpenStructColumnarHandoffTest.class.getName());
    _tempDir = Files.createTempDirectory(OpenStructColumnarHandoffTest.class.getSimpleName()).toFile();
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    // Both seal() directories are non-empty on exit, so deleteOnExit() alone would leak them.
    // The delete must run even if close() throws, or a close() failure reintroduces the leak.
    try {
      _memMgr.close();
    } finally {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  private static ComplexFieldSpec openStructSpec() {
    return new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, Map.of());
  }

  /// Same rows for both paths: a mix of always-present, mostly-present and rare keys so dense and
  /// sparse classification both get exercised.
  private static Map<Integer, Map<String, Object>> buildRows() {
    Map<Integer, Map<String, Object>> rows = new HashMap<>();
    Random random = new Random(11);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      Map<String, Object> row = new HashMap<>();
      row.put("always", (long) docId);
      if (random.nextDouble() < 0.7) {
        row.put("mostly", (long) (docId * 2));
      }
      if (random.nextDouble() < 0.05) {
        row.put("rare", "v" + docId);
      }
      rows.put(docId, row);
    }
    return rows;
  }

  /// OPEN_STRUCT spec carrying declared child specs. "declaredFloat" and "rareDeclaredFloat" are
  /// fed `Long` values, so their declared type differs from both the raw value's type and the type
  /// the undeclared path would infer; "declaredBool" and "declaredTimestamp" are logical types
  /// whose stored types (INT and LONG) differ from the declared type; "declaredNeverPresent" is
  /// declared but never ingested, so neither path may register a column for it.
  private static ComplexFieldSpec declaredChildSpecs() {
    Map<String, FieldSpec> childFieldSpecs = new HashMap<>();
    childFieldSpecs.put("declaredFloat", new DimensionFieldSpec("declaredFloat", DataType.FLOAT, true));
    childFieldSpecs.put("declaredBool", new DimensionFieldSpec("declaredBool", DataType.BOOLEAN, true));
    childFieldSpecs.put("declaredTimestamp", new DimensionFieldSpec("declaredTimestamp", DataType.TIMESTAMP, true));
    childFieldSpecs.put("rareDeclaredFloat", new DimensionFieldSpec("rareDeclaredFloat", DataType.FLOAT, true));
    childFieldSpecs.put("declaredNeverPresent", new DimensionFieldSpec("declaredNeverPresent", DataType.INT, true));
    return new ComplexFieldSpec("metrics", DataType.OPEN_STRUCT, true, childFieldSpecs);
  }

  private static OpenStructIndexConfig configWithIgnoredKey() {
    return new OpenStructIndexConfig(false, null, null, null, null, null, null, null, Set.of("ignored"));
  }

  /// Rows for the declared-child-spec case. Fill rates are derived from docId rather than drawn
  /// randomly so the dense/sparse split the assertions pin is exact, and every doc carries the
  /// ignored key so both paths have to drop it (the per-doc path at ingestion, the columnar path
  /// during consumption).
  private static Map<Integer, Map<String, Object>> buildDeclaredRows() {
    Map<Integer, Map<String, Object>> rows = new HashMap<>();
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      Map<String, Object> row = new HashMap<>();
      row.put("declaredFloat", (long) docId);
      row.put("declaredTimestamp", new Timestamp(1_700_000_000_000L + docId * 1000L));
      row.put("ignored", "dropped-" + docId);
      if (docId % 5 != 0) {
        row.put("declaredBool", docId % 3 == 0);
      }
      if (docId % 10 < 6) {
        row.put("undeclaredString", "s" + docId);
      }
      if (docId % 97 == 0) {
        row.put("rareDeclaredFloat", (long) docId * 7);
      }
      if (docId % 89 == 0) {
        row.put("rareUndeclared", (long) docId);
      }
      rows.put(docId, row);
    }
    return rows;
  }

  private static OpenStructColumnSplitter newSplitter(File indexDir, ComplexFieldSpec fieldSpec,
      OpenStructIndexConfig config) {
    return new OpenStructColumnSplitter(indexDir, "metrics", "testTable_REALTIME", fieldSpec, config);
  }

  @Test
  public void testColumnarHandoffMatchesPerDocPath()
      throws Exception {
    // Pin the classification itself, not just that the two paths agree: with the row shapes in
    // buildRows() ("always" ~100% fill, "mostly" ~70% fill, "rare" ~5% fill against the default 50%
    // dense threshold) both the dense and sparse branches must actually be exercised, or the
    // comparison would still pass with the sparse tier silently untested.
    assertPathsAgree("inferred", openStructSpec(), OpenStructIndexConfig.DEFAULT, buildRows(),
        Set.of("always", "mostly"));
  }

  /// Every key in [#buildRows] is undeclared, so `hasDeclaredType` is false throughout and the
  /// declared-child-spec branches of both ingestion paths go unexercised. This case declares child
  /// specs — including a FLOAT key fed `Long` values and a BOOLEAN key, whose stored types differ
  /// from both the declared type and the raw value's type — alongside undeclared keys and an
  /// ignored key, so the columnar hand-off's "skip seeding _inferredTypes" branch is compared
  /// against the per-document path under the shapes that make it observable.
  @Test
  public void testColumnarHandoffMatchesPerDocPathWithDeclaredChildSpecs()
      throws Exception {
    // "declaredFloat" 100%, "declaredTimestamp" 100%, "declaredBool" 80%, "undeclaredString" 60%
    // are dense against the default 50% threshold; the two ~1%-fill keys (one declared, one not)
    // land in the sparse tier, so declared types are exercised on both sides of the split.
    Map<String, PropertiesConfiguration> columnarMeta = assertPathsAgree("declared", declaredChildSpecs(),
        configWithIgnoredKey(), buildDeclaredRows(),
        Set.of("declaredFloat", "declaredTimestamp", "declaredBool", "undeclaredString"));

    // assertPathsAgree only pins that the two paths *agree*, which a shared wrong answer would
    // still satisfy. "declaredFloat" is fed Long values but declared FLOAT, so this closes the gap
    // with an absolute check that the declared type -- not the raw value's type -- is what actually
    // got materialized.
    String declaredFloatCol = OpenStructNaming.materializedColumnName("metrics", "declaredFloat");
    PropertiesConfiguration declaredFloatMeta = columnarMeta.get(declaredFloatCol);
    assertEquals(declaredFloatMeta.getString(V1Constants.MetadataKeys.Column.getKeyFor(
        declaredFloatCol, V1Constants.MetadataKeys.Column.DATA_TYPE)), "FLOAT",
        "declaredFloat should be materialized with its declared type, not the raw Long value's type");
  }

  /// Feeds the identical rows through the per-document path and the columnar hand-off into two
  /// separate directories, then asserts they classified keys the same way, wrote the same column
  /// metadata, and produced byte-identical files. Returns the columnar path's materialized column
  /// metadata so callers can pin absolute properties beyond the two paths' agreement.
  private Map<String, PropertiesConfiguration> assertPathsAgree(String caseName, ComplexFieldSpec fieldSpec,
      OpenStructIndexConfig config, Map<Integer, Map<String, Object>> rows, Set<String> expectedDenseKeys)
      throws Exception {
    File perDocDir = new File(_tempDir, caseName + "-perdoc");
    assertTrue(perDocDir.mkdirs());
    OpenStructColumnSplitter perDoc = newSplitter(perDocDir, fieldSpec, config);
    for (int docId = 0; docId < NUM_DOCS; docId++) {
      perDoc.add(rows.get(docId), docId);
    }
    perDoc.seal();

    File columnarDir = new File(_tempDir, caseName + "-columnar");
    assertTrue(columnarDir.mkdirs());
    OpenStructColumnSplitter columnar = newSplitter(columnarDir, fieldSpec, config);
    try (MutableOpenStructIndex index = new MutableOpenStructIndex(
        "metrics", "testTable_REALTIME", fieldSpec, config, _memMgr, NUM_DOCS)) {
      for (int docId = 0; docId < NUM_DOCS; docId++) {
        index.index(docId, rows.get(docId));
      }
      assertTrue(columnar.supportsColumnarAdd());
      OpenStructColumnarSource source = index.asColumnarSource(NUM_DOCS);
      columnar.addColumnar(source);
    }
    columnar.seal();

    assertEquals(perDoc.getResolvedDenseKeys(), expectedDenseKeys,
        "Per-doc path classified an unexpected dense-key set; the sparse tier would go untested");
    assertEquals(columnar.getResolvedDenseKeys(), expectedDenseKeys,
        "Dense/sparse classification diverged between the two paths");

    Map<String, PropertiesConfiguration> columnarMeta = columnar.getMaterializedColumnMetadata();
    Map<String, PropertiesConfiguration> perDocMeta = perDoc.getMaterializedColumnMetadata();
    assertEquals(columnarMeta.keySet(), perDocMeta.keySet(),
        "The two paths produced different materialized columns");
    // Pin the written column set against the expected classification rather than only against the
    // other path: it proves the sparse tier was actually written, and that keys neither path may
    // register (ignored keys, declared-but-never-ingested keys) produced no column in either.
    Set<String> expectedColumns = new HashSet<>();
    for (String denseKey : expectedDenseKeys) {
      expectedColumns.add(OpenStructNaming.materializedColumnName("metrics", denseKey));
    }
    expectedColumns.add(OpenStructNaming.sparseColumnName("metrics"));
    expectedColumns.add("metrics");
    assertEquals(perDocMeta.keySet(), expectedColumns, "Unexpected set of materialized columns");
    for (String column : perDocMeta.keySet()) {
      PropertiesConfiguration expected = perDocMeta.get(column);
      PropertiesConfiguration actual = columnarMeta.get(column);
      // Symmetric: a property present only in one path's output must fail the comparison too, not
      // just a divergent value for a property both paths happen to share.
      expected.getKeys().forEachRemaining(key ->
          assertEquals(String.valueOf(actual.getProperty(key)), String.valueOf(expected.getProperty(key)),
              "Metadata diverged for " + column + " key " + key));
      actual.getKeys().forEachRemaining(key ->
          assertEquals(String.valueOf(actual.getProperty(key)), String.valueOf(expected.getProperty(key)),
              "Metadata diverged for " + column + " key " + key + " (present only in the columnar path)"));
    }

    // Metadata alone (totalDocs, cardinality, min, max) is invariant under a *permutation* of
    // values across docIds -- exactly the failure mode a presence-bitmap-to-value-list ordinal
    // pairing bug is exposed to. Diffing the written segment files byte-for-byte is the only way to
    // pin the actual claim: that the two paths produce byte-identical segment output.
    assertDirectoriesEqual(perDocDir, columnarDir);
    return columnarMeta;
  }

  /// Asserts `expectedDir` and `actualDir` contain the same set of (relative) file names and that
  /// each corresponding pair has byte-identical content.
  private static void assertDirectoriesEqual(File expectedDir, File actualDir)
      throws IOException {
    Set<String> expectedFiles = relativeFileNames(expectedDir);
    Set<String> actualFiles = relativeFileNames(actualDir);
    // Otherwise the comparison below would pass vacuously if both directories were empty.
    assertFalse(expectedFiles.isEmpty(), "Expected directory produced no files to compare");
    assertEquals(actualFiles, expectedFiles, "The columnar and per-doc paths wrote a different set of files");
    for (String relativePath : expectedFiles) {
      byte[] expectedBytes = Files.readAllBytes(new File(expectedDir, relativePath).toPath());
      byte[] actualBytes = Files.readAllBytes(new File(actualDir, relativePath).toPath());
      assertEquals(actualBytes, expectedBytes,
          "File content diverged between the columnar and per-doc paths for " + relativePath);
    }
  }

  private static Set<String> relativeFileNames(File dir)
      throws IOException {
    Path root = dir.toPath();
    try (Stream<Path> paths = Files.walk(root)) {
      return paths.filter(Files::isRegularFile)
          .map(path -> root.relativize(path).toString())
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  @Test
  public void testColumnarSourceSkipsDocsBeyondSnapshot()
      throws Exception {
    try (MutableOpenStructIndex index = new MutableOpenStructIndex(
        "metrics", "testTable_REALTIME", openStructSpec(), OpenStructIndexConfig.DEFAULT, _memMgr, 100)) {
      for (int docId = 0; docId < 10; docId++) {
        index.index(docId, Map.of("k", (long) docId));
      }
      // Snapshot at 5 docs, then keep writing as the consuming thread would.
      OpenStructColumnarSource source = index.asColumnarSource(5);
      for (int docId = 10; docId < 20; docId++) {
        index.index(docId, Map.of("k", (long) docId));
      }
      int[] visited = new int[]{0};
      source.forEachPresentValue("k", (docId, value) -> {
        assertTrue(docId < 5, "Snapshot leaked docId " + docId);
        visited[0]++;
      });
      assertEquals(visited[0], 5);
      assertEquals(source.getNumDocs(), 5);
    }
  }
}
