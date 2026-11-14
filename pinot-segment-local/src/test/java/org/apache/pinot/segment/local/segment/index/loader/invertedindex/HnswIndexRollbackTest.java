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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.Version;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.creator.impl.vector.HnswVectorIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.vector.lucene99.HnswVectorIndexCombined;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.local.utils.LuceneIndexCompatibilityUtils;
import org.apache.pinot.segment.local.utils.LuceneIndexTestUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.VectorBackendType;
import org.apache.pinot.segment.spi.index.creator.VectorIndexConfig;
import org.apache.pinot.segment.spi.index.reader.VectorIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises server-load rollback from real Lucene 10 HNSW files to the running Lucene writer.
public class HnswIndexRollbackTest implements PinotBuffersAfterClassCheckRule {
  private static final String COLUMN = "embedding";
  private static final int DIMENSIONS = 2048;
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addMultiValueDimension(COLUMN, DataType.FLOAT).build();

  @DataProvider(name = "layouts")
  public Object[][] layouts() {
    return new Object[][]{
        {SegmentVersion.v1, "directory", false},
        {SegmentVersion.v3, "directory", false},
        {SegmentVersion.v3, "combined", true},
        {SegmentVersion.v3, "consolidated", true},
        {SegmentVersion.v3, "consolidated", false}
    };
  }

  @Test(dataProvider = "layouts")
  public void testLoadingRebuildsNewerHnswIndex(SegmentVersion version, String layout, boolean storeInSegmentFile)
      throws Exception {
    File tempDir = Files.createTempDirectory("hnsw-rollback").toFile();
    try {
      TableConfig tableConfig = tableConfig(storeInSegmentFile, false);
      File segmentDir = buildSegment(tempDir, tableConfig, version);
      installNewerIndex(tempDir, segmentDir, version, layout);
      IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);

      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentPreProcessor processor = new SegmentPreProcessor(directory, loadingConfig)) {
        assertTrue(processor.needProcess(), "the newer HNSW format must trigger preprocessing");
      }
      assertLoadsAndQueries(segmentDir, loadingConfig);
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentPreProcessor processor = new SegmentPreProcessor(directory, loadingConfig);
          SegmentDirectory.Reader reader = directory.createReader()) {
        assertFalse(processor.needProcess(), "a rebuilt index must be retained on the next load");
        if (storeInSegmentFile) {
          assertFalse(LuceneIndexCompatibilityUtils.isIncompatible(
              reader.getIndexFor(COLUMN, StandardIndexes.vector()), COLUMN));
        } else {
          File hnswDir = SegmentDirectoryPaths.findVectorIndexIndexFile(segmentDir, COLUMN, VectorBackendType.HNSW);
          assertNotNull(hnswDir);
          assertFalse(LuceneIndexCompatibilityUtils.isIncompatible(hnswDir));
        }
      }
      assertLoadsAndQueries(segmentDir, loadingConfig);
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  @DataProvider(name = "interruptedLayouts")
  public Object[][] interruptedLayouts() {
    return new Object[][]{{"directory"}, {"combined"}, {"consolidated"}};
  }

  @Test(dataProvider = "interruptedLayouts")
  public void testLoadingRestartsReadablePartialHnswIndex(String layout)
      throws Exception {
    File tempDir = Files.createTempDirectory("hnsw-rollback-interrupted").toFile();
    try {
      boolean storeInSegmentFile = !layout.equals("directory");
      TableConfig tableConfig = tableConfig(storeInSegmentFile, false);
      File segmentDir = buildSegment(tempDir, tableConfig, SegmentVersion.v3);
      File storageDir = SegmentDirectoryPaths.segmentDirectoryFor(segmentDir, SegmentVersion.v3);
      IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentDirectory.Writer writer = directory.createWriter()) {
        writer.removeIndex(COLUMN, StandardIndexes.vector());
        writer.save();
      }
      VectorIndexConfig vectorConfig =
          loadingConfig.getFieldIndexConfigByColName().get(COLUMN).getConfig(StandardIndexes.vector());
      // A crash can leave a valid commit containing only the first input document. Compatibility alone cannot
      // distinguish this from a completed rebuild; the creator's marker must force a restart from all source rows.
      try (HnswVectorIndexCreator creator = new HnswVectorIndexCreator(COLUMN, storageDir, vectorConfig)) {
        float[] firstVector = new float[DIMENSIONS];
        firstVector[0] = 1;
        creator.add(firstVector);
        creator.seal();
      }
      File combined = new File(storageDir, COLUMN + V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
      if (layout.equals("consolidated")) {
        try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
            SegmentDirectory.Writer writer = directory.createWriter()) {
          LoaderUtils.writeIndexToV3Format(writer, COLUMN, combined, StandardIndexes.vector());
          writer.save();
        }
      }
      String extension = storeInSegmentFile
          ? V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION
          : V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION;
      File marker = new File(storageDir, COLUMN + extension + ".inprogress");
      FileUtils.touch(marker);
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentPreProcessor processor = new SegmentPreProcessor(directory, loadingConfig)) {
        assertTrue(processor.needProcess(), "the owned marker must force recovery of a readable partial commit");
      }
      assertLoadsAndQueries(segmentDir, loadingConfig);
      assertFalse(marker.exists(), "a successful rebuild must clear its marker");
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentPreProcessor processor = new SegmentPreProcessor(directory, loadingConfig)) {
        assertFalse(processor.needProcess(), "a completed retry must be retained");
      }
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  @Test
  public void testMissingOriginalForwardIndexPreservesNewerHnswIndex()
      throws Exception {
    File tempDir = Files.createTempDirectory("hnsw-rollback-no-source").toFile();
    try {
      // Dictionary + inverted indexes cannot recover either repeated coordinates or their original order.
      TableConfig tableConfig = tableConfig(false, true);
      File segmentDir = buildSegment(tempDir, tableConfig, SegmentVersion.v3);
      installNewerIndex(tempDir, segmentDir, SegmentVersion.v3, "directory");
      File hnswDir = SegmentDirectoryPaths.findVectorIndexIndexFile(segmentDir, COLUMN, VectorBackendType.HNSW);
      assertNotNull(hnswDir);
      File commit = new File(hnswDir, "segments_1");
      byte[] originalCommit = Files.readAllBytes(commit.toPath());
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentDirectory.Writer writer = directory.createWriter()) {
        writer.removeIndex(COLUMN, StandardIndexes.forward());
        writer.save();
        assertTrue(writer.hasIndexFor(COLUMN, StandardIndexes.dictionary()));
        assertTrue(writer.hasIndexFor(COLUMN, StandardIndexes.inverted()));
      }

      // The unchanged table config still enables forward, so this also tests that recovery is checked before
      // ForwardIndexHandler can regenerate an unordered/deduplicated replacement from the inverted index.
      IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentPreProcessor processor = new SegmentPreProcessor(directory, loadingConfig)) {
        IOException error = expectThrows(IOException.class, processor::process);
        assertTrue(error.getMessage().contains("original forward index"));
        try (SegmentDirectory.Reader reader = directory.createReader()) {
          assertFalse(reader.hasIndexFor(COLUMN, StandardIndexes.forward()));
        }
      }
      assertEquals(Files.readAllBytes(commit.toPath()), originalCommit);
      assertTrue(LuceneIndexCompatibilityUtils.isIncompatible(hnswDir));

      // A crash before the creator opens its directory can leave only the marker. This must still be checked
      // before any handler regenerates a forward index from the unordered inverted-index values.
      FileUtils.deleteDirectory(hnswDir);
      File marker = new File(hnswDir.getParentFile(), hnswDir.getName() + ".inprogress");
      FileUtils.touch(marker);
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentPreProcessor processor = new SegmentPreProcessor(directory, loadingConfig)) {
        IOException error = expectThrows(IOException.class, processor::process);
        assertTrue(error.getMessage().contains("original forward index"));
        assertTrue(marker.exists());
      }
    } finally {
      FileUtils.deleteDirectory(tempDir);
    }
  }

  private static TableConfig tableConfig(boolean storeInSegmentFile, boolean dictionary) {
    VectorIndexConfig vectorConfig = new VectorIndexConfig(false, "HNSW", DIMENSIONS, 1,
        VectorIndexConfig.VectorDistanceFunction.EUCLIDEAN,
        Map.of(VectorIndexConfig.STORE_IN_SEGMENT_FILE, Boolean.toString(storeInSegmentFile)));
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("hnswRollback").build();
    tableConfig.setFieldConfigList(List.of(new FieldConfig.Builder(COLUMN)
        .withEncodingType(dictionary ? FieldConfig.EncodingType.DICTIONARY : FieldConfig.EncodingType.RAW)
        .withIndexes(JsonUtils.objectToJsonNode(Map.of("vector", vectorConfig))).build()));
    if (dictionary) {
      tableConfig.getIndexingConfig().setInvertedIndexColumns(List.of(COLUMN));
    }
    return tableConfig;
  }

  private static File buildSegment(File tempDir, TableConfig tableConfig, SegmentVersion version)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(tempDir.getAbsolutePath());
    config.setSegmentName("segment");
    config.setSegmentVersion(version);
    List<GenericRow> rows = new ArrayList<>();
    for (int docId = 0; docId < 3; docId++) {
      Float[] vector = new Float[DIMENSIONS];
      Arrays.fill(vector, 0F);
      vector[docId] = 1F;
      GenericRow row = new GenericRow();
      row.putValue(COLUMN, vector);
      rows.add(row);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(tempDir, "segment");
  }

  private static void installNewerIndex(File tempDir, File segmentDir, SegmentVersion version, String layout)
      throws Exception {
    if (Version.LATEST.major != 9) {
      throw new SkipException("Lucene 10 fixture exercises rollback recovery on Lucene 9 only");
    }
    File fixtures = LuceneIndexTestUtils.extractLucene10Fixtures(new File(tempDir, "lucene10-hnsw"));
    File fixture = new File(fixtures, "hnsw");
    assertTrue(LuceneIndexCompatibilityUtils.isIncompatible(fixture));
    File storageDir = SegmentDirectoryPaths.segmentDirectoryFor(segmentDir, version);
    try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = directory.createWriter()) {
      writer.removeIndex(COLUMN, StandardIndexes.vector());
      if (layout.equals("directory")) {
        FileUtils.copyDirectory(fixture,
            new File(storageDir, COLUMN + V1Constants.Indexes.VECTOR_V912_HNSW_INDEX_FILE_EXTENSION));
      } else {
        File combined = new File(storageDir, COLUMN + V1Constants.Indexes.VECTOR_HNSW_COMBINED_INDEX_FILE_EXTENSION);
        HnswVectorIndexCombined.combineHnswIndexFiles(fixture, combined.getAbsolutePath(), null, null);
        if (layout.equals("consolidated")) {
          LoaderUtils.writeIndexToV3Format(writer, COLUMN, combined, StandardIndexes.vector());
        }
      }
      writer.save();
    }
  }

  private static void assertLoadsAndQueries(File segmentDir, IndexLoadingConfig loadingConfig)
      throws Exception {
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, loadingConfig, true);
    try {
      VectorIndexReader reader = segment.getDataSource(COLUMN).getVectorIndex();
      assertNotNull(reader);
      for (int docId = 0; docId < 3; docId++) {
        float[] query = new float[DIMENSIONS];
        query[docId] = 1;
        assertEquals(reader.getDocIds(query, 1).toArray(), new int[]{docId});
      }
    } finally {
      segment.destroy();
    }
  }
}
