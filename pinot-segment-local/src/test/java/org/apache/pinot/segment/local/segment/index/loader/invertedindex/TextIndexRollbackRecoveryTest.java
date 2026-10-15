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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCombined;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.LoaderUtils;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.local.utils.LuceneIndexCompatibilityUtils;
import org.apache.pinot.segment.local.utils.LuceneIndexTestUtils;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextIndexConstants;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.segment.spi.utils.SegmentMetadataUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises server loading with real Lucene 10 indexes and retained Pinot source values on Lucene 9.12.
public class TextIndexRollbackRecoveryTest implements PinotBuffersAfterClassCheckRule {
  private File _tempDir;
  private File _fixtures;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("text-rollback").toFile();
    _fixtures = LuceneIndexTestUtils.extractLucene10Fixtures(new File(_tempDir, "fixtures"));
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    FileUtils.deleteDirectory(_tempDir);
  }

  @DataProvider
  public Object[][] textFormats() {
    return new Object[][]{
        {SegmentVersion.v1, false, false, false},
        {SegmentVersion.v1, true, false, false},
        {SegmentVersion.v3, false, false, true},
        {SegmentVersion.v3, true, false, false},
        {SegmentVersion.v3, false, true, false},
        {SegmentVersion.v3, true, true, true}
    };
  }

  @Test(dataProvider = "textFormats")
  public void testSingleColumnRollback(SegmentVersion version, boolean combined, boolean multiValue, boolean raw)
      throws Exception {
    Schema schema = schema(multiValue);
    TableConfig tableConfig = tableConfig(false, combined, raw);
    File segmentDir = buildSegment(tableConfig, schema, version, multiValue);
    installLucene10(segmentDir, version, false, combined);

    IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, schema);
    try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap)) {
      assertTrue(ImmutableSegmentLoader.needPreprocess(directory, loadingConfig));
    }
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, loadingConfig, true);
    try {
      assertEquals(segment.getDataSource("text").getTextIndex().getDocIds("hello", null).toArray(), new int[]{0});
      assertEquals(segment.getDataSource("text").getTextIndex().getDocIds("other", null).toArray(), new int[]{1});
      // For MV fields, original values contain duplicate terms and a phrase inside one value.
      assertEquals(segment.getDataSource("text").getTextIndex().getDocIds("\"hello world\"", null).toArray(),
          new int[]{0});
      if (multiValue) {
        assertEquals(
            segment.getDataSource("text").getTextIndex().getDocIds("\"hello world hello world\"", null).toArray(),
            new int[]{0});
        assertEquals(segment.getDataSource("text").getTextIndex().getDocIds("\"value other\"", null).toArray(),
            new int[]{1});
      }
    } finally {
      segment.destroy();
    }
    assertStableReload(segmentDir, loadingConfig);
  }

  @DataProvider
  public Object[][] multiColumnVersions() {
    return new Object[][]{{SegmentVersion.v1}, {SegmentVersion.v3}};
  }

  @Test(dataProvider = "multiColumnVersions")
  public void testMultiColumnRollback(SegmentVersion version)
      throws Exception {
    Schema schema = schema(false);
    TableConfig tableConfig = tableConfig(true, false, false);
    File segmentDir = buildSegment(tableConfig, schema, version, false);
    installLucene10(segmentDir, version, true, false);
    IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, schema);
    try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap)) {
      assertTrue(ImmutableSegmentLoader.needPreprocess(directory, loadingConfig));
    }
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, loadingConfig, true);
    try {
      MultiColumnLuceneTextIndexReader reader = (MultiColumnLuceneTextIndexReader) segment.getMultiColumnTextIndex();
      assertEquals(reader.getDocIds("text", "hello", (String) null).toArray(), new int[]{0});
      assertEquals(reader.getDocIds("other", "beta", (String) null).toArray(), new int[]{1});
    } finally {
      segment.destroy();
    }
    assertStableReload(segmentDir, loadingConfig);
  }

  @Test(dataProvider = "multiColumnVersions")
  public void testMultiColumnRollbackWithNewDefaultColumn(SegmentVersion version)
      throws Exception {
    Schema schema = schema(false);
    TableConfig tableConfig = tableConfig(true, false, false);
    File segmentDir = buildSegment(tableConfig, schema, version, false);
    installLucene10(segmentDir, version, true, false);

    Schema updatedSchema = new Schema.SchemaBuilder().setSchemaName("rollback")
        .addSingleValueDimension("text", DataType.STRING).addSingleValueDimension("other", DataType.STRING)
        .addSingleValueDimension("added", DataType.STRING, "defaultvalue").build();
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(
        new MultiColumnTextIndexConfig(List.of("text", "other", "added"), Map.of(), Map.of()));
    IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, updatedSchema);
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, loadingConfig, true);
    try {
      MultiColumnLuceneTextIndexReader reader = (MultiColumnLuceneTextIndexReader) segment.getMultiColumnTextIndex();
      assertEquals(reader.getDocIds("text", "hello", (String) null).toArray(), new int[]{0});
      assertEquals(reader.getDocIds("other", "beta", (String) null).toArray(), new int[]{1});
      assertEquals(reader.getDocIds("added", "defaultvalue", (String) null).toArray(), new int[]{0, 1});
    } finally {
      segment.destroy();
    }
    assertStableReload(segmentDir, loadingConfig);
  }

  @Test(dataProvider = "multiColumnVersions")
  public void testInterruptedMultiColumnRemoval(SegmentVersion version)
      throws Exception {
    Schema schema = schema(false);
    TableConfig tableConfig = tableConfig(true, false, false);
    File segmentDir = buildSegment(tableConfig, schema, version, false);
    File indexDir = SegmentDirectoryPaths.segmentDirectoryFor(segmentDir, version);
    File luceneDir = SegmentDirectoryPaths.findTextIndexIndexFile(segmentDir,
        MultiColumnTextIndexConstants.INDEX_DIR_NAME);
    // A failed removal has cleared metadata, but the marker and index files remain.
    PropertiesConfiguration properties = SegmentMetadataUtils.getPropertiesConfiguration(segmentDir);
    properties.subset(MultiColumnTextIndexConstants.MetadataKey.ROOT_SUBSET).clear();
    SegmentMetadataUtils.savePropertiesConfiguration(properties, segmentDir);
    File marker = new File(segmentDir, MultiColumnTextIndexConstants.INDEX_DIR_NAME + ".text.inprogress");
    FileUtils.touch(marker);
    tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(null);

    IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, schema);
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, loadingConfig, true);
    try {
      assertNull(segment.getMultiColumnTextIndex());
      assertFalse(luceneDir.exists());
      assertFalse(new File(indexDir, MultiColumnTextIndexConstants.DOCID_MAPPING_FILE_NAME).exists());
      assertFalse(marker.exists());
    } finally {
      segment.destroy();
    }
    assertStableReload(segmentDir, loadingConfig);
  }

  @Test(dataProvider = "multiColumnVersions")
  public void testMissingOriginalForwardIndexPreservesTextIndex(SegmentVersion version)
      throws Exception {
    // Dictionary/inverted reconstruction cannot retain MV order or duplicates. Fail before that handler runs.
    for (boolean multiColumn : List.of(false, true)) {
      Schema schema = schema(true);
      TableConfig tableConfig = tableConfig(multiColumn, false, false);
      tableConfig.getIndexingConfig().setInvertedIndexColumns(List.of("text"));
      File segmentDir = buildSegment(tableConfig, schema, version, true);
      installLucene10(segmentDir, version, multiColumn, false);
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentDirectory.Writer writer = directory.createWriter()) {
        writer.removeIndex("text", StandardIndexes.forward());
        writer.save();
      }
      File luceneDir = SegmentDirectoryPaths.findTextIndexIndexFile(segmentDir,
          multiColumn ? MultiColumnTextIndexConstants.INDEX_DIR_NAME : "text");
      File commitFile = new File(luceneDir, "segments_1");
      byte[] original = Files.readAllBytes(commitFile.toPath());
      assertThrows(IllegalStateException.class,
          () -> ImmutableSegmentLoader.load(segmentDir, new IndexLoadingConfig(tableConfig, schema), true));
      assertEquals(Files.readAllBytes(commitFile.toPath()), original);
      assertTrue(LuceneIndexCompatibilityUtils.isIncompatible(luceneDir));
      FileUtils.deleteDirectory(segmentDir);
    }
  }

  @DataProvider
  public Object[][] textKinds() {
    return new Object[][]{{false}, {true}};
  }

  @Test(dataProvider = "textKinds")
  public void testInterruptedReadableIndexRestartsFromOriginalValues(boolean multiColumn)
      throws Exception {
    Schema schema = schema(false);
    TableConfig tableConfig = tableConfig(multiColumn, false, false);
    File segmentDir = buildSegment(tableConfig, schema, SegmentVersion.v3, false);
    File partial = installInterruptedBuild(segmentDir, multiColumn);
    assertFalse(LuceneIndexCompatibilityUtils.isIncompatible(partial));
    IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, schema);
    try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap)) {
      assertTrue(ImmutableSegmentLoader.needPreprocess(directory, loadingConfig));
    }
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, loadingConfig, true);
    try {
      if (multiColumn) {
        MultiColumnLuceneTextIndexReader reader = (MultiColumnLuceneTextIndexReader) segment.getMultiColumnTextIndex();
        assertEquals(reader.getDocIds("text", "hello", (String) null).toArray(), new int[]{0});
        assertEquals(reader.getDocIds("text", "other", (String) null).toArray(), new int[]{1});
        assertTrue(reader.getDocIds("text", "partial", (String) null).isEmpty());
      } else {
        assertEquals(segment.getDataSource("text").getTextIndex().getDocIds("hello", null).toArray(), new int[]{0});
        assertEquals(segment.getDataSource("text").getTextIndex().getDocIds("other", null).toArray(), new int[]{1});
        assertTrue(segment.getDataSource("text").getTextIndex().getDocIds("partial", null).isEmpty());
      }
    } finally {
      segment.destroy();
    }
    try (FSDirectory directory = FSDirectory.open(partial.toPath());
        DirectoryReader reader = DirectoryReader.open(directory)) {
      assertEquals(reader.numDocs(), 2);
    }
    assertStableReload(segmentDir, loadingConfig);
  }

  @Test(dataProvider = "textKinds")
  public void testInterruptedBuildRequiresOriginalForwardIndex(boolean multiColumn)
      throws Exception {
    Schema schema = schema(true);
    TableConfig tableConfig = tableConfig(multiColumn, false, false);
    tableConfig.getIndexingConfig().setInvertedIndexColumns(List.of("text"));
    File segmentDir = buildSegment(tableConfig, schema, SegmentVersion.v3, true);
    File partial = installInterruptedBuild(segmentDir, multiColumn);
    removeForwardIndex(segmentDir);
    assertThrows(IllegalStateException.class,
        () -> ImmutableSegmentLoader.load(segmentDir, new IndexLoadingConfig(tableConfig, schema), true));
    try (FSDirectory directory = FSDirectory.open(partial.toPath());
        DirectoryReader reader = DirectoryReader.open(directory)) {
      assertEquals(reader.numDocs(), 1);
    }
    try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = directory.createReader()) {
      assertFalse(reader.hasIndexFor("text", StandardIndexes.forward()));
    }
  }

  @Test
  public void testCompatibleDirectoryDoesNotHideIncompatiblePackedIndex()
      throws Exception {
    Schema schema = schema(true);
    TableConfig tableConfig = tableConfig(false, true, false);
    tableConfig.getIndexingConfig().setInvertedIndexColumns(List.of("text"));
    File segmentDir = buildSegment(tableConfig, schema, SegmentVersion.v3, true);
    installLucene10(segmentDir, SegmentVersion.v3, false, true);
    File directory = new File(SegmentDirectoryPaths.segmentDirectoryFor(segmentDir, SegmentVersion.v3),
        "text" + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    writePartialIndex(directory);
    assertFalse(LuceneIndexCompatibilityUtils.isIncompatible(directory));
    removeForwardIndex(segmentDir);
    assertThrows(IllegalStateException.class,
        () -> ImmutableSegmentLoader.load(segmentDir, new IndexLoadingConfig(tableConfig, schema), true));
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentDirectory.Reader reader = segmentDirectory.createReader()) {
      assertTrue(LuceneIndexCompatibilityUtils.isIncompatible(
          reader.getIndexFor("text", StandardIndexes.text()), "text"));
      assertFalse(reader.hasIndexFor("text", StandardIndexes.forward()));
    }
  }

  private File installInterruptedBuild(File segmentDir, boolean multiColumn)
      throws Exception {
    String column = multiColumn ? MultiColumnTextIndexConstants.INDEX_DIR_NAME : "text";
    File partial = new File(SegmentDirectoryPaths.segmentDirectoryFor(segmentDir, SegmentVersion.v3),
        column + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    writePartialIndex(partial);
    if (multiColumn) {
      // Reproduce the old loader's crash window: metadata was removed and the partial directory remains.
      PropertiesConfiguration properties = SegmentMetadataUtils.getPropertiesConfiguration(segmentDir);
      properties.subset(MultiColumnTextIndexConstants.MetadataKey.ROOT_SUBSET).clear();
      SegmentMetadataUtils.savePropertiesConfiguration(properties, segmentDir);
    } else {
      FileUtils.touch(new File(segmentDir, "text.text.inprogress"));
    }
    return partial;
  }

  private void writePartialIndex(File directory)
      throws Exception {
    try (FSDirectory lucene = FSDirectory.open(directory.toPath());
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriter writer = new IndexWriter(lucene,
            new IndexWriterConfig(analyzer).setOpenMode(IndexWriterConfig.OpenMode.CREATE))) {
      Document document = new Document();
      document.add(new TextField("text", "partial", Field.Store.NO));
      document.add(new StoredField(LuceneTextIndexCreator.LUCENE_INDEX_DOC_ID_COLUMN_NAME, 0));
      writer.addDocument(document);
    }
  }

  private void removeForwardIndex(File segmentDir)
      throws Exception {
    try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
        SegmentDirectory.Writer writer = directory.createWriter()) {
      writer.removeIndex("text", StandardIndexes.forward());
      writer.save();
    }
  }

  private void assertStableReload(File segmentDir, IndexLoadingConfig loadingConfig)
      throws Exception {
    try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap)) {
      assertFalse(ImmutableSegmentLoader.needPreprocess(directory, loadingConfig));
    }
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, loadingConfig, true);
    segment.destroy();
  }

  private Schema schema(boolean multiValue) {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder().setSchemaName("rollback");
    if (multiValue) {
      builder.addMultiValueDimension("text", DataType.STRING);
    } else {
      builder.addSingleValueDimension("text", DataType.STRING);
    }
    return builder.addSingleValueDimension("other", DataType.STRING).build();
  }

  private TableConfig tableConfig(boolean multiColumn, boolean combined, boolean raw) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("rollback").build();
    if (raw) {
      tableConfig.getIndexingConfig().setNoDictionaryColumns(List.of("text"));
    }
    if (multiColumn) {
      tableConfig.getIndexingConfig().setMultiColumnTextIndexConfig(
          new MultiColumnTextIndexConfig(List.of("text", "other"), Map.of(), Map.of()));
    } else {
      tableConfig.setFieldConfigList(List.of(new FieldConfig("text",
          raw ? FieldConfig.EncodingType.RAW : FieldConfig.EncodingType.DICTIONARY,
          List.of(FieldConfig.IndexType.TEXT), null, Map.of("storeInSegmentFile", Boolean.toString(combined)))));
    }
    return tableConfig;
  }

  private File buildSegment(TableConfig tableConfig, Schema schema, SegmentVersion version, boolean multiValue)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(_tempDir.getAbsolutePath());
    config.setSegmentName("segment");
    config.setSegmentVersion(version);
    List<GenericRow> rows = new ArrayList<>();
    GenericRow first = new GenericRow();
    first.putValue("text", multiValue ? new String[]{"hello world", "hello world"} : "hello world");
    first.putValue("other", "alpha");
    rows.add(first);
    GenericRow second = new GenericRow();
    second.putValue("text", multiValue ? new String[]{"other value", "other"} : "other value");
    second.putValue("other", "beta");
    rows.add(second);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    return new File(_tempDir, "segment");
  }

  private void installLucene10(File segmentDir, SegmentVersion version, boolean multiColumn, boolean combined)
      throws Exception {
    if (Version.LATEST.major != 9) {
      throw new SkipException("Lucene 10 fixture exercises rollback recovery on Lucene 9 only");
    }
    String column = multiColumn ? MultiColumnTextIndexConstants.INDEX_DIR_NAME : "text";
    File actualDirectory = SegmentDirectoryPaths.segmentDirectoryFor(segmentDir, version);
    File luceneDir = new File(actualDirectory, column + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    if (!multiColumn) {
      try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
          SegmentDirectory.Writer writer = directory.createWriter()) {
        writer.removeIndex(column, StandardIndexes.text());
        writer.save();
      }
    }
    FileUtils.deleteQuietly(luceneDir);
    FileUtils.copyDirectory(new File(_fixtures, multiColumn ? "multi" : "text"), luceneDir);
    try (FSDirectory directory = FSDirectory.open(luceneDir.toPath())) {
      assertThrows(IllegalArgumentException.class, () -> DirectoryReader.open(directory));
    }
    if (combined) {
      File combinedFile =
          new File(actualDirectory, column + V1Constants.Indexes.LUCENE_COMBINE_TEXT_INDEX_FILE_EXTENSION);
      LuceneTextIndexCombined.combineLuceneIndexFiles(luceneDir, combinedFile.getAbsolutePath());
      FileUtils.deleteDirectory(luceneDir);
      if (version == SegmentVersion.v3) {
        try (SegmentDirectory directory = new SegmentLocalFSDirectory(segmentDir, ReadMode.mmap);
            SegmentDirectory.Writer writer = directory.createWriter()) {
          LoaderUtils.writeIndexToV3Format(writer, column, combinedFile, StandardIndexes.text());
          writer.save();
        }
      }
    }
  }
}
