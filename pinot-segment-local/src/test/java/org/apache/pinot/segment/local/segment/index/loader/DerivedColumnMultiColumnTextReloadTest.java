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
package org.apache.pinot.segment.local.segment.index.loader;

import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// A changed derived-column transform must invalidate a multi-column text index even when the index configuration is
/// unchanged, otherwise text queries can continue serving tokens from the old materialized values.
public class DerivedColumnMultiColumnTextReloadTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), DerivedColumnMultiColumnTextReloadTest.class.getSimpleName());
  private static final String TABLE = "testTable";
  private static final String SEGMENT = "testSegment";
  private static final String SOURCE = "sourceText";
  private static final String DERIVED = "derivedText";
  private static final String ORIGINAL_TRANSFORM = "concat(sourceText, ' oldsentinel')";
  private static final String UPDATED_TRANSFORM = "concat(sourceText, ' newsentinel')";

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testTextIndexRebuiltWhenDerivedTransformChanges()
      throws Exception {
    File indexDir = buildBaseSegment();
    Schema schema = schemaWithDerived();
    MultiColumnTextIndexConfig textIndexConfig = new MultiColumnTextIndexConfig(List.of(SOURCE, DERIVED));

    preprocess(indexDir, tableConfig(ORIGINAL_TRANSFORM, textIndexConfig), schema);
    assertTextMatches(indexDir, "oldsentinel", MutableRoaringBitmap.bitmapOf(0, 1));
    assertTextMatches(indexDir, "newsentinel", new MutableRoaringBitmap());

    preprocess(indexDir, tableConfig(UPDATED_TRANSFORM, textIndexConfig), schema);
    assertTextMatches(indexDir, "oldsentinel", new MutableRoaringBitmap());
    assertTextMatches(indexDir, "newsentinel", MutableRoaringBitmap.bitmapOf(0, 1));

    SegmentMetadataImpl metadata = new SegmentMetadataImpl(indexDir);
    assertEquals(metadata.getMultiColumnTextMetadata().getColumns(), List.of(SOURCE, DERIVED));
    assertEquals(metadata.getColumnMetadataFor(DERIVED).getTransformFunction(), UPDATED_TRANSFORM);
  }

  /// A failure between the column rewrite and the text index rebuild, like a crash there, must not leave the index over
  /// the old values: the index is deleted before the rewrite, so the next load rebuilds it.
  @Test
  public void testTextIndexRebuiltAfterFailureFollowingColumnRewrite()
      throws Exception {
    File indexDir = buildBaseSegment();
    Schema schema = schemaWithDerived();
    MultiColumnTextIndexConfig textIndexConfig = new MultiColumnTextIndexConfig(List.of(SOURCE, DERIVED));
    preprocess(indexDir, tableConfig(ORIGINAL_TRANSFORM, textIndexConfig), schema);
    assertTextMatches(indexDir, "oldsentinel", MutableRoaringBitmap.bitmapOf(0, 1));

    // A star-tree over a column the segment does not have fails the star-tree build after the column rewrite and
    // before the text index step. The preprocessor logs and skips a failed star-tree build instead of failing the
    // load, so process() completes with the column rewritten and the text index no longer over the old values.
    TableConfig failingConfig = tableConfig(UPDATED_TRANSFORM, textIndexConfig);
    failingConfig.getIndexingConfig().setEnableDynamicStarTreeCreation(true);
    failingConfig.getIndexingConfig().setStarTreeIndexConfigs(
        List.of(new StarTreeIndexConfig(List.of("missingColumn"), null, List.of("COUNT__*"), null, 1000)));
    preprocess(indexDir, failingConfig, schema);
    assertEquals(new SegmentMetadataImpl(indexDir).getColumnMetadataFor(DERIVED).getTransformFunction(),
        UPDATED_TRANSFORM);
    assertTextMatches(indexDir, "oldsentinel", new MutableRoaringBitmap());
    assertTextMatches(indexDir, "newsentinel", MutableRoaringBitmap.bitmapOf(0, 1));
  }

  private static File buildBaseSegment()
      throws Exception {
    File outDir = new File(TEMP_DIR, "base");
    FileUtils.deleteQuietly(outDir);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE)
        .addSingleValueDimension(SOURCE, DataType.STRING)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE).build();
    GenericRow row = new GenericRow();
    row.putValue(SOURCE, "alpha");
    GenericRow row2 = new GenericRow();
    row2.putValue(SOURCE, "beta");
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outDir.getAbsolutePath());
    config.setSegmentName(SEGMENT);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(row, row2)));
    driver.build();
    return new File(outDir, SEGMENT);
  }

  private static Schema schemaWithDerived() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE)
        .addSingleValueDimension(SOURCE, DataType.STRING)
        .addSingleValueDimension(DERIVED, DataType.STRING)
        .build();
  }

  private static TableConfig tableConfig(String transformFunction, MultiColumnTextIndexConfig textIndexConfig) {
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setTransformConfigs(List.of(new TransformConfig(DERIVED, transformFunction)));
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE)
        .setIngestionConfig(ingestionConfig)
        .setMultiColumnTextIndexConfig(textIndexConfig)
        .build();
  }

  private static void preprocess(File indexDir, TableConfig tableConfig, Schema schema)
      throws Exception {
    try (SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
        SegmentPreProcessor processor =
            new SegmentPreProcessor(segmentDirectory, new IndexLoadingConfig(tableConfig, schema))) {
      assertTrue(processor.needProcess());
      processor.process();
    }
  }

  private static void assertTextMatches(File indexDir, String query, MutableRoaringBitmap expected)
      throws Exception {
    try (MultiColumnLuceneTextIndexReader reader =
        new MultiColumnLuceneTextIndexReader(new SegmentMetadataImpl(indexDir))) {
      assertEquals(reader.getDocIds(DERIVED, query, (String) null), expected);
    }
  }
}
