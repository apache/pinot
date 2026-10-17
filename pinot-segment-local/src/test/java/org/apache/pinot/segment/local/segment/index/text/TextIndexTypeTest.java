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

package org.apache.pinot.segment.local.segment.index.text;

import java.io.File;
import java.nio.ByteOrder;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCombined;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests for [TextIndexType], focused on the reader factory's legacy-directory-first gate for
/// `storeInSegmentFile=true`.
public class TextIndexTypeTest {

  private static final String COLUMN = "foo";
  private static final int NUM_DOCS = 5;

  /// Rolling-upgrade safety: with `storeInSegmentFile=true` and the legacy text-index Lucene
  /// directory still on disk (a not-yet-migrated V3 segment — TextIndexHandler's combined-format
  /// conversion is v3-gated — or a V1/V2 segment backed by `FilePerIndexDirectory`), the reader
  /// factory must use the directory directly WITHOUT probing the consolidated entry. On
  /// `FilePerIndexDirectory` the probe resolves the directory itself and fails to map it
  /// (`IllegalArgumentException: ... must be a regular file`), which used to kill the segment load
  /// before the legacy fallback could run.
  @Test
  public void testReaderFactoryUsesLegacyTextDirectoryWithoutProbingConsolidatedEntry()
      throws Exception {
    File indexDir = new File(FileUtils.getTempDirectory(), "text-index-type-legacy-" + System.nanoTime());
    FileUtils.deleteQuietly(indexDir);
    try {
      Assert.assertTrue(indexDir.mkdirs());
      createLegacyTextIndex(indexDir);
      File luceneDir = new File(indexDir, COLUMN + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
      Assert.assertTrue(luceneDir.isDirectory(), "test setup: legacy Lucene text index directory must exist");

      SegmentDirectory.Reader segmentReader = mockSegmentReader(indexDir);
      // Mirror FilePerIndexDirectory on a V1/V2 segment: getIndexFor resolves the Lucene DIRECTORY
      // and mapForReads rejects it. If the factory probes the consolidated entry, this propagates
      // and fails the load — the legacy-directory-first gate must prevent the call entirely.
      Mockito.when(segmentReader.getIndexFor(COLUMN, StandardIndexes.text()))
          .thenThrow(new IllegalArgumentException("File: " + luceneDir + " must be a regular file"));

      try (TextIndexReader reader = createReaderWithStoreInSegmentFile(segmentReader)) {
        Assert.assertNotNull(reader, "legacy text index directory must be readable with storeInSegmentFile=true");
        Assert.assertEquals(reader.getDocIds("clean", null).getCardinality(), 2);
      }
      // The consolidated-entry probe must never have run while the legacy directory exists.
      Mockito.verify(segmentReader, Mockito.never()).getIndexFor(COLUMN, StandardIndexes.text());
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }

  /// Regression: when the segment directory path is not a local filesystem directory (e.g. a
  /// `SegmentDirectory` backed by remote/tiered storage), the legacy-directory probe must be
  /// skipped — `SegmentDirectoryPaths.findTextIndexIndexFile` -> `findFormatFile` rejects
  /// non-directory paths with `IllegalArgumentException`, which used to fail the whole segment
  /// load before `getIndexFor` could serve the valid consolidated columns.psf entry. The factory
  /// must go straight to the consolidated entry and return a working reader. Mirrors the vector
  /// reader's `testReaderFactoryLoadsConsolidatedHnswWhenSegmentDirectoryIsNotLocal`.
  @Test
  public void testReaderFactoryLoadsConsolidatedTextWhenSegmentDirectoryIsNotLocal()
      throws Exception {
    File buildDir = new File(FileUtils.getTempDirectory(), "text-index-type-nonlocal-" + System.nanoTime());
    FileUtils.deleteQuietly(buildDir);
    PinotDataBuffer buffer = null;
    try {
      Assert.assertTrue(buildDir.mkdirs());
      // Build a legacy Lucene text index directory, then pack it into a combined buffer standing in
      // for the consolidated columns.psf entry.
      createLegacyTextIndex(buildDir);
      File luceneDir = new File(buildDir, COLUMN + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
      Assert.assertTrue(luceneDir.isDirectory(), "test setup: legacy Lucene text index directory must exist");
      File combinedFile = new File(buildDir, COLUMN + V1Constants.Indexes.LUCENE_COMBINE_TEXT_INDEX_FILE_EXTENSION);
      LuceneTextIndexCombined.combineLuceneIndexFiles(luceneDir, combinedFile.getAbsolutePath());
      // BIG_ENDIAN mirrors how columns.psf entries are mapped in production.
      buffer = PinotDataBuffer.mapFile(combinedFile, /* readOnly */ true, 0, combinedFile.length(),
          ByteOrder.BIG_ENDIAN, "text-index-type-nonlocal-test");

      SegmentDirectory segmentDirectory = Mockito.mock(SegmentDirectory.class);
      SegmentDirectory.Reader segmentReader = Mockito.mock(SegmentDirectory.Reader.class);
      // A path that exists nowhere on the local filesystem, as getPath() yields for remote-backed
      // segment directories.
      Mockito.when(segmentDirectory.getPath())
          .thenReturn(new File("/segments/textTest/nonexistent-" + System.nanoTime()).toPath());
      Mockito.when(segmentReader.toSegmentDirectory()).thenReturn(segmentDirectory);
      Mockito.when(segmentReader.getIndexFor(COLUMN, StandardIndexes.text())).thenReturn(buffer);

      TextIndexConfig readerConfig = new TextIndexConfigBuilder().withStoreInSegmentFile(true).build();
      FieldIndexConfigs fieldIndexConfigs =
          new FieldIndexConfigs.Builder().add(StandardIndexes.text(), readerConfig).build();
      ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
      Mockito.when(metadata.getColumnName()).thenReturn(COLUMN);
      Mockito.when(metadata.getDataType()).thenReturn(FieldSpec.DataType.STRING);
      Mockito.when(metadata.getTotalDocs()).thenReturn(NUM_DOCS);

      try (TextIndexReader reader = StandardIndexes.text().getReaderFactory()
          .createIndexReader(segmentReader, fieldIndexConfigs, metadata)) {
        Assert.assertNotNull(reader,
            "consolidated text entry must load when the segment directory is not a local path");
        Assert.assertEquals(reader.getDocIds("clean", null).getCardinality(), 2);
      }
    } finally {
      if (buffer != null) {
        buffer.close();
      }
      FileUtils.deleteQuietly(buildDir);
    }
  }

  /// Builds a real legacy text index (storeInSegmentFile=false => Lucene directory on disk).
  private static void createLegacyTextIndex(File dir)
      throws Exception {
    TextIndexConfig creatorConfig = new TextIndexConfigBuilder().build();
    try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator(COLUMN, dir, true, false, null, null,
        creatorConfig)) {
      creator.add("clean this");
      creator.add("retain this");
      creator.add("keep this");
      creator.add("hold this");
      creator.add("clean that");
      creator.seal();
    }
  }

  private static SegmentDirectory.Reader mockSegmentReader(File indexDir) {
    SegmentDirectory segmentDirectory = Mockito.mock(SegmentDirectory.class);
    SegmentDirectory.Reader segmentReader = Mockito.mock(SegmentDirectory.Reader.class);
    Mockito.when(segmentDirectory.getPath()).thenReturn(indexDir.toPath());
    Mockito.when(segmentReader.toSegmentDirectory()).thenReturn(segmentDirectory);
    return segmentReader;
  }

  private static TextIndexReader createReaderWithStoreInSegmentFile(SegmentDirectory.Reader segmentReader)
      throws Exception {
    TextIndexConfig readerConfig = new TextIndexConfigBuilder().withStoreInSegmentFile(true).build();
    FieldIndexConfigs fieldIndexConfigs =
        new FieldIndexConfigs.Builder().add(StandardIndexes.text(), readerConfig).build();

    ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
    Mockito.when(metadata.getColumnName()).thenReturn(COLUMN);
    Mockito.when(metadata.getDataType()).thenReturn(FieldSpec.DataType.STRING);
    Mockito.when(metadata.getTotalDocs()).thenReturn(NUM_DOCS);

    return StandardIndexes.text().getReaderFactory().createIndexReader(segmentReader, fieldIndexConfigs, metadata);
  }
}
