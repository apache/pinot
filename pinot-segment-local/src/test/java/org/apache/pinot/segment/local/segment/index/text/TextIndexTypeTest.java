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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
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

  /// V3-layout sibling of the legacy-directory-first test: a not-yet-migrated V3 segment keeps its
  /// Lucene directory under the `v3/` subdirectory. The gate's lookup
  /// (`SegmentDirectoryPaths.findTextIndexIndexFile`) must find it there and skip the
  /// consolidated-entry probe the same way.
  @Test
  public void testReaderFactoryUsesLegacyTextDirectoryInV3LayoutWithoutProbing()
      throws Exception {
    File indexDir = new File(FileUtils.getTempDirectory(), "text-index-type-v3-legacy-" + System.nanoTime());
    FileUtils.deleteQuietly(indexDir);
    try {
      File v3Dir = new File(indexDir, SegmentDirectoryPaths.V3_SUBDIRECTORY_NAME);
      Assert.assertTrue(v3Dir.mkdirs());
      createLegacyTextIndex(v3Dir);
      Assert.assertTrue(
          new File(v3Dir, COLUMN + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION).isDirectory(),
          "test setup: legacy Lucene text index directory must exist under v3/");

      SegmentDirectory.Reader segmentReader = mockSegmentReader(indexDir);

      try (TextIndexReader reader = createReaderWithStoreInSegmentFile(segmentReader)) {
        Assert.assertNotNull(reader, "v3-layout legacy text index directory must be readable with "
            + "storeInSegmentFile=true");
        Assert.assertEquals(reader.getDocIds("clean", null).getCardinality(), 2);
      }
      Mockito.verify(segmentReader, Mockito.never()).getIndexFor(COLUMN, StandardIndexes.text());
    } finally {
      FileUtils.deleteQuietly(indexDir);
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
