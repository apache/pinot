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
package org.apache.pinot.segment.local.segment.store;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.text.NativeTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexDirectory;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.util.TestUtils;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class FilePerIndexDirectoryTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), FilePerIndexDirectoryTest.class.toString());

  private SegmentMetadataImpl _segmentMetadata;

  static final long ONE_KB = 1024L;
  static final long ONE_MB = ONE_KB * ONE_KB;
  static final long ONE_GB = ONE_MB * ONE_KB;

  @BeforeMethod
  public void setUp()
      throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    _segmentMetadata = ColumnIndexDirectoryTestHelper.writeMetadata(SegmentVersion.v1);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testEmptyDirectory()
      throws Exception {
    assertEquals(0, TEMP_DIR.list().length, TEMP_DIR.list().toString());
    try (FilePerIndexDirectory fpiDir = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.heap);
        PinotDataBuffer buffer = fpiDir.newBuffer("col1", StandardIndexes.dictionary(), 1024)) {
      assertEquals(1, TEMP_DIR.list().length, TEMP_DIR.list().toString());

      buffer.putLong(0, 0xbadfadL);
      buffer.putInt(8, 51);
      // something at random location
      buffer.putInt(101, 55);
    }

    assertEquals(1, TEMP_DIR.list().length);

    try (FilePerIndexDirectory colDir = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap);
        PinotDataBuffer readBuffer = colDir.getBuffer("col1", StandardIndexes.dictionary())) {
      assertEquals(readBuffer.getLong(0), 0xbadfadL);
      assertEquals(readBuffer.getInt(8), 51);
      assertEquals(readBuffer.getInt(101), 55);
    }
  }

  @Test
  public void testMmapLargeBuffer()
      throws Exception {
    testMultipleRW(ReadMode.mmap, 6, 3L * ONE_MB);
  }

  @Test
  public void testLargeRWDirectBuffer()
      throws Exception {
    testMultipleRW(ReadMode.heap, 6, 3L * ONE_MB);
  }

  @Test
  public void testReadModeChange()
      throws Exception {
    // first verify it all works for one mode
    testMultipleRW(ReadMode.heap, 6, 100 * ONE_MB);
    try (ColumnIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", 6);
    }
  }

  private void testMultipleRW(ReadMode readMode, int numIter, long size)
      throws Exception {
    try (FilePerIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, readMode)) {
      ColumnIndexDirectoryTestHelper.performMultipleWrites(columnDirectory, "foo", size, numIter);
    }
    // now read and validate data
    try (FilePerIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, readMode)) {
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", numIter);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testWriteExisting()
      throws Exception {
    try (FilePerIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      columnDirectory.newBuffer("column1", StandardIndexes.dictionary(), 1024);
    }
    try (FilePerIndexDirectory columnDirectory = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      columnDirectory.newBuffer("column1", StandardIndexes.dictionary(), 1024);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMissingIndex()
      throws IOException {
    try (FilePerIndexDirectory fpiDirectory = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      fpiDirectory.getBuffer("noSuchColumn", StandardIndexes.dictionary());
    }
  }

  @Test
  public void testHasIndex()
      throws IOException {
    try (FilePerIndexDirectory fpiDirectory = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      PinotDataBuffer buffer = fpiDirectory.newBuffer("foo", StandardIndexes.dictionary(), 1024);
      buffer.putInt(0, 100);
      assertTrue(fpiDirectory.hasIndexFor("foo", StandardIndexes.dictionary()));
    }
  }

  @Test
  public void testRemoveIndex()
      throws IOException {
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      fpi.newBuffer("col1", StandardIndexes.forward(), 1024);
      fpi.newBuffer("col2", StandardIndexes.dictionary(), 100);
      assertTrue(fpi.getFileFor("col1", StandardIndexes.forward()).exists());
      assertTrue(fpi.getFileFor("col2", StandardIndexes.dictionary()).exists());
      fpi.removeIndex("col1", StandardIndexes.forward());
      assertFalse(fpi.getFileFor("col1", StandardIndexes.forward()).exists());
    }
  }

  @Test
  public void nativeTextIndexIsRecognized()
      throws IOException {
    // See https://github.com/apache/pinot/issues/11529
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap);
        NativeTextIndexCreator fooCreator = new NativeTextIndexCreator("foo", TEMP_DIR)) {

      fooCreator.add("{\"clean\":\"this\"}");
      fooCreator.seal();

      assertTrue(fpi.hasIndexFor("foo", StandardIndexes.text()), "Native text index not found");
    }
  }

  @Test
  public void nativeTextIndexIsDeleted()
      throws IOException {
    // See https://github.com/apache/pinot/issues/11529
    nativeTextIndexIsRecognized();
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      fpi.removeIndex("foo", StandardIndexes.text());
    }
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      assertFalse(fpi.hasIndexFor("foo", StandardIndexes.text()), "Native text index was not deleted");
    }
  }

  @Test
  public void testRemoveTextIndices()
      throws IOException {
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap);
        LuceneTextIndexCreator fooCreator = new LuceneTextIndexCreator("foo", TEMP_DIR, true,
            null, null, true, 500);
        LuceneTextIndexCreator barCreator = new LuceneTextIndexCreator("bar", TEMP_DIR, true,
            null, null, true, 500)) {
      PinotDataBuffer buf = fpi.newBuffer("col1", StandardIndexes.forward(), 1024);
      buf.putInt(0, 1);

      buf = fpi.newBuffer("col1", StandardIndexes.dictionary(), 1024);
      buf.putChar(111, 'h');

      fooCreator.add("{\"clean\":\"this\"}");
      fooCreator.seal();
      barCreator.add("{\"retain\":\"this\"}");
      barCreator.add("{\"keep\":\"this\"}");
      barCreator.add("{\"hold\":\"this\"}");
      barCreator.seal();
    }

    // Remove the Text index to trigger cleanup.
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      assertTrue(fpi.hasIndexFor("foo", StandardIndexes.text()));
      // Use TextIndex once to trigger the creation of mapping files.
      LuceneTextIndexReader fooReader = new LuceneTextIndexReader("foo", TEMP_DIR, 1, new HashMap<>());
      fooReader.getDocIds("clean");
      LuceneTextIndexReader barReader = new LuceneTextIndexReader("bar", TEMP_DIR, 3, new HashMap<>());
      barReader.getDocIds("retain hold");

      // Both files for TextIndex should be removed.
      fpi.removeIndex("foo", StandardIndexes.text());
      assertFalse(new File(TEMP_DIR, "foo" + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION).exists());
      assertFalse(
          new File(TEMP_DIR, "foo" + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION).exists());
    }
    assertTrue(new File(TEMP_DIR, "bar" + V1Constants.Indexes.LUCENE_V9_TEXT_INDEX_FILE_EXTENSION).exists());
    assertTrue(
        new File(TEMP_DIR, "bar" + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION).exists());

    // Read indices back and check the content.
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      assertFalse(fpi.hasIndexFor("foo", StandardIndexes.text()));

      assertTrue(fpi.hasIndexFor("col1", StandardIndexes.forward()));
      PinotDataBuffer buf = fpi.getBuffer("col1", StandardIndexes.forward());
      assertEquals(buf.getInt(0), 1);

      assertTrue(fpi.hasIndexFor("col1", StandardIndexes.dictionary()));
      buf = fpi.getBuffer("col1", StandardIndexes.dictionary());
      assertEquals(buf.getChar(111), 'h');

      assertTrue(fpi.hasIndexFor("bar", StandardIndexes.text()));

      // Check if the text index still work.
      LuceneTextIndexReader barReader = new LuceneTextIndexReader("bar", TEMP_DIR, 3, new HashMap<>());
      MutableRoaringBitmap ids = barReader.getDocIds("retain hold");
      assertTrue(ids.contains(0));
      assertTrue(ids.contains(2));
    }
  }

  @Test
  public void testGetColumnIndices()
      throws IOException {
    // Write sth to buffers and flush them to index files on disk
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap);
        LuceneTextIndexCreator fooCreator = new LuceneTextIndexCreator("foo", TEMP_DIR, true,
            null, null, true, 500);
        LuceneTextIndexCreator barCreator = new LuceneTextIndexCreator("bar", TEMP_DIR, true,
            null, null, true, 500)) {
      PinotDataBuffer buf = fpi.newBuffer("col1", StandardIndexes.forward(), 1024);
      buf.putInt(0, 111);
      buf = fpi.newBuffer("col2", StandardIndexes.dictionary(), 1024);
      buf.putInt(0, 222);
      buf = fpi.newBuffer("col3", StandardIndexes.forward(), 1024);
      buf.putInt(0, 333);
      buf = fpi.newBuffer("col4", StandardIndexes.inverted(), 1024);
      buf.putInt(0, 444);
      buf = fpi.newBuffer("col5", StandardIndexes.h3(), 1024);
      buf.putInt(0, 555);

      fooCreator.add("{\"clean\":\"this\"}");
      fooCreator.seal();
      barCreator.add("{\"retain\":\"this\"}");
      barCreator.add("{\"keep\":\"this\"}");
      barCreator.add("{\"hold\":\"this\"}");
      barCreator.seal();
    }

    // Need segmentMetadata to tell the full set of columns in this segment.
    when(_segmentMetadata.getAllColumns())
        .thenReturn(new TreeSet<>(Arrays.asList("col1", "col2", "col3", "col4", "col5", "foo", "bar")));
    try (FilePerIndexDirectory fpi = new FilePerIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.forward()),
          new HashSet<>(Arrays.asList("col1", "col3")));
      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.dictionary()),
          new HashSet<>(Collections.singletonList("col2")));
      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.inverted()),
          new HashSet<>(Collections.singletonList("col4")));
      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.h3()),
          new HashSet<>(Collections.singletonList("col5")));
      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.text()), new HashSet<>(Arrays.asList("foo", "bar")));

      fpi.removeIndex("col1", StandardIndexes.forward());
      fpi.removeIndex("col2", StandardIndexes.dictionary());
      fpi.removeIndex("col5", StandardIndexes.h3());
      fpi.removeIndex("foo", StandardIndexes.text());
      fpi.removeIndex("col111", StandardIndexes.dictionary());

      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.forward()),
          new HashSet<>(Collections.singletonList("col3")));
      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.dictionary()), new HashSet<>(Collections.emptySet()));
      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.inverted()),
          new HashSet<>(Collections.singletonList("col4")));
      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.h3()), new HashSet<>(Collections.emptySet()));
      assertEquals(fpi.getColumnsWithIndex(StandardIndexes.text()),
          new HashSet<>(Collections.singletonList("bar")));
    }
  }
}
