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

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexDirectory;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class SingleFileIndexDirectoryTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), SingleFileIndexDirectoryTest.class.toString());

  static final long ONE_KB = 1024L;
  static final long ONE_MB = ONE_KB * ONE_KB;
  static final long ONE_GB = ONE_MB * ONE_KB;

  private SegmentMetadataImpl _segmentMetadata;

  @BeforeMethod
  public void setUp()
      throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(TEMP_DIR);
    writeMetadata();
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  void writeMetadata() {
    SegmentMetadataImpl meta = Mockito.mock(SegmentMetadataImpl.class);
    Mockito.when(meta.getVersion()).thenReturn(SegmentVersion.v3);
    _segmentMetadata = meta;
  }

  @Test
  public void testWithEmptyDir()
      throws Exception {
    // segmentDir does not have anything to begin with
    assertEquals(TEMP_DIR.list().length, 0);
    SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap);
    PinotDataBuffer writtenBuffer = columnDirectory.newBuffer("foo", ColumnIndexType.DICTIONARY, 1024);
    String data = "This is a test string";
    final byte[] dataBytes = data.getBytes();
    int pos = 0;
    for (byte b : dataBytes) {
      writtenBuffer.putByte(pos++, b);
    }
    writtenBuffer.close();

    Mockito.when(_segmentMetadata.getAllColumns()).thenReturn(new HashSet<String>(Arrays.asList("foo")));
    try (SingleFileIndexDirectory directoryReader = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata,
        ReadMode.mmap); PinotDataBuffer readBuffer = directoryReader.getBuffer("foo", ColumnIndexType.DICTIONARY)) {
      assertEquals(1024, readBuffer.size());
      int length = dataBytes.length;
      for (int i = 0; i < length; i++) {
        byte b = readBuffer.getByte(i);
        assertEquals(dataBytes[i], b);
      }
    }
  }

  @Test
  public void testMmapLargeBuffer()
      throws Exception {
    testMultipleRW(ReadMode.mmap, 6, 4L * ONE_MB);
  }

  @Test
  public void testLargeRWDirectBuffer()
      throws Exception {
    testMultipleRW(ReadMode.heap, 6, 3L * ONE_MB);
  }

  @Test
  public void testModeChange()
      throws Exception {
    // first verify it all works for one mode
    long size = 2L * ONE_MB;
    testMultipleRW(ReadMode.heap, 6, size);
    try (ColumnIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata,
        ReadMode.mmap)) {
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", 6);
    }
  }

  private void testMultipleRW(ReadMode readMode, int numIter, long size)
      throws Exception {
    try (
        SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, readMode)) {
      ColumnIndexDirectoryTestHelper.performMultipleWrites(columnDirectory, "foo", size, numIter);
    }

    // now read and validate data
    try (ColumnIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, readMode)) {
      ColumnIndexDirectoryTestHelper.verifyMultipleReads(columnDirectory, "foo", numIter);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testWriteExisting()
      throws Exception {
    try (SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata,
        ReadMode.mmap)) {
      columnDirectory.newBuffer("column1", ColumnIndexType.DICTIONARY, 1024);
    }
    try (SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata,
        ReadMode.mmap)) {
      columnDirectory.newBuffer("column1", ColumnIndexType.DICTIONARY, 1024);
    }
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testMissingIndex()
      throws IOException, ConfigurationException {
    try (SingleFileIndexDirectory columnDirectory = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata,
        ReadMode.mmap)) {
      columnDirectory.getBuffer("column1", ColumnIndexType.DICTIONARY);
    }
  }

  @Test
  public void testRemoveIndex()
      throws IOException, ConfigurationException {
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      sfd.newBuffer("col1", ColumnIndexType.DICTIONARY, 1024);
      sfd.removeIndex("col1", ColumnIndexType.DICTIONARY);
      assertFalse(sfd.hasIndexFor("col1", ColumnIndexType.DICTIONARY));
    }
  }

  @Test
  public void testCleanupRemovedIndices()
      throws IOException, ConfigurationException {
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      PinotDataBuffer buf = sfd.newBuffer("col1", ColumnIndexType.FORWARD_INDEX, 1024);
      buf.putInt(0, 1); // from begin position.

      buf = sfd.newBuffer("col1", ColumnIndexType.DICTIONARY, 1024);
      buf.putChar(111, 'h');

      buf = sfd.newBuffer("col2", ColumnIndexType.FORWARD_INDEX, 1024);
      buf.putChar(222, 'w');

      buf = sfd.newBuffer("col1", ColumnIndexType.JSON_INDEX, 1024);
      buf.putLong(333, 111111L);

      buf = sfd.newBuffer("col2", ColumnIndexType.H3_INDEX, 1024);
      buf.putDouble(1016, 222.222); // touch end position.
    }

    // Remove the JSON index to trigger cleanup, but keep H3 index.
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      assertTrue(sfd.hasIndexFor("col1", ColumnIndexType.JSON_INDEX));
      sfd.removeIndex("col1", ColumnIndexType.JSON_INDEX);
    }

    // Read indices back and check the content.
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      assertFalse(sfd.hasIndexFor("col1", ColumnIndexType.JSON_INDEX));

      assertTrue(sfd.hasIndexFor("col1", ColumnIndexType.FORWARD_INDEX));
      PinotDataBuffer buf = sfd.getBuffer("col1", ColumnIndexType.FORWARD_INDEX);
      assertEquals(buf.getInt(0), 1);

      assertTrue(sfd.hasIndexFor("col1", ColumnIndexType.DICTIONARY));
      buf = sfd.getBuffer("col1", ColumnIndexType.DICTIONARY);
      assertEquals(buf.getChar(111), 'h');

      assertTrue(sfd.hasIndexFor("col2", ColumnIndexType.FORWARD_INDEX));
      buf = sfd.getBuffer("col2", ColumnIndexType.FORWARD_INDEX);
      assertEquals(buf.getChar(222), 'w');

      assertTrue(sfd.hasIndexFor("col2", ColumnIndexType.H3_INDEX));
      buf = sfd.getBuffer("col2", ColumnIndexType.H3_INDEX);
      assertEquals(buf.getDouble(1016), 222.222);
    }
  }

  @Test
  public void testRemoveTextIndices()
      throws IOException, ConfigurationException {
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap);
        LuceneTextIndexCreator fooCreator = new LuceneTextIndexCreator("foo", TEMP_DIR, true,
            null, null);
        LuceneTextIndexCreator barCreator = new LuceneTextIndexCreator("bar", TEMP_DIR, true,
            null, null)) {
      PinotDataBuffer buf = sfd.newBuffer("col1", ColumnIndexType.FORWARD_INDEX, 1024);
      buf.putInt(0, 1);

      buf = sfd.newBuffer("col1", ColumnIndexType.DICTIONARY, 1024);
      buf.putChar(111, 'h');

      fooCreator.add("{\"clean\":\"this\"}");
      fooCreator.seal();
      barCreator.add("{\"retain\":\"this\"}");
      barCreator.add("{\"keep\":\"this\"}");
      barCreator.add("{\"hold\":\"this\"}");
      barCreator.seal();
    }

    // Remove the Text index to trigger cleanup.
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      assertTrue(sfd.hasIndexFor("foo", ColumnIndexType.TEXT_INDEX));
      // Use TextIndex once to trigger the creation of mapping files.
      LuceneTextIndexReader fooReader = new LuceneTextIndexReader("foo", TEMP_DIR, 1, new HashMap<>());
      fooReader.getDocIds("clean");
      LuceneTextIndexReader barReader = new LuceneTextIndexReader("bar", TEMP_DIR, 3, new HashMap<>());
      barReader.getDocIds("retain hold");

      // Both files for TextIndex should be removed.
      sfd.removeIndex("foo", ColumnIndexType.TEXT_INDEX);
      assertFalse(new File(TEMP_DIR, "foo" + V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION).exists());
      assertFalse(
          new File(TEMP_DIR, "foo" + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION).exists());
    }
    assertTrue(new File(TEMP_DIR, "bar" + V1Constants.Indexes.LUCENE_TEXT_INDEX_FILE_EXTENSION).exists());
    assertTrue(new File(TEMP_DIR, "bar" + V1Constants.Indexes.LUCENE_TEXT_INDEX_DOCID_MAPPING_FILE_EXTENSION).exists());

    // Read indices back and check the content.
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      assertFalse(sfd.hasIndexFor("foo", ColumnIndexType.TEXT_INDEX));

      assertTrue(sfd.hasIndexFor("col1", ColumnIndexType.FORWARD_INDEX));
      PinotDataBuffer buf = sfd.getBuffer("col1", ColumnIndexType.FORWARD_INDEX);
      assertEquals(buf.getInt(0), 1);

      assertTrue(sfd.hasIndexFor("col1", ColumnIndexType.DICTIONARY));
      buf = sfd.getBuffer("col1", ColumnIndexType.DICTIONARY);
      assertEquals(buf.getChar(111), 'h');

      assertTrue(sfd.hasIndexFor("bar", ColumnIndexType.TEXT_INDEX));

      // Check if the text index still work.
      LuceneTextIndexReader barReader = new LuceneTextIndexReader("bar", TEMP_DIR, 3, new HashMap<>());
      MutableRoaringBitmap ids = barReader.getDocIds("retain hold");
      assertTrue(ids.contains(0));
      assertTrue(ids.contains(2));
    }
  }

  @Test
  public void testCopyIndices()
      throws IOException {
    File srcTmp = new File(TEMP_DIR, UUID.randomUUID().toString());
    if (!srcTmp.exists()) {
      FileUtils.touch(srcTmp);
    }
    File dstTmp = new File(TEMP_DIR, UUID.randomUUID().toString());
    Map<IndexKey, IndexEntry> indicesToCopy = new TreeMap<>(ImmutableMap
        .of(new IndexKey("foo", ColumnIndexType.INVERTED_INDEX),
            new IndexEntry(new IndexKey("foo", ColumnIndexType.INVERTED_INDEX), 0, 0),
            new IndexKey("foo", ColumnIndexType.FORWARD_INDEX),
            new IndexEntry(new IndexKey("foo", ColumnIndexType.FORWARD_INDEX), 0, 0),
            new IndexKey("bar", ColumnIndexType.FORWARD_INDEX),
            new IndexEntry(new IndexKey("bar", ColumnIndexType.FORWARD_INDEX), 0, 0),
            new IndexKey("bar", ColumnIndexType.DICTIONARY),
            new IndexEntry(new IndexKey("bar", ColumnIndexType.DICTIONARY), 0, 0),
            new IndexKey("bar", ColumnIndexType.JSON_INDEX),
            new IndexEntry(new IndexKey("bar", ColumnIndexType.JSON_INDEX), 0, 0)));
    List<IndexEntry> retained = SingleFileIndexDirectory.copyIndices(srcTmp, dstTmp, indicesToCopy);
    List<IndexKey> retainedKeys = retained.stream().map(e -> e._key).collect(Collectors.toList());
    // The returned entries are sorted.
    assertEquals(retainedKeys, Arrays
        .asList(new IndexKey("bar", ColumnIndexType.DICTIONARY), new IndexKey("bar", ColumnIndexType.FORWARD_INDEX),
            new IndexKey("bar", ColumnIndexType.JSON_INDEX), new IndexKey("foo", ColumnIndexType.FORWARD_INDEX),
            new IndexKey("foo", ColumnIndexType.INVERTED_INDEX)));
  }

  @Test
  public void testPersistIndexMaps() {
    ByteArrayOutputStream output = new ByteArrayOutputStream(1024 * 1024);
    try (PrintWriter pw = new PrintWriter(output)) {
      List<IndexEntry> entries = Arrays
          .asList(new IndexEntry(new IndexKey("foo", ColumnIndexType.INVERTED_INDEX), 0, 1024),
              new IndexEntry(new IndexKey("bar", ColumnIndexType.INVERTED_INDEX), 1024, 100),
              new IndexEntry(new IndexKey("baz", ColumnIndexType.INVERTED_INDEX), 1124, 200));
      SingleFileIndexDirectory.persistIndexMaps(entries, pw);
    }
    assertEquals(output.toString(), "foo.inverted_index.startOffset = 0\nfoo.inverted_index.size = 1024\n"
        + "bar.inverted_index.startOffset = 1024\nbar.inverted_index.size = 100\n"
        + "baz.inverted_index.startOffset = 1124\nbaz.inverted_index.size = 200\n");
  }

  @Test
  public void testGetColumnIndices()
      throws Exception {
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap);
        LuceneTextIndexCreator fooCreator = new LuceneTextIndexCreator("foo", TEMP_DIR, true,
            null, null);
        LuceneTextIndexCreator barCreator = new LuceneTextIndexCreator("bar", TEMP_DIR, true,
            null, null)) {
      PinotDataBuffer buf = sfd.newBuffer("col1", ColumnIndexType.FORWARD_INDEX, 1024);
      buf.putInt(0, 111);
      buf = sfd.newBuffer("col2", ColumnIndexType.DICTIONARY, 1024);
      buf.putInt(0, 222);
      buf = sfd.newBuffer("col3", ColumnIndexType.FORWARD_INDEX, 1024);
      buf.putInt(0, 333);
      buf = sfd.newBuffer("col4", ColumnIndexType.INVERTED_INDEX, 1024);
      buf.putInt(0, 444);
      buf = sfd.newBuffer("col5", ColumnIndexType.H3_INDEX, 1024);
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
        .thenReturn(new HashSet<>(Arrays.asList("col1", "col2", "col3", "col4", "foo", "bar")));
    try (SingleFileIndexDirectory sfd = new SingleFileIndexDirectory(TEMP_DIR, _segmentMetadata, ReadMode.mmap)) {
      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.FORWARD_INDEX),
          new HashSet<>(Arrays.asList("col1", "col3")));
      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.DICTIONARY),
          new HashSet<>(Collections.singletonList("col2")));
      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.INVERTED_INDEX),
          new HashSet<>(Collections.singletonList("col4")));
      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.H3_INDEX),
          new HashSet<>(Collections.singletonList("col5")));
      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.TEXT_INDEX), new HashSet<>(Arrays.asList("foo", "bar")));

      sfd.removeIndex("col1", ColumnIndexType.FORWARD_INDEX);
      sfd.removeIndex("col2", ColumnIndexType.DICTIONARY);
      sfd.removeIndex("col5", ColumnIndexType.H3_INDEX);
      sfd.removeIndex("foo", ColumnIndexType.TEXT_INDEX);
      sfd.removeIndex("col111", ColumnIndexType.DICTIONARY);

      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.FORWARD_INDEX),
          new HashSet<>(Collections.singletonList("col3")));
      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.DICTIONARY), new HashSet<>(Collections.emptySet()));
      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.INVERTED_INDEX),
          new HashSet<>(Collections.singletonList("col4")));
      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.H3_INDEX), new HashSet<>(Collections.emptySet()));
      assertEquals(sfd.getColumnsWithIndex(ColumnIndexType.TEXT_INDEX),
          new HashSet<>(Collections.singletonList("bar")));
    }
  }
}
