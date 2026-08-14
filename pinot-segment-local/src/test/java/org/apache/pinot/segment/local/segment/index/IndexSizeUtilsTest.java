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
package org.apache.pinot.segment.local.segment.index;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.IndexType;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// Unit tests for [IndexSizeUtils#sizeOfFileOrDirIndex], covering the file/directory/mixed layouts and the marker
/// overhead rule that [org.apache.pinot.segment.local.segment.creator.impl.BaseSegmentCreator], `SegmentPreProcessor`
/// and `SegmentCompressionStatsReader` all rely on.
public class IndexSizeUtilsTest {
  private static final String COLUMN = "column1";
  private static final String EXT_A = ".a";
  private static final String EXT_B = ".b";
  private static final long MARKER_OVERHEAD = 8L;

  private File _contentDir;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _contentDir = Files.createTempDirectory("IndexSizeUtilsTest").toFile();
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_contentDir);
  }

  private IndexType<?, ?, ?> mockIndexType(String... extensions) {
    IndexType<?, ?, ?> indexType = mock(IndexType.class);
    when(indexType.getFileExtensions(org.mockito.ArgumentMatchers.any())).thenReturn(List.of(extensions));
    return indexType;
  }

  @Test
  public void testNoFilesReturnsUnavailable()
      throws Exception {
    IndexType<?, ?, ?> indexType = mockIndexType(EXT_A);
    long size = IndexSizeUtils.sizeOfFileOrDirIndex(indexType, _contentDir, COLUMN, MARKER_OVERHEAD, null);
    assertEquals(size, ColumnMetadata.UNAVAILABLE);
  }

  @Test
  public void testSingleFileAddsMarkerOverheadOnce()
      throws Exception {
    File file = new File(_contentDir, COLUMN + EXT_A);
    FileUtils.writeStringToFile(file, "0123456789", (java.nio.charset.Charset) null);
    IndexType<?, ?, ?> indexType = mockIndexType(EXT_A);
    long size = IndexSizeUtils.sizeOfFileOrDirIndex(indexType, _contentDir, COLUMN, MARKER_OVERHEAD, null);
    assertEquals(size, file.length() + MARKER_OVERHEAD);
  }

  @Test
  public void testZeroMarkerOverheadPassthrough()
      throws Exception {
    File file = new File(_contentDir, COLUMN + EXT_A);
    FileUtils.writeStringToFile(file, "0123456789", (java.nio.charset.Charset) null);
    IndexType<?, ?, ?> indexType = mockIndexType(EXT_A);
    long size = IndexSizeUtils.sizeOfFileOrDirIndex(indexType, _contentDir, COLUMN, 0, null);
    assertEquals(size, file.length());
  }

  @Test
  public void testTwoFileExtensionsSummedWithOverheadAddedOnce()
      throws Exception {
    File fileA = new File(_contentDir, COLUMN + EXT_A);
    File fileB = new File(_contentDir, COLUMN + EXT_B);
    FileUtils.writeStringToFile(fileA, "01234", (java.nio.charset.Charset) null);
    FileUtils.writeStringToFile(fileB, "0123456789", (java.nio.charset.Charset) null);
    IndexType<?, ?, ?> indexType = mockIndexType(EXT_A, EXT_B);
    long size = IndexSizeUtils.sizeOfFileOrDirIndex(indexType, _contentDir, COLUMN, MARKER_OVERHEAD, null);
    assertEquals(size, fileA.length() + fileB.length() + MARKER_OVERHEAD);
  }

  @Test
  public void testDirectoryOnlyNeverAddsMarkerOverhead()
      throws Exception {
    File dir = new File(_contentDir, COLUMN + EXT_A);
    org.apache.commons.io.FileUtils.forceMkdir(dir);
    FileUtils.writeStringToFile(new File(dir, "part"), "0123456789", (java.nio.charset.Charset) null);
    IndexType<?, ?, ?> indexType = mockIndexType(EXT_A);
    long size = IndexSizeUtils.sizeOfFileOrDirIndex(indexType, _contentDir, COLUMN, MARKER_OVERHEAD, null);
    assertEquals(size, FileUtils.sizeOfDirectory(dir));
  }

  @Test
  public void testMixedFileAndDirectoryAddsOverheadOnceForTheFile()
      throws Exception {
    File file = new File(_contentDir, COLUMN + EXT_A);
    File dir = new File(_contentDir, COLUMN + EXT_B);
    FileUtils.writeStringToFile(file, "01234", (java.nio.charset.Charset) null);
    org.apache.commons.io.FileUtils.forceMkdir(dir);
    FileUtils.writeStringToFile(new File(dir, "part"), "0123456789", (java.nio.charset.Charset) null);
    IndexType<?, ?, ?> indexType = mockIndexType(EXT_A, EXT_B);
    long size = IndexSizeUtils.sizeOfFileOrDirIndex(indexType, _contentDir, COLUMN, MARKER_OVERHEAD, null);
    assertEquals(size, file.length() + FileUtils.sizeOfDirectory(dir) + MARKER_OVERHEAD);
  }

  @Test
  public void testDirectorySizingFailureReturnsUnavailableRatherThanPartialSum()
      throws Exception {
    File file = new File(_contentDir, COLUMN + EXT_A);
    File dir = new File(_contentDir, COLUMN + EXT_B);
    FileUtils.writeStringToFile(file, "01234", (java.nio.charset.Charset) null);
    org.apache.commons.io.FileUtils.forceMkdir(dir);
    IndexType<?, ?, ?> indexType = mockIndexType(EXT_A, EXT_B);
    try (MockedStatic<FileUtils> mockedFileUtils = mockStatic(FileUtils.class, CALLS_REAL_METHODS)) {
      mockedFileUtils.when(() -> FileUtils.sizeOfDirectory(dir))
          .thenThrow(new java.io.UncheckedIOException(new java.io.IOException("Simulated directory sizing failure")));
      long size = IndexSizeUtils.sizeOfFileOrDirIndex(indexType, _contentDir, COLUMN, MARKER_OVERHEAD, null);
      assertEquals(size, ColumnMetadata.UNAVAILABLE);
    }
  }
}
