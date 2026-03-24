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
package org.apache.pinot.plugin.filesystem.test;

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.PathItem;
import java.io.IOException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for ADLSGen2PinotFS.listFilesWithMetadata(URI, boolean, Predicate, int)
 * — the paginated listing with lazy PagedIterable iteration and early termination.
 */
public class ADLSGen2PinotFSPaginatedListTest {

  private static final Predicate<String> ACCEPT_ALL = path -> true;
  private static final OffsetDateTime MTIME = OffsetDateTime.of(2026, 1, 15, 10, 30, 0, 0, ZoneOffset.UTC);

  @Mock
  private DataLakeFileSystemClient _mockFileSystemClient;

  @SuppressWarnings("rawtypes")
  @Mock
  private PagedIterable _mockPagedIterable;

  private ADLSGen2PinotFS _adlsPinotFS;
  private AutoCloseable _mocks;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    _adlsPinotFS = new ADLSGen2PinotFS(_mockFileSystemClient);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    _mocks.close();
  }

  private static PathItem mockPathItem(final String name, final boolean isDirectory,
      final long contentLength) {
    final PathItem item = mock(PathItem.class);
    when(item.getName()).thenReturn(name);
    when(item.isDirectory()).thenReturn(isDirectory);
    when(item.getContentLength()).thenReturn(contentLength);
    when(item.getLastModified()).thenReturn(MTIME);
    return item;
  }

  @SuppressWarnings("unchecked")
  private void setupIterator(final PathItem... items) {
    when(_mockFileSystemClient.listPaths(any(), any())).thenReturn(_mockPagedIterable);
    final Iterator<PathItem> iterator = Arrays.asList(items).iterator();
    when(_mockPagedIterable.iterator()).thenReturn(iterator);
  }

  @Test
  public void testAllMatch() throws IOException {
    setupIterator(
        mockPathItem("data/file1.parquet", false, 100),
        mockPathItem("data/file2.parquet", false, 200),
        mockPathItem("data/file3.parquet", false, 300)
    );

    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 3);
    assertEquals(result.get(0).getFilePath(), "/data/file1.parquet");
    assertEquals(result.get(1).getFilePath(), "/data/file2.parquet");
    assertEquals(result.get(2).getFilePath(), "/data/file3.parquet");
  }

  @Test
  public void testEarlyTermination() throws IOException {
    // 5 items, maxResults=3
    setupIterator(
        mockPathItem("data/f1.parquet", false, 100),
        mockPathItem("data/f2.parquet", false, 200),
        mockPathItem("data/f3.parquet", false, 300),
        mockPathItem("data/f4.parquet", false, 400),
        mockPathItem("data/f5.parquet", false, 500)
    );

    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, ACCEPT_ALL, 3);

    assertEquals(result.size(), 3);
    assertEquals(result.get(2).getFilePath(), "/data/f3.parquet");
  }

  @Test
  public void testMaxResultsOne() throws IOException {
    setupIterator(
        mockPathItem("data/f1.parquet", false, 100),
        mockPathItem("data/f2.parquet", false, 200)
    );

    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, ACCEPT_ALL, 1);

    assertEquals(result.size(), 1);
  }

  @Test
  public void testFilterPredicate() throws IOException {
    setupIterator(
        mockPathItem("data/file1.parquet", false, 100),
        mockPathItem("data/file2.csv", false, 200),
        mockPathItem("data/file3.parquet", false, 300),
        mockPathItem("data/file4.json", false, 400)
    );

    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, path -> path.endsWith(".parquet"), 10);

    assertEquals(result.size(), 2);
    assertTrue(result.stream().allMatch(f -> f.getFilePath().endsWith(".parquet")));
  }

  @Test
  public void testDirectoriesSkipped() throws IOException {
    setupIterator(
        mockPathItem("data/subdir", true, 0),
        mockPathItem("data/file1.parquet", false, 100),
        mockPathItem("data/anotherdir", true, 0),
        mockPathItem("data/file2.parquet", false, 200)
    );

    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 2);
    assertTrue(result.stream().noneMatch(FileMetadata::isDirectory));
  }

  @Test
  public void testFilterRejectsAll() throws IOException {
    setupIterator(
        mockPathItem("data/file1.parquet", false, 100),
        mockPathItem("data/file2.parquet", false, 200)
    );

    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, path -> false, 10);

    assertEquals(result.size(), 0);
  }

  @Test
  public void testEmptyListing() throws IOException {
    setupIterator();

    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 0);
  }

  @Test
  public void testFileMetadataAttributes() throws IOException {
    setupIterator(
        mockPathItem("data/file.parquet", false, 4096)
    );

    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 1);
    final FileMetadata fm = result.get(0);
    assertEquals(fm.getFilePath(), "/data/file.parquet");
    assertEquals(fm.getLength(), 4096L);
    assertEquals(fm.getLastModifiedTime(), MTIME.toInstant().toEpochMilli());
    assertFalse(fm.isDirectory());
  }

  @SuppressWarnings("unchecked")
  @Test(expectedExceptions = IOException.class)
  public void testDataLakeExceptionWrappedAsIOException() throws IOException {
    when(_mockFileSystemClient.listPaths(any(), any()))
        .thenThrow(mock(DataLakeStorageException.class));

    _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, ACCEPT_ALL, 10);
  }

  @Test
  public void testMaxResultsZeroReturnsEmpty() throws IOException {
    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, ACCEPT_ALL, 0);

    assertEquals(result.size(), 0);
  }

  @Test
  public void testMaxResultsNegativeReturnsEmpty() throws IOException {
    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, ACCEPT_ALL, -3);

    assertEquals(result.size(), 0);
  }

  @Test
  public void testEarlyTerminationWithFilterAndDirectories() throws IOException {
    // Mix of dirs, matching files, and non-matching files; maxResults=2
    setupIterator(
        mockPathItem("data/dir1", true, 0),
        mockPathItem("data/file1.csv", false, 100),
        mockPathItem("data/file2.parquet", false, 200),
        mockPathItem("data/dir2", true, 0),
        mockPathItem("data/file3.parquet", false, 300),
        mockPathItem("data/file4.parquet", false, 400)
    );

    final List<FileMetadata> result = _adlsPinotFS.listFilesWithMetadata(
        URI.create("abfss://container@account.dfs.core.windows.net/data/"),
        true, path -> path.endsWith(".parquet"), 2);

    assertEquals(result.size(), 2);
    assertEquals(result.get(0).getFilePath(), "/data/file2.parquet");
    assertEquals(result.get(1).getFilePath(), "/data/file3.parquet");
  }
}
