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
package org.apache.pinot.plugin.filesystem;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for GcsPinotFS.listFilesWithMetadata(URI, boolean, Predicate, int)
 * — the paginated listing with early termination using Page&lt;Blob&gt;.
 */
public class GcsPinotFSPaginatedListTest {

  private static final Predicate<String> ACCEPT_ALL = path -> true;

  private Storage _mockStorage;
  private GcsPinotFS _gcsPinotFS;

  @BeforeMethod
  public void setUp() throws Exception {
    _mockStorage = mock(Storage.class);
    _gcsPinotFS = new GcsPinotFS();
    final Field storageField = GcsPinotFS.class.getDeclaredField("_storage");
    storageField.setAccessible(true);
    storageField.set(_gcsPinotFS, _mockStorage);
  }

  @SuppressWarnings("unchecked")
  private static Page<Blob> mockPage(final List<Blob> blobs, final Page<Blob> nextPage) {
    final Page<Blob> page = mock(Page.class);
    when(page.getValues()).thenReturn(blobs);
    when(page.hasNextPage()).thenReturn(nextPage != null);
    when(page.getNextPage()).thenReturn(nextPage);
    return page;
  }

  private static Blob mockBlob(final String name, final long size, final Long updateTime) {
    final Blob blob = mock(Blob.class);
    when(blob.getName()).thenReturn(name);
    when(blob.getSize()).thenReturn(size);
    when(blob.getUpdateTime()).thenReturn(updateTime);
    return blob;
  }

  @SuppressWarnings("unchecked")
  private void stubStorageList(final Page<Blob> page) {
    when(_mockStorage.list(anyString(), (Storage.BlobListOption[]) any()))
        .thenReturn(page);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testSinglePageAllMatch() throws IOException {
    final List<Blob> blobs = Arrays.asList(
        mockBlob("data/file1.parquet", 100, 1000L),
        mockBlob("data/file2.parquet", 200, 2000L)
    );
    stubStorageList(mockPage(blobs, null));

    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 2);
    assertEquals(result.get(0).getFilePath(), "gs://bucket/data/file1.parquet");
    assertEquals(result.get(1).getFilePath(), "gs://bucket/data/file2.parquet");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testEarlyTerminationSkipsProcessingNextPage() throws IOException {
    // Page 1 has 3 blobs, Page 2 has 1 blob; maxResults=3
    // Page 2's getValues() should never be called — while-loop exits because result.size() == maxResults
    final Page<Blob> page2 = mockPage(
        Arrays.asList(mockBlob("data/f4.parquet", 400, 4000L)), null);
    final Page<Blob> page1 = mockPage(Arrays.asList(
        mockBlob("data/f1.parquet", 100, 1000L),
        mockBlob("data/f2.parquet", 200, 2000L),
        mockBlob("data/f3.parquet", 300, 3000L)
    ), page2);
    stubStorageList(page1);

    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, 3);

    assertEquals(result.size(), 3);
    // page2.getValues() should never be invoked: the while-loop exits before iterating page 2
    verify(page2, never()).getValues();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMultiplePagesNeeded() throws IOException {
    final Page<Blob> page2 = mockPage(
        Arrays.asList(mockBlob("data/f3.parquet", 300, 3000L)), null);
    final Page<Blob> page1 = mockPage(Arrays.asList(
        mockBlob("data/f1.parquet", 100, 1000L),
        mockBlob("data/f2.parquet", 200, 2000L)
    ), page2);
    stubStorageList(page1);

    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 3);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFilterPredicate() throws IOException {
    final List<Blob> blobs = Arrays.asList(
        mockBlob("data/file1.parquet", 100, 1000L),
        mockBlob("data/file2.csv", 200, 2000L),
        mockBlob("data/file3.parquet", 300, 3000L)
    );
    stubStorageList(mockPage(blobs, null));

    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true,
        path -> path.endsWith(".parquet"), 10);

    assertEquals(result.size(), 2);
    assertTrue(result.stream().allMatch(f -> f.getFilePath().endsWith(".parquet")));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDirectoriesSkipped() throws IOException {
    final List<Blob> blobs = Arrays.asList(
        mockBlob("data/subdir/", 0, 1000L),
        mockBlob("data/file1.parquet", 100, 2000L),
        mockBlob("data/another/", 0, 3000L),
        mockBlob("data/file2.parquet", 200, 4000L)
    );
    stubStorageList(mockPage(blobs, null));

    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 2);
    assertTrue(result.stream().noneMatch(FileMetadata::isDirectory));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testPrefixDirectoryMarkerSkipped() throws IOException {
    final List<Blob> blobs = Arrays.asList(
        mockBlob("data/", 0, 1000L),
        mockBlob("data/file1.parquet", 100, 2000L)
    );
    stubStorageList(mockPage(blobs, null));

    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 1);
    assertEquals(result.get(0).getFilePath(), "gs://bucket/data/file1.parquet");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNullUpdateTimeHandled() throws IOException {
    final List<Blob> blobs = Arrays.asList(
        mockBlob("data/file1.parquet", 100, null)
    );
    stubStorageList(mockPage(blobs, null));

    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 1);
    // lastModifiedTime defaults to 0 when updateTime is null
    assertEquals(result.get(0).getLastModifiedTime(), 0L);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testEmptyBucket() throws IOException {
    stubStorageList(mockPage(Collections.emptyList(), null));

    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 0);
  }

  @Test
  public void testMaxResultsZeroReturnsEmpty() throws IOException {
    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, 0);

    assertEquals(result.size(), 0);
    verify(_mockStorage, never()).list(anyString(), (Storage.BlobListOption[]) any());
  }

  @Test
  public void testMaxResultsNegativeReturnsEmpty() throws IOException {
    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, -1);

    assertEquals(result.size(), 0);
    verify(_mockStorage, never()).list(anyString(), (Storage.BlobListOption[]) any());
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testFileMetadataAttributes() throws IOException {
    final List<Blob> blobs = Arrays.asList(
        mockBlob("data/file.parquet", 4096, 1705312200000L)
    );
    stubStorageList(mockPage(blobs, null));

    final List<FileMetadata> result = _gcsPinotFS.listFilesWithMetadata(
        URI.create("gs://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 1);
    final FileMetadata fm = result.get(0);
    assertEquals(fm.getFilePath(), "gs://bucket/data/file.parquet");
    assertEquals(fm.getLength(), 4096L);
    assertEquals(fm.getLastModifiedTime(), 1705312200000L);
    assertEquals(fm.isDirectory(), false);
  }
}
