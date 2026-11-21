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
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class GcsPinotFSListingAfterTest {
  private static final URI DIR = URI.create("gs://bucket/data/");

  private Storage _storage;
  private GcsPinotFS _gcsPinotFS;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _storage = mock(Storage.class);
    _gcsPinotFS = new GcsPinotFS();
    // Same injection as GcsPinotFSPaginatedListTest: GcsPinotFS has no constructor taking a Storage.
    final Field storageField = GcsPinotFS.class.getDeclaredField("_storage");
    storageField.setAccessible(true);
    storageField.set(_gcsPinotFS, _storage);
  }

  /// Paths are URI-escaped object names; the start path is decoded back to the name, and the inclusive startOffset
  /// match is dropped.
  @Test
  public void testStartsAfterTheObjectTheEscapedPathNames()
      throws IOException {
    stubList(page(null, "data/a b", "data/a c/", "data/b c"));

    final List<FileMetadata> files = _gcsPinotFS.listFilesWithMetadataAfter(DIR, "gs://bucket/data/a%20b", 10);

    assertTrue(_gcsPinotFS.supportsListingAfter());
    assertEquals(files.stream().map(FileMetadata::getFilePath).toList(), List.of("gs://bucket/data/b%20c"));
    final List<Storage.BlobListOption> options = options();
    assertTrue(options.contains(Storage.BlobListOption.startOffset("data/a b")));
    assertTrue(options.contains(Storage.BlobListOption.prefix("data/")));
    assertTrue(options.contains(Storage.BlobListOption.pageSize(10)));
  }

  @Test
  public void testNullStartsAtTheFirstObject()
      throws IOException {
    stubList(page(null, "data/a"));

    assertEquals(_gcsPinotFS.listFilesWithMetadataAfter(DIR, null, 5).size(), 1);
    assertFalse(options().stream().anyMatch(o -> o.toString().contains("startOffset")));
  }

  @Test
  public void testPagesUntilMaxResults()
      throws IOException {
    final Page<Blob> last = page(null, "data/never");
    stubList(page(page(last, "data/b", "data/c"), "data/a/"));

    final List<FileMetadata> files = _gcsPinotFS.listFilesWithMetadataAfter(DIR, null, 1);

    assertEquals(files.stream().map(FileMetadata::getFilePath).toList(), List.of("gs://bucket/data/b"));
    verify(last, never()).getValues();
  }

  @Test
  public void testNonPositiveMaxResultsSendsNoRequest()
      throws IOException {
    assertTrue(_gcsPinotFS.listFilesWithMetadataAfter(DIR, null, 0).isEmpty());
    verify(_storage, never()).list(anyString(), any(Storage.BlobListOption[].class));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectsStartAfterInAnotherBucket()
      throws IOException {
    _gcsPinotFS.listFilesWithMetadataAfter(DIR, "gs://other/data/a", 5);
  }

  private void stubList(final Page<Blob> first) {
    when(_storage.list(anyString(), any(Storage.BlobListOption[].class))).thenReturn(first);
  }

  private List<Storage.BlobListOption> options() {
    final ArgumentCaptor<Storage.BlobListOption[]> captor = ArgumentCaptor.forClass(Storage.BlobListOption[].class);
    verify(_storage).list(eq("bucket"), captor.capture());
    return List.of(captor.getValue());
  }

  @SuppressWarnings("unchecked")
  private static Page<Blob> page(final Page<Blob> next, final String... names) {
    final List<Blob> blobs = new ArrayList<>();
    for (final String name : names) {
      final Blob blob = mock(Blob.class);
      when(blob.getName()).thenReturn(name);
      when(blob.getSize()).thenReturn(1L);
      blobs.add(blob);
    }
    final Page<Blob> page = mock(Page.class);
    when(page.getValues()).thenReturn(blobs);
    when(page.hasNextPage()).thenReturn(next != null);
    when(page.getNextPage()).thenReturn(next);
    return page;
  }
}
