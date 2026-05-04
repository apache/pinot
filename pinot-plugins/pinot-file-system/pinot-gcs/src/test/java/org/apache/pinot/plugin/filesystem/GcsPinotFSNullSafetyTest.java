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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;


/**
 * Unit tests verifying null-safety fixes in GcsPinotFS:
 * - open() throws FileNotFoundException (not NPE) when the blob does not exist
 * - copy() throws FileNotFoundException (not NPE) when the source blob does not exist
 */
public class GcsPinotFSNullSafetyTest {

  private Storage _mockStorage;
  private GcsPinotFS _gcsPinotFS;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _mockStorage = mock(Storage.class);
    _gcsPinotFS = new GcsPinotFS();
    Field storageField = GcsPinotFS.class.getDeclaredField("_storage");
    storageField.setAccessible(true);
    storageField.set(_gcsPinotFS, _mockStorage);
  }

  @Test
  public void testOpenThrowsFileNotFoundExceptionWhenBlobDoesNotExist()
      throws IOException {
    URI uri = URI.create("gs://test-bucket/missing-file");
    when(_mockStorage.get(any(BlobId.class))).thenReturn(null);

    try {
      _gcsPinotFS.open(uri);
      fail("Expected FileNotFoundException");
    } catch (FileNotFoundException ex) {
      assertEquals(ex.getMessage(), "File '" + uri + "' does not exist");
    }
  }

  @Test
  public void testOpenDoesNotThrowNullPointerException()
      throws IOException {
    URI uri = URI.create("gs://test-bucket/missing-file");
    when(_mockStorage.get(any(BlobId.class))).thenReturn(null);

    // Must throw FileNotFoundException, not NullPointerException
    assertThrows(FileNotFoundException.class, () -> _gcsPinotFS.open(uri));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCopyFileThrowsFileNotFoundExceptionWhenSourceBlobDoesNotExist()
      throws IOException {
    URI srcUri = URI.create("gs://test-bucket/src-file");
    URI dstUri = URI.create("gs://test-bucket/dst-file");

    // existsDirectoryOrBucket calls _storage.get(BlobId with prefix ending in '/')  -> null
    // Then calls _storage.list(...) -> empty page -> returns false
    Page<Blob> emptyPage = mock(Page.class);
    when(emptyPage.iterateAll()).thenReturn(Collections.emptyList());
    when(_mockStorage.list(anyString(), (Storage.BlobListOption[]) any())).thenReturn(emptyPage);

    // _storage.get for exact path (no trailing slash):
    //   First call  -> existsFile check: return a blob so exists() returns true
    //   Second call -> copyFile's getBlob: return null to trigger FileNotFoundException
    Blob mockBlob = mock(Blob.class);
    when(mockBlob.exists()).thenReturn(true);
    when(_mockStorage.get(any(BlobId.class)))
        .thenReturn(null)      // existsDirectoryOrBucket: prefix-path blob (with trailing /)
        .thenReturn(mockBlob)  // existsFile: file-path blob (no trailing /)
        .thenReturn(null);     // copyFile's getBlob: returns null -> FileNotFoundException

    try {
      _gcsPinotFS.copyDir(srcUri, dstUri);
      fail("Expected FileNotFoundException");
    } catch (FileNotFoundException ex) {
      assertEquals(ex.getMessage(), "Source file 'gs://test-bucket/src-file' does not exist");
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCopyFileDoesNotThrowNullPointerExceptionWhenSourceBlobDoesNotExist()
      throws IOException {
    URI srcUri = URI.create("gs://test-bucket/src-file");
    URI dstUri = URI.create("gs://test-bucket/dst-file");

    Page<Blob> emptyPage = mock(Page.class);
    when(emptyPage.iterateAll()).thenReturn(Collections.emptyList());
    when(_mockStorage.list(anyString(), (Storage.BlobListOption[]) any())).thenReturn(emptyPage);

    Blob mockBlob = mock(Blob.class);
    when(mockBlob.exists()).thenReturn(true);
    when(_mockStorage.get(any(BlobId.class)))
        .thenReturn(null)
        .thenReturn(mockBlob)
        .thenReturn(null);

    // Must throw FileNotFoundException, not NullPointerException
    assertThrows(FileNotFoundException.class, () -> _gcsPinotFS.copyDir(srcUri, dstUri));
  }
}
