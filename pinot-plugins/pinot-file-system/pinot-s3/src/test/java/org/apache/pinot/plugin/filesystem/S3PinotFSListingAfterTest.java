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

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class S3PinotFSListingAfterTest {
  private static final URI DIR = URI.create("s3://bucket/data/");

  private S3Client _s3Client;
  private S3PinotFS _s3PinotFS;

  @BeforeMethod
  public void setUp() {
    _s3Client = mock(S3Client.class);
    _s3PinotFS = new S3PinotFS();
    _s3PinotFS.init(_s3Client);
  }

  @Test
  public void testStartsAfterTheGivenKey()
      throws IOException {
    when(_s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(false, null, "data/d=2/a.parquet", "data/d=3/a.parquet"));

    assertTrue(_s3PinotFS.supportsListingAfter());
    final List<FileMetadata> files = _s3PinotFS.listFilesWithMetadataAfter(DIR, "s3://bucket/data/d=1/~", 10);

    assertEquals(files.stream().map(FileMetadata::getFilePath).toList(),
        List.of("s3://bucket/data/d=2/a.parquet", "s3://bucket/data/d=3/a.parquet"));
    assertTrue(files.stream().noneMatch(FileMetadata::isDirectory));
    final ListObjectsV2Request request = onlyRequest();
    assertEquals(request.bucket(), "bucket");
    assertEquals(request.prefix(), "data/");
    assertEquals(request.startAfter(), "data/d=1/~");
    assertNull(request.delimiter());
    assertEquals(request.maxKeys().intValue(), 10);
  }

  @Test
  public void testNullStartsAtTheFirstKey()
      throws IOException {
    when(_s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(page(false, null, "data/a"));

    _s3PinotFS.listFilesWithMetadataAfter(DIR, null, 5);

    assertNull(onlyRequest().startAfter());
  }

  @Test
  public void testContinuationReplacesStartAfter()
      throws IOException {
    when(_s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(true, "token", "data/b"))
        .thenReturn(page(false, null, "data/c"));

    final List<FileMetadata> files = _s3PinotFS.listFilesWithMetadataAfter(DIR, "s3://bucket/data/a", 5);

    assertEquals(files.size(), 2);
    final ArgumentCaptor<ListObjectsV2Request> captor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
    verify(_s3Client, times(2)).listObjectsV2(captor.capture());
    assertEquals(captor.getAllValues().get(0).startAfter(), "data/a");
    assertEquals(captor.getAllValues().get(1).continuationToken(), "token");
    assertNull(captor.getAllValues().get(1).startAfter());
    assertEquals(captor.getAllValues().get(1).maxKeys().intValue(), 4);
  }

  @Test
  public void testSkipsDirectoryMarkersAndKeepsPaging()
      throws IOException {
    when(_s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(true, "token", "data/d=1/", "data/d=1/a"))
        .thenReturn(page(false, null, "data/d=2/a"));

    final List<FileMetadata> files = _s3PinotFS.listFilesWithMetadataAfter(DIR, null, 2);

    assertEquals(files.stream().map(FileMetadata::getFilePath).toList(),
        List.of("s3://bucket/data/d=1/a", "s3://bucket/data/d=2/a"));
  }

  @Test
  public void testStopsAtMaxResults()
      throws IOException {
    when(_s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(true, "token", "data/a", "data/b", "data/c"));

    assertEquals(_s3PinotFS.listFilesWithMetadataAfter(DIR, null, 2).size(), 2);
    onlyRequest();
  }

  @Test
  public void testCapsPageSizeAtTheS3Limit()
      throws IOException {
    when(_s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(page(false, null));

    _s3PinotFS.listFilesWithMetadataAfter(DIR, null, 5000);

    assertEquals(onlyRequest().maxKeys().intValue(), 1000);
  }

  @Test
  public void testNonPositiveMaxResultsSendsNoRequest()
      throws IOException {
    assertTrue(_s3PinotFS.listFilesWithMetadataAfter(DIR, null, 0).isEmpty());
    verify(_s3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectsStartAfterInAnotherBucket()
      throws IOException {
    _s3PinotFS.listFilesWithMetadataAfter(DIR, "s3://other/data/a", 5);
  }

  private ListObjectsV2Request onlyRequest() {
    final ArgumentCaptor<ListObjectsV2Request> captor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
    verify(_s3Client, times(1)).listObjectsV2(captor.capture());
    return captor.getValue();
  }

  private static ListObjectsV2Response page(final boolean truncated, final String nextToken, final String... keys) {
    final List<S3Object> objects = new ArrayList<>();
    for (final String key : keys) {
      objects.add(S3Object.builder().key(key).size(1L).lastModified(Instant.EPOCH).build());
    }
    return ListObjectsV2Response.builder().contents(objects).isTruncated(truncated)
        .nextContinuationToken(nextToken).build();
  }
}
