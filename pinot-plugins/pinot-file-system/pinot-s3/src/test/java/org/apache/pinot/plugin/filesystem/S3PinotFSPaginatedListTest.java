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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for S3PinotFS.listFilesWithMetadata(URI, boolean, Predicate, int)
 * — the paginated listing with early termination.
 */
public class S3PinotFSPaginatedListTest {

  private static final Instant NOW = Instant.now();
  private static final Predicate<String> ACCEPT_ALL = path -> true;

  private S3Client _mockS3Client;
  private S3PinotFS _s3PinotFS;

  @BeforeMethod
  public void setUp() {
    _mockS3Client = mock(S3Client.class);
    _s3PinotFS = new S3PinotFS();
    _s3PinotFS.init(_mockS3Client);
  }

  private static S3Object s3obj(final String key, final long size) {
    return S3Object.builder()
        .key(key)
        .size(size)
        .lastModified(NOW)
        .build();
  }

  private static ListObjectsV2Response page(final List<S3Object> objects,
      final boolean truncated, final String nextToken) {
    final ListObjectsV2Response.Builder builder = ListObjectsV2Response.builder()
        .contents(objects)
        .isTruncated(truncated);
    if (nextToken != null) {
      builder.nextContinuationToken(nextToken);
    }
    return builder.build();
  }

  @Test
  public void testSinglePageAllMatch() throws IOException {
    final List<S3Object> objects = Arrays.asList(
        s3obj("data/file1.parquet", 100),
        s3obj("data/file2.parquet", 200),
        s3obj("data/file3.parquet", 300)
    );
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(objects, false, null));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 3);
    assertEquals(result.get(0).getFilePath(), "s3://bucket/data/file1.parquet");
    assertEquals(result.get(1).getFilePath(), "s3://bucket/data/file2.parquet");
    assertEquals(result.get(2).getFilePath(), "s3://bucket/data/file3.parquet");
    verify(_mockS3Client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testSinglePageWithFilter() throws IOException {
    final List<S3Object> objects = Arrays.asList(
        s3obj("data/file1.parquet", 100),
        s3obj("data/file2.csv", 200),
        s3obj("data/file3.parquet", 300),
        s3obj("data/file4.json", 400)
    );
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(objects, false, null));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true,
        path -> path.endsWith(".parquet"), 10);

    assertEquals(result.size(), 2);
    assertTrue(result.stream().allMatch(f -> f.getFilePath().endsWith(".parquet")));
  }

  @Test
  public void testEarlyTerminationAcrossPages() throws IOException {
    // 3 pages with 3 files each; maxResults=5 should stop after page 2
    final List<S3Object> page1 = Arrays.asList(
        s3obj("data/file1.parquet", 100),
        s3obj("data/file2.parquet", 200),
        s3obj("data/file3.parquet", 300)
    );
    final List<S3Object> page2 = Arrays.asList(
        s3obj("data/file4.parquet", 400),
        s3obj("data/file5.parquet", 500),
        s3obj("data/file6.parquet", 600)
    );
    // page 3 should never be fetched
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(page1, true, "token1"))
        .thenReturn(page(page2, true, "token2"));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 5);

    assertEquals(result.size(), 5);
    // Only 2 S3 API calls, NOT 3
    verify(_mockS3Client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testEarlyTerminationWithinPage() throws IOException {
    // 10 files on a single page; maxResults=3 should stop mid-page
    final List<S3Object> objects = Arrays.asList(
        s3obj("data/f1.parquet", 100),
        s3obj("data/f2.parquet", 200),
        s3obj("data/f3.parquet", 300),
        s3obj("data/f4.parquet", 400),
        s3obj("data/f5.parquet", 500)
    );
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(objects, false, null));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 3);

    assertEquals(result.size(), 3);
    assertEquals(result.get(0).getFilePath(), "s3://bucket/data/f1.parquet");
    assertEquals(result.get(2).getFilePath(), "s3://bucket/data/f3.parquet");
  }

  @Test
  public void testMaxResultsOneStopsImmediately() throws IOException {
    final List<S3Object> objects = Arrays.asList(
        s3obj("data/file1.parquet", 100),
        s3obj("data/file2.parquet", 200)
    );
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(objects, true, "token1"));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 1);

    assertEquals(result.size(), 1);
    // Only 1 API call despite truncated=true
    verify(_mockS3Client, times(1)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testContinuationTokenPassedCorrectly() throws IOException {
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(Arrays.asList(s3obj("data/f1.parquet", 100)), true, "abc123"))
        .thenReturn(page(Arrays.asList(s3obj("data/f2.parquet", 200)), false, null));

    _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 10);

    final ArgumentCaptor<ListObjectsV2Request> captor =
        ArgumentCaptor.forClass(ListObjectsV2Request.class);
    verify(_mockS3Client, times(2)).listObjectsV2(captor.capture());
    // First call: no continuation token
    assertTrue(captor.getAllValues().get(0).continuationToken() == null);
    // Second call: has the token from page 1
    assertEquals(captor.getAllValues().get(1).continuationToken(), "abc123");
  }

  @Test
  public void testDirectoriesSkipped() throws IOException {
    final List<S3Object> objects = Arrays.asList(
        s3obj("data/subdir/", 0),
        s3obj("data/file1.parquet", 100),
        s3obj("data/another-dir/", 0),
        s3obj("data/file2.parquet", 200)
    );
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(objects, false, null));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 2);
    assertTrue(result.stream().noneMatch(FileMetadata::isDirectory));
  }

  @Test
  public void testFilterRejectsAll() throws IOException {
    final List<S3Object> objects = Arrays.asList(
        s3obj("data/file1.parquet", 100),
        s3obj("data/file2.parquet", 200)
    );
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(objects, false, null));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, path -> false, 10);

    assertEquals(result.size(), 0);
  }

  @Test
  public void testEmptyBucket() throws IOException {
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(Collections.emptyList(), false, null));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 0);
  }

  @Test
  public void testFileMetadataAttributes() throws IOException {
    final Instant mtime = Instant.parse("2026-01-15T10:30:00Z");
    final S3Object obj = S3Object.builder()
        .key("data/file.parquet")
        .size(4096L)
        .lastModified(mtime)
        .build();
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(Arrays.asList(obj), false, null));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 1);
    final FileMetadata fm = result.get(0);
    assertEquals(fm.getFilePath(), "s3://bucket/data/file.parquet");
    assertEquals(fm.getLength(), 4096L);
    assertEquals(fm.getLastModifiedTime(), mtime.toEpochMilli());
    assertEquals(fm.isDirectory(), false);
  }

  @Test
  public void testS3aScheme() throws IOException {
    final List<S3Object> objects = Arrays.asList(s3obj("data/file.parquet", 100));
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(objects, false, null));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3a://bucket/data/"), true, ACCEPT_ALL, 10);

    assertEquals(result.size(), 1);
    assertTrue(result.get(0).getFilePath().startsWith("s3a://"));
  }

  @Test
  public void testAllPagesNeededWhenMaxResultsHigh() throws IOException {
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(Arrays.asList(s3obj("data/f1.parquet", 100)), true, "t1"))
        .thenReturn(page(Arrays.asList(s3obj("data/f2.parquet", 200)), true, "t2"))
        .thenReturn(page(Arrays.asList(s3obj("data/f3.parquet", 300)), false, null));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 100);

    assertEquals(result.size(), 3);
    verify(_mockS3Client, times(3)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testFilterWithEarlyTerminationAcrossPages() throws IOException {
    // Page 1: 2 parquet + 1 csv; Page 2: 2 parquet + 1 csv; maxResults=3 for parquet only
    final List<S3Object> page1 = Arrays.asList(
        s3obj("data/a.parquet", 100),
        s3obj("data/b.csv", 200),
        s3obj("data/c.parquet", 300)
    );
    final List<S3Object> page2 = Arrays.asList(
        s3obj("data/d.csv", 400),
        s3obj("data/e.parquet", 500),
        s3obj("data/f.csv", 600)
    );
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(page1, true, "t1"))
        .thenReturn(page(page2, true, "t2"));

    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true,
        path -> path.endsWith(".parquet"), 3);

    assertEquals(result.size(), 3);
    assertTrue(result.stream().allMatch(f -> f.getFilePath().endsWith(".parquet")));
    // 2 pages fetched (2 from page1 + 1 from page2 = 3 matches)
    verify(_mockS3Client, times(2)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testMaxResultsZeroReturnsEmpty() throws IOException {
    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, 0);

    assertEquals(result.size(), 0);
    verify(_mockS3Client, times(0)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testMaxResultsNegativeReturnsEmpty() throws IOException {
    final List<FileMetadata> result = _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/"), true, ACCEPT_ALL, -5);

    assertEquals(result.size(), 0);
    verify(_mockS3Client, times(0)).listObjectsV2(any(ListObjectsV2Request.class));
  }

  @Test
  public void testPrefixSentToS3() throws IOException {
    when(_mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
        .thenReturn(page(Collections.emptyList(), false, null));

    _s3PinotFS.listFilesWithMetadata(
        URI.create("s3://bucket/data/2026/01/"), true, ACCEPT_ALL, 10);

    final ArgumentCaptor<ListObjectsV2Request> captor =
        ArgumentCaptor.forClass(ListObjectsV2Request.class);
    verify(_mockS3Client).listObjectsV2(captor.capture());
    assertEquals(captor.getValue().bucket(), "bucket");
    assertEquals(captor.getValue().prefix(), "data/2026/01/");
  }
}
