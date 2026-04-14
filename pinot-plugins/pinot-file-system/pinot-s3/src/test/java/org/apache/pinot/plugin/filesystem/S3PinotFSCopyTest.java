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

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests to verify that CopyObjectRequest uses sourceBucket/sourceKey
 * instead of the deprecated copySource with URL encoding.
 * This fixes compatibility with S3-compatible backends (Ceph, NetApp ONTAP)
 * where %2F-encoded slashes are interpreted literally.
 *
 * @see <a href="https://github.com/apache/pinot/issues/11182">#11182</a>
 */
public class S3PinotFSCopyTest {

  private S3Client _s3Client;
  private S3PinotFS _s3PinotFS;

  @BeforeMethod
  public void setUp() {
    _s3Client = mock(S3Client.class);
    _s3PinotFS = new S3PinotFS();
    _s3PinotFS.init(_s3Client);

    // copyDir calls delete(dstUri) before copyFile, and isDirectory before that.
    // Mock these so the flow reaches copyFile/copyObject.
    ListObjectsV2Response listResponse = mock(ListObjectsV2Response.class);
    when(listResponse.hasContents()).thenReturn(false);
    when(_s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listResponse);

    DeleteObjectResponse deleteResponse = mock(DeleteObjectResponse.class);
    SdkHttpResponse deleteHttpResponse = mock(SdkHttpResponse.class);
    when(deleteHttpResponse.isSuccessful()).thenReturn(true);
    when(deleteResponse.sdkHttpResponse()).thenReturn(deleteHttpResponse);
    when(_s3Client.deleteObject(any(DeleteObjectRequest.class))).thenReturn(deleteResponse);
  }

  private void mockHeadObject() {
    HeadObjectResponse headResponse = mock(HeadObjectResponse.class);
    when(headResponse.lastModified()).thenReturn(Instant.ofEpochMilli(1000L));
    when(_s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(headResponse);
  }

  private void mockSuccessfulCopy() {
    CopyObjectResponse copyResponse = mock(CopyObjectResponse.class);
    SdkHttpResponse httpResponse = mock(SdkHttpResponse.class);
    when(httpResponse.isSuccessful()).thenReturn(true);
    when(copyResponse.sdkHttpResponse()).thenReturn(httpResponse);
    when(_s3Client.copyObject(any(CopyObjectRequest.class))).thenReturn(copyResponse);
  }

  private CopyObjectRequest captureCopyRequest() {
    ArgumentCaptor<CopyObjectRequest> captor = ArgumentCaptor.forClass(CopyObjectRequest.class);
    verify(_s3Client).copyObject(captor.capture());
    return captor.getValue();
  }

  // ---- copyDir (which delegates to copyFile for non-directory sources) tests ----

  @Test
  public void testCopyFileUsesSourceBucketAndSourceKey()
      throws Exception {
    mockHeadObject();
    mockSuccessfulCopy();

    URI srcUri = URI.create("s3://test-bucket/myfile.txt");
    URI dstUri = URI.create("s3://test-bucket/myfile-copy.txt");
    _s3PinotFS.copyDir(srcUri, dstUri);

    CopyObjectRequest captured = captureCopyRequest();
    assertEquals(captured.sourceBucket(), "test-bucket");
    assertEquals(captured.sourceKey(), "myfile.txt");
    assertEquals(captured.destinationBucket(), "test-bucket");
    assertEquals(captured.destinationKey(), "myfile-copy.txt");
    assertNull(captured.copySource(),
        "copySource should not be set; sourceBucket/sourceKey should be used instead");
  }

  @Test
  public void testCopyFilePreservesSlashesInDeepPath()
      throws Exception {
    mockHeadObject();
    mockSuccessfulCopy();

    URI srcUri = URI.create("s3://my-bucket/a/b/c/d/file.tar.gz");
    URI dstUri = URI.create("s3://my-bucket/x/y/z/file.tar.gz");
    _s3PinotFS.copyDir(srcUri, dstUri);

    CopyObjectRequest captured = captureCopyRequest();
    assertEquals(captured.sourceBucket(), "my-bucket");
    assertEquals(captured.sourceKey(), "a/b/c/d/file.tar.gz");
    assertEquals(captured.destinationBucket(), "my-bucket");
    assertEquals(captured.destinationKey(), "x/y/z/file.tar.gz");
    assertNull(captured.copySource());
  }

  @Test
  public void testCopyFileAcrossBuckets()
      throws Exception {
    mockHeadObject();
    mockSuccessfulCopy();

    URI srcUri = URI.create("s3://src-bucket/data/segment.tar.gz");
    URI dstUri = URI.create("s3://dst-bucket/backup/segment.tar.gz");
    _s3PinotFS.copyDir(srcUri, dstUri);

    CopyObjectRequest captured = captureCopyRequest();
    assertEquals(captured.sourceBucket(), "src-bucket");
    assertEquals(captured.sourceKey(), "data/segment.tar.gz");
    assertEquals(captured.destinationBucket(), "dst-bucket");
    assertEquals(captured.destinationKey(), "backup/segment.tar.gz");
  }

  @Test
  public void testCopyFileWithS3aScheme()
      throws Exception {
    mockHeadObject();
    mockSuccessfulCopy();

    URI srcUri = URI.create("s3a://my-bucket/path/to/file.txt");
    URI dstUri = URI.create("s3a://my-bucket/new/path/file.txt");
    _s3PinotFS.copyDir(srcUri, dstUri);

    CopyObjectRequest captured = captureCopyRequest();
    assertEquals(captured.sourceBucket(), "my-bucket");
    assertEquals(captured.sourceKey(), "path/to/file.txt");
    assertEquals(captured.destinationBucket(), "my-bucket");
    assertEquals(captured.destinationKey(), "new/path/file.txt");
  }

  @Test
  public void testCopyFileWithSpecialCharactersInPath()
      throws Exception {
    mockHeadObject();
    mockSuccessfulCopy();

    URI srcUri = URI.create("s3://test-bucket/path%20with%20spaces/file+name.txt");
    URI dstUri = URI.create("s3://test-bucket/dest/file+name.txt");
    _s3PinotFS.copyDir(srcUri, dstUri);

    CopyObjectRequest captured = captureCopyRequest();
    assertEquals(captured.sourceBucket(), "test-bucket");
    assertNull(captured.copySource(), "copySource must not be set; no manual encoding should happen");
  }

  // ---- touch() tests ----

  @Test
  public void testTouchExistingFileUsesSourceBucketAndSourceKey()
      throws Exception {
    HeadObjectResponse oldHead = mock(HeadObjectResponse.class);
    when(oldHead.lastModified()).thenReturn(Instant.ofEpochMilli(1000L));
    HeadObjectResponse newHead = mock(HeadObjectResponse.class);
    when(newHead.lastModified()).thenReturn(Instant.ofEpochMilli(2000L));
    when(_s3Client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(oldHead)
        .thenReturn(newHead);
    mockSuccessfulCopy();

    URI uri = URI.create("s3://my-bucket/deep/nested/path/file.txt");
    boolean result = _s3PinotFS.touch(uri);
    assertTrue(result);

    CopyObjectRequest captured = captureCopyRequest();
    assertEquals(captured.sourceBucket(), "my-bucket");
    assertEquals(captured.sourceKey(), "deep/nested/path/file.txt");
    assertEquals(captured.destinationBucket(), "my-bucket");
    assertEquals(captured.destinationKey(), "deep/nested/path/file.txt");
    assertNull(captured.copySource(), "copySource should not be set");
    assertTrue(captured.metadata().containsKey("lastModified"));
  }

  @Test
  public void testTouchNewFileCreatesPutObject()
      throws Exception {
    when(_s3Client.headObject(any(HeadObjectRequest.class)))
        .thenThrow(NoSuchKeyException.builder().message("Not found").build());

    PutObjectResponse putResponse = mock(PutObjectResponse.class);
    SdkHttpResponse httpResponse = mock(SdkHttpResponse.class);
    when(httpResponse.isSuccessful()).thenReturn(true);
    when(putResponse.sdkHttpResponse()).thenReturn(httpResponse);
    when(_s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(putResponse);

    URI uri = URI.create("s3://my-bucket/new/file.txt");
    boolean result = _s3PinotFS.touch(uri);
    assertTrue(result);

    ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
    verify(_s3Client).putObject(captor.capture(), any(RequestBody.class));
    PutObjectRequest captured = captor.getValue();
    assertEquals(captured.bucket(), "my-bucket");
    assertEquals(captured.key(), "new/file.txt");
  }

  /**
   * Reproduces the exact scenario from issue #11182:
   * Ceph S3-compatible backend receives a copy request during segment finalization.
   * With the old code, slashes were encoded as %2F which caused Ceph to fail
   * with "failed to parse x-amz-copy-source header".
   */
  @Test
  public void testCephCompatibilitySlashesNotEncoded()
      throws Exception {
    mockHeadObject();
    mockSuccessfulCopy();

    URI srcUri = URI.create("s3://pinot-segments/table_REALTIME/segment__0__1__20240101T0000Z/v3");
    URI dstUri = URI.create("s3://pinot-segments/table_REALTIME/segment__0__1__20240101T0000Z/v3_finalized");
    _s3PinotFS.copyDir(srcUri, dstUri);

    CopyObjectRequest captured = captureCopyRequest();
    assertEquals(captured.sourceBucket(), "pinot-segments");
    assertEquals(captured.sourceKey(), "table_REALTIME/segment__0__1__20240101T0000Z/v3");
    assertNull(captured.copySource(),
        "copySource must not be set - it would contain %2F-encoded slashes that break Ceph");
  }

  /**
   * Demonstrates the exact bug from issue #11182 and proves the fix.
   *
   * OLD code did: URLEncoder.encode(host + path) → produced "pinot-segments%2Ftable%2Fsegment"
   * then passed that as copySource. Ceph rejected this with 400: "failed to parse x-amz-copy-source header"
   * because %2F was interpreted as a literal key character, not a path delimiter.
   *
   * NEW code passes sourceBucket and sourceKey separately, so the SDK builds the header correctly
   * with real "/" delimiters that Ceph (and all S3-compatible backends) can parse.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testOldCodeProducedEncodedSlashesNewCodeDoesNot()
      throws Exception {
    URI srcUri = URI.create("s3://pinot-segments/table_REALTIME/segment__0__1__20240101T0000Z/v3");
    String bucket = srcUri.getHost();
    String key = srcUri.getPath().substring(1); // strip leading "/"

    // --- OLD behavior (what the code used to do) ---
    // This is what caused the Ceph 400 error
    String oldEncodedCopySource = URLEncoder.encode(bucket + srcUri.getPath(), StandardCharsets.UTF_8);

    // The old code encoded "/" as "%2F", making the entire path opaque
    assertTrue(oldEncodedCopySource.contains("%2F"),
        "Old URLEncoder.encode approach encodes '/' as '%2F'");
    assertEquals(oldEncodedCopySource,
        "pinot-segments%2Ftable_REALTIME%2Fsegment__0__1__20240101T0000Z%2Fv3");

    // Building a CopyObjectRequest the old way with copySource
    CopyObjectRequest oldRequest = CopyObjectRequest.builder()
        .copySource(oldEncodedCopySource)
        .destinationBucket(bucket)
        .destinationKey(key)
        .build();
    // The copySource contains %2F — Ceph rejects this
    assertTrue(oldRequest.copySource().contains("%2F"),
        "Old request copySource contains %2F which breaks Ceph");

    // --- NEW behavior (what the fixed code does) ---
    mockHeadObject();
    mockSuccessfulCopy();
    URI dstUri = URI.create("s3://pinot-segments/table_REALTIME/segment__0__1__20240101T0000Z/v3_finalized");
    _s3PinotFS.copyDir(srcUri, dstUri);

    CopyObjectRequest newRequest = captureCopyRequest();

    // The new code uses sourceBucket + sourceKey — no encoding issues
    assertEquals(newRequest.sourceBucket(), "pinot-segments");
    assertEquals(newRequest.sourceKey(), "table_REALTIME/segment__0__1__20240101T0000Z/v3");
    assertNull(newRequest.copySource(),
        "New request must not use copySource at all — sourceBucket/sourceKey let the SDK handle encoding");

    // sourceKey preserves real "/" delimiters — Ceph parses this correctly
    assertTrue(newRequest.sourceKey().contains("/"),
        "sourceKey contains real '/' delimiters, not %2F");
    assertTrue(!newRequest.sourceKey().contains("%2F"),
        "sourceKey must not contain %2F encoded slashes");
  }

  /**
   * Verifies the fix works for NetApp ONTAP S3-compatible storage.
   * ONTAP has the same issue as Ceph: %2F-encoded slashes in the x-amz-copy-source
   * header are interpreted literally, causing copy/touch operations to fail with 400.
   *
   * Uses a realistic ONTAP bucket/path structure to confirm sourceBucket/sourceKey
   * are passed cleanly without any URL encoding.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testNetappOntapCompatibilitySlashesNotEncoded()
      throws Exception {
    // ONTAP-style deep path with multiple nested directories
    URI srcUri = URI.create("s3://ontap-pinot-store/prod/tables/events_REALTIME/segment__0__2__20240315T1200Z");
    String bucket = srcUri.getHost();

    // Prove the old approach would have broken ONTAP (same as Ceph)
    String oldEncoded = URLEncoder.encode(bucket + srcUri.getPath(), StandardCharsets.UTF_8);
    assertTrue(oldEncoded.contains("%2F"),
        "Old URLEncoder.encode encodes '/' as '%2F' which breaks NetApp ONTAP");
    assertEquals(oldEncoded,
        "ontap-pinot-store%2Fprod%2Ftables%2Fevents_REALTIME%2Fsegment__0__2__20240315T1200Z");

    // Verify the new code path produces a clean request
    mockHeadObject();
    mockSuccessfulCopy();
    URI dstUri = URI.create("s3://ontap-pinot-store/prod/tables/events_REALTIME/segment__0__2__20240315T1200Z_copy");
    _s3PinotFS.copyDir(srcUri, dstUri);

    CopyObjectRequest captured = captureCopyRequest();
    assertEquals(captured.sourceBucket(), "ontap-pinot-store");
    assertEquals(captured.sourceKey(), "prod/tables/events_REALTIME/segment__0__2__20240315T1200Z");
    assertEquals(captured.destinationBucket(), "ontap-pinot-store");
    assertEquals(captured.destinationKey(), "prod/tables/events_REALTIME/segment__0__2__20240315T1200Z_copy");
    assertNull(captured.copySource(),
        "copySource must not be set — ONTAP rejects %2F-encoded slashes just like Ceph");
    assertTrue(!captured.sourceKey().contains("%2F"),
        "sourceKey must not contain %2F — ONTAP interprets these literally");
  }

  /**
   * Verifies touch() also works correctly for NetApp ONTAP paths.
   * Touch copies an object onto itself with updated metadata — the same
   * %2F encoding bug affected this code path too.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testNetappOntapTouchCompatibility()
      throws Exception {
    HeadObjectResponse oldHead = mock(HeadObjectResponse.class);
    when(oldHead.lastModified()).thenReturn(Instant.ofEpochMilli(1000L));
    HeadObjectResponse newHead = mock(HeadObjectResponse.class);
    when(newHead.lastModified()).thenReturn(Instant.ofEpochMilli(2000L));
    when(_s3Client.headObject(any(HeadObjectRequest.class)))
        .thenReturn(oldHead)
        .thenReturn(newHead);
    mockSuccessfulCopy();

    URI uri = URI.create("s3://ontap-pinot-store/prod/tables/events_REALTIME/segment__0__2__20240315T1200Z");
    boolean result = _s3PinotFS.touch(uri);
    assertTrue(result);

    CopyObjectRequest captured = captureCopyRequest();
    assertEquals(captured.sourceBucket(), "ontap-pinot-store");
    assertEquals(captured.sourceKey(), "prod/tables/events_REALTIME/segment__0__2__20240315T1200Z");
    assertEquals(captured.destinationBucket(), "ontap-pinot-store");
    assertEquals(captured.destinationKey(), "prod/tables/events_REALTIME/segment__0__2__20240315T1200Z");
    assertNull(captured.copySource(),
        "copySource must not be set — touch on ONTAP must use sourceBucket/sourceKey");
    assertTrue(!captured.sourceKey().contains("%2F"));
    assertTrue(captured.metadata().containsKey("lastModified"));
  }
}
