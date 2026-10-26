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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class S3PinotFSMultipartUploadTest {
  private static final URI DST_URI = URI.create("s3://bucket/dir/file.tar.gz");
  private static final int PART_SIZE = 1000;

  private File _srcFile;
  private byte[] _content;
  private S3Client _s3Client;
  private S3PinotFS _s3PinotFS;
  private final Map<Integer, byte[]> _uploadedParts = new ConcurrentHashMap<>();

  @BeforeMethod
  public void setUp()
      throws Exception {
    // 10 full parts and a short last one.
    _content = new byte[10 * PART_SIZE + 123];
    new Random(7).nextBytes(_content);
    _srcFile = Files.createTempFile("s3-multipart", ".bin").toFile();
    FileUtils.writeByteArrayToFile(_srcFile, _content);
    _uploadedParts.clear();
    _s3Client = mock(S3Client.class);
    when(_s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(
        CreateMultipartUploadResponse.builder().uploadId("upload-1").build());
    _s3PinotFS = new S3PinotFS();
    _s3PinotFS.init(_s3Client);
    _s3PinotFS.setMultiPartUploadConfigs(1, PART_SIZE);
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(_srcFile);
  }

  @DataProvider
  public Object[][] parallelism() {
    return new Object[][]{{1}, {4}, {32}};
  }

  @Test(dataProvider = "parallelism")
  public void testPartsCoverFileInOrder(int parallelism)
      throws Exception {
    _s3PinotFS.setMultiPartUploadParallelism(parallelism);
    when(_s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(invocation -> {
      UploadPartRequest request = invocation.getArgument(0);
      _uploadedParts.put(request.partNumber(), readBody(invocation.getArgument(1)));
      return UploadPartResponse.builder().eTag("etag-" + request.partNumber()).build();
    });

    _s3PinotFS.copyFromLocalFile(_srcFile, DST_URI);

    ArgumentCaptor<CompleteMultipartUploadRequest> captor =
        ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
    verify(_s3Client).completeMultipartUpload(captor.capture());
    List<CompletedPart> parts = captor.getValue().multipartUpload().parts();
    assertEquals(parts.size(), 11);
    ByteArrayOutputStream joined = new ByteArrayOutputStream();
    for (int i = 0; i < parts.size(); i++) {
      assertEquals(parts.get(i).partNumber(), i + 1);
      assertEquals(parts.get(i).eTag(), "etag-" + (i + 1));
      joined.write(_uploadedParts.get(i + 1));
    }
    assertEquals(joined.toByteArray(), _content);
    verify(_s3Client, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
  }

  @Test(dataProvider = "parallelism")
  public void testFailedPartAbortsUpload(int parallelism) {
    _s3PinotFS.setMultiPartUploadParallelism(parallelism);
    when(_s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(invocation -> {
      UploadPartRequest request = invocation.getArgument(0);
      if (request.partNumber() == 5) {
        throw S3Exception.builder().statusCode(500).message("boom").build();
      }
      return UploadPartResponse.builder().eTag("etag-" + request.partNumber()).build();
    });

    assertThrows(S3Exception.class, () -> _s3PinotFS.copyFromLocalFile(_srcFile, DST_URI));
    verify(_s3Client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    verify(_s3Client, never()).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
  }

  private static byte[] readBody(RequestBody body)
      throws Exception {
    try (InputStream in = body.contentStreamProvider().newStream()) {
      return in.readAllBytes();
    }
  }
}
