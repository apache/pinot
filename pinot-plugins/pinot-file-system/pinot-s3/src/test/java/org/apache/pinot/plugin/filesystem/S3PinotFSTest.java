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

import com.adobe.testing.s3mock.testng.S3Mock;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;


@Test
@Listeners(com.adobe.testing.s3mock.testng.S3MockListener.class)
public class S3PinotFSTest {
  final String DELIMITER = "/";
  S3PinotFS _s3PinotFS;
  S3Client _s3Client;
  final String BUCKET = "test-bucket";
  final String SCHEME = "s3";
  final String FILE_FORMAT = "%s://%s/%s";
  final String DIR_FORMAT = "%s://%s";

  @BeforeClass
  public void setUp() {
    S3Mock s3Mock = S3Mock.getInstance();
    _s3Client = s3Mock.createS3ClientV2();
    _s3PinotFS = new S3PinotFS();
    _s3PinotFS.init(_s3Client);
    _s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _s3PinotFS.close();
    _s3Client.close();
  }

  private void createEmptyFile(String folderName, String fileName) {
    String fileNameWithFolder = folderName + DELIMITER + fileName;
    _s3Client
        .putObject(S3TestUtils.getPutObjectRequest(BUCKET, fileNameWithFolder), RequestBody.fromBytes(new byte[0]));
  }

  @Test
  public void testTouchFileInBucket()
      throws Exception {

    String[] originalFiles = new String[]{"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      _s3PinotFS.touch(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, fileName)));
    }
    ListObjectsV2Response listObjectsV2Response =
        _s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(BUCKET, "", true));

    String[] response = listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("touch"))
        .toArray(String[]::new);

    Assert.assertEquals(response.length, originalFiles.length);
    Assert.assertTrue(Arrays.equals(response, originalFiles));
  }

  @Test
  public void testTouchFilesInFolder()
      throws Exception {

    String folder = "my-files";
    String[] originalFiles = new String[]{"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      String fileNameWithFolder = folder + DELIMITER + fileName;
      _s3PinotFS.touch(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, fileNameWithFolder)));
    }
    ListObjectsV2Response listObjectsV2Response =
        _s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(BUCKET, folder, false));

    String[] response = listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("touch"))
        .toArray(String[]::new);
    Assert.assertEquals(response.length, originalFiles.length);

    Assert.assertTrue(Arrays.equals(response, Arrays.stream(originalFiles).map(x -> folder + DELIMITER + x).toArray()));
  }

  @Test
  public void testListFilesInBucketNonRecursive()
      throws Exception {
    String[] originalFiles = new String[]{"a-list.txt", "b-list.txt", "c-list.txt"};
    List<String> expectedFileNames = new ArrayList<>();

    for (String fileName : originalFiles) {
      createEmptyFile("", fileName);
      expectedFileNames.add(String.format(FILE_FORMAT, SCHEME, BUCKET, fileName));
    }

    String[] actualFiles = _s3PinotFS.listFiles(URI.create(String.format(DIR_FORMAT, SCHEME, BUCKET)), false);

    actualFiles =
        Arrays.stream(actualFiles).filter(x -> x.contains("list")).toArray(String[]::new);
    Assert.assertEquals(actualFiles.length, originalFiles.length);

    Assert.assertTrue(Arrays.equals(actualFiles, expectedFileNames.toArray()));
  }

  @Test
  public void testListFilesInFolderNonRecursive()
      throws Exception {
    String folder = "list-files";
    String[] originalFiles = new String[]{"a-list-2.txt", "b-list-2.txt", "c-list-2.txt"};

    for (String fileName : originalFiles) {
      createEmptyFile(folder, fileName);
    }

    String[] actualFiles = _s3PinotFS.listFiles(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folder)), false);

    actualFiles = Arrays.stream(actualFiles).filter(x -> x.contains("list-2")).toArray(String[]::new);
    Assert.assertEquals(actualFiles.length, originalFiles.length);

    Assert.assertTrue(
        Arrays.equals(Arrays.stream(originalFiles).map(fileName -> String.format(FILE_FORMAT, SCHEME, BUCKET, folder + DELIMITER + fileName)).toArray(), actualFiles));
  }

  @Test
  public void testListFilesInFolderRecursive()
      throws Exception {
    String folder = "list-files-rec";
    String[] nestedFolders = new String[]{"list-files-child-1", "list-files-child-2"};
    String[] originalFiles = new String[]{"a-list-3.txt", "b-list-3.txt", "c-list-3.txt"};

    List<String> expectedResultList = new ArrayList<>();
    for (String childFolder : nestedFolders) {
      String folderName = folder + DELIMITER + childFolder;
      for (String fileName : originalFiles) {
        createEmptyFile(folderName, fileName);
        expectedResultList.add(String.format(FILE_FORMAT, SCHEME, BUCKET, folderName + DELIMITER + fileName));
      }
    }
    String[] actualFiles = _s3PinotFS.listFiles(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folder)), true);

    actualFiles = Arrays.stream(actualFiles).filter(x -> x.contains("list-3")).toArray(String[]::new);
    Assert.assertEquals(actualFiles.length, expectedResultList.size());
    Assert.assertTrue(Arrays.equals(expectedResultList.toArray(), actualFiles));
  }

  @Test
  public void testDeleteFile()
      throws Exception {
    String[] originalFiles = new String[]{"a-delete.txt", "b-delete.txt", "c-delete.txt"};
    String fileToDelete = "a-delete.txt";

    List<String> expectedResultList = new ArrayList<>();
    for (String fileName : originalFiles) {
      createEmptyFile("", fileName);
      if (!fileName.equals(fileToDelete)) {
        expectedResultList.add(fileName);
      }
    }

    _s3PinotFS.delete(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, fileToDelete)), false);

    ListObjectsV2Response listObjectsV2Response =
        _s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(BUCKET, "", true));
    String[] actualResponse =
        listObjectsV2Response.contents().stream().map(x -> x.key().substring(1)).filter(x -> x.contains("delete"))
            .toArray(String[]::new);

    Assert.assertEquals(actualResponse.length, 2);
    Assert.assertTrue(Arrays.equals(actualResponse, expectedResultList.toArray()));
  }

  @Test
  public void testDeleteFolder()
      throws Exception {
    String[] originalFiles = new String[]{"a-delete-2.txt", "b-delete-2.txt", "c-delete-2.txt"};
    String folderName = "my-files";

    for (String fileName : originalFiles) {
      createEmptyFile(folderName, fileName);
    }

    _s3PinotFS.delete(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folderName)), true);

    ListObjectsV2Response listObjectsV2Response =
        _s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(BUCKET, "", true));
    String[] actualResponse =
        listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("delete-2"))
            .toArray(String[]::new);

    Assert.assertEquals(0, actualResponse.length);
  }

  @Test
  public void testIsDirectory()
      throws Exception {
    String[] originalFiles = new String[]{"a-dir.txt", "b-dir.txt", "c-dir.txt"};
    String folder = "my-files-dir";
    String childFolder = "my-files-dir-child";
    for (String fileName : originalFiles) {
      String folderName = folder + DELIMITER + childFolder;
      createEmptyFile(folderName, fileName);
    }

    boolean isBucketDir = _s3PinotFS.isDirectory(URI.create(String.format(DIR_FORMAT, SCHEME, BUCKET)));
    boolean isDir = _s3PinotFS.isDirectory(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folder)));
    boolean isDirChild = _s3PinotFS
        .isDirectory(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folder + DELIMITER + childFolder)));
    boolean notIsDir = _s3PinotFS.isDirectory(URI.create(
        String.format(FILE_FORMAT, SCHEME, BUCKET, folder + DELIMITER + childFolder + DELIMITER + "a-delete.txt")));

    Assert.assertTrue(isBucketDir);
    Assert.assertTrue(isDir);
    Assert.assertTrue(isDirChild);
    Assert.assertFalse(notIsDir);
  }

  @Test
  public void testExists()
      throws Exception {
    String[] originalFiles = new String[]{"a-ex.txt", "b-ex.txt", "c-ex.txt"};
    String folder = "my-files-dir";
    String childFolder = "my-files-dir-child";

    for (String fileName : originalFiles) {
      String folderName = folder + DELIMITER + childFolder;
      createEmptyFile(folderName, fileName);
    }

    boolean bucketExists = _s3PinotFS.exists(URI.create(String.format(DIR_FORMAT, SCHEME, BUCKET)));
    boolean dirExists = _s3PinotFS.exists(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folder)));
    boolean childDirExists =
        _s3PinotFS.exists(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folder + DELIMITER + childFolder)));
    boolean fileExists = _s3PinotFS.exists(URI.create(
        String.format(FILE_FORMAT, SCHEME, BUCKET, folder + DELIMITER + childFolder + DELIMITER + "a-ex.txt")));
    boolean fileNotExists = _s3PinotFS.exists(URI.create(
        String.format(FILE_FORMAT, SCHEME, BUCKET, folder + DELIMITER + childFolder + DELIMITER + "d-ex.txt")));

    Assert.assertTrue(bucketExists);
    Assert.assertTrue(dirExists);
    Assert.assertTrue(childDirExists);
    Assert.assertTrue(fileExists);
    Assert.assertFalse(fileNotExists);
  }

  @Test
  public void testCopyFromAndToLocal()
      throws Exception {
    String fileName = "copyFile.txt";

    File fileToCopy = new File(getClass().getClassLoader().getResource(fileName).getFile());

    _s3PinotFS.copyFromLocalFile(fileToCopy, URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, fileName)));

    HeadObjectResponse headObjectResponse = _s3Client.headObject(S3TestUtils.getHeadObjectRequest(BUCKET, fileName));

    Assert.assertEquals(headObjectResponse.contentLength(), (Long) fileToCopy.length());

    File fileToDownload = new File("copyFile_download.txt");
    _s3PinotFS.copyToLocalFile(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, fileName)), fileToDownload);
    Assert.assertEquals(fileToCopy.length(), fileToDownload.length());

    fileToDownload.deleteOnExit();
  }

  @Test
  public void testOpenFile()
      throws Exception {
    String fileName = "sample.txt";
    String fileContent = "Hello, World";

    _s3Client.putObject(S3TestUtils.getPutObjectRequest(BUCKET, fileName), RequestBody.fromString(fileContent));

    InputStream is = _s3PinotFS.open(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, fileName)));
    String actualContents = IOUtils.toString(is, StandardCharsets.UTF_8);
    Assert.assertEquals(actualContents, fileContent);
  }

  @Test
  public void testMkdir()
      throws Exception {
    String folderName = "my-test-folder";

    _s3PinotFS.mkdir(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folderName)));

    HeadObjectResponse headObjectResponse = _s3Client.headObject(S3TestUtils.getHeadObjectRequest(BUCKET, folderName));
    Assert.assertTrue(headObjectResponse.sdkHttpResponse().isSuccessful());
  }

}
