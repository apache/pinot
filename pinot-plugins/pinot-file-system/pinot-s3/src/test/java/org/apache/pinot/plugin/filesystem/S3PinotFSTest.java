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

import com.adobe.testing.s3mock.testcontainers.S3MockContainer;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.StorageClass;


@Test
public class S3PinotFSTest {
  private static final String S3MOCK_VERSION = System.getProperty("s3mock.version", "2.12.2");
  private static final File TEMP_FILE = new File(FileUtils.getTempDirectory(), "S3PinotFSTest");

  private static final String S3_SCHEME = "s3";
  private static final String S3A_SCHEME = "s3a";
  private static final String DELIMITER = "/";
  private static final String BUCKET = "test-bucket";
  private static final String FILE_FORMAT = "%s://%s/%s";
  private static final String DIR_FORMAT = "%s://%s";

  private S3MockContainer _s3MockContainer;
  private S3PinotFS _s3PinotFS;
  private S3Client _s3Client;

  @DataProvider(name = "scheme")
  public static Object[][] schemes() {
    return new Object[][]{{S3_SCHEME}, {S3A_SCHEME}};
  }

  @BeforeClass
  public void setUp() {
    _s3MockContainer = new S3MockContainer(S3MOCK_VERSION);
    _s3MockContainer.start();
    String endpoint = _s3MockContainer.getHttpEndpoint();
    _s3Client = createS3ClientV2(endpoint);
    _s3PinotFS = new S3PinotFS();
    _s3PinotFS.init(_s3Client);
    _s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _s3PinotFS.close();
    _s3Client.close();
    _s3MockContainer.stop();
    FileUtils.deleteQuietly(TEMP_FILE);
  }

  @BeforeMethod
  public void beforeMethod() {
    _s3PinotFS.setStorageClass(null);
  }

  private void createEmptyFile(String folderName, String fileName) {
    String fileNameWithFolder = folderName.length() == 0 ? fileName : folderName + DELIMITER + fileName;
    _s3Client.putObject(S3TestUtils.getPutObjectRequest(BUCKET, fileNameWithFolder, _s3PinotFS.getStorageClass()),
        RequestBody.fromBytes(new byte[0]));
  }

  @Test(dataProvider = "scheme")
  public void testTouchFileInBucket(String scheme)
      throws Exception {

    String[] originalFiles = new String[]{"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      _s3PinotFS.touch(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, fileName)));
    }
    ListObjectsV2Response listObjectsV2Response =
        _s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(BUCKET, "", true));

    String[] response = listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("touch"))
        .toArray(String[]::new);

    Assert.assertEquals(response.length, originalFiles.length);
    Assert.assertTrue(Arrays.equals(response, originalFiles));
  }

  @Test(dataProvider = "scheme")
  public void testTouchFilesInFolder(String scheme)
      throws Exception {

    String folder = "my-files";
    String[] originalFiles = new String[]{"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      String fileNameWithFolder = folder + DELIMITER + fileName;
      _s3PinotFS.touch(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, fileNameWithFolder)));
    }
    ListObjectsV2Response listObjectsV2Response =
        _s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(BUCKET, folder, false));

    String[] response = listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("touch"))
        .toArray(String[]::new);
    Assert.assertEquals(response.length, originalFiles.length);

    Assert.assertTrue(Arrays.equals(response, Arrays.stream(originalFiles).map(x -> folder + DELIMITER + x).toArray()));
  }

  @Test(dataProvider = "scheme")
  public void testListFilesInBucketNonRecursive(String scheme)
      throws Exception {
    String[] originalFiles = new String[]{"a-list.txt", "b-list.txt", "c-list.txt"};
    List<String> expectedFileNames = new ArrayList<>();

    for (String fileName : originalFiles) {
      createEmptyFile("", fileName);
      expectedFileNames.add(String.format(FILE_FORMAT, scheme, BUCKET, fileName));
    }

    String[] actualFiles = _s3PinotFS.listFiles(URI.create(String.format(DIR_FORMAT, scheme, BUCKET)), false);

    actualFiles = Arrays.stream(actualFiles).filter(x -> x.contains("list")).toArray(String[]::new);
    Assert.assertEquals(actualFiles.length, originalFiles.length);

    Assert.assertTrue(Arrays.equals(actualFiles, expectedFileNames.toArray()));
  }

  @Test(dataProvider = "scheme")
  public void testListFilesInFolderNonRecursive(String scheme)
      throws Exception {
    String folder = "list-files";
    String[] originalFiles = new String[]{"a-list-2.txt", "b-list-2.txt", "c-list-2.txt"};
    List<String> expectedFileNames = new ArrayList<>();

    for (String fileName : originalFiles) {
      createEmptyFile(folder, fileName);
      expectedFileNames.add(String.format(FILE_FORMAT, scheme, BUCKET, folder + DELIMITER + fileName));
    }

    String[] subfolders = new String[]{"subfolder1", "subfolder2"};
    for (String subfolder : subfolders) {
      createEmptyFile(folder + DELIMITER + subfolder, "a-sub-file.txt");
      expectedFileNames.add(String.format(FILE_FORMAT, scheme, BUCKET, folder + DELIMITER + subfolder));
    }

    String[] actualFiles = _s3PinotFS.listFiles(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folder)), false);
    Assert.assertEquals(actualFiles, expectedFileNames.toArray());
  }

  @Test(dataProvider = "scheme")
  public void testListFilesInFolderRecursive(String scheme)
      throws Exception {
    String folder = "list-files-rec";
    String[] nestedFolders = new String[]{"", "list-files-child-1", "list-files-child-2"};
    String[] originalFiles = new String[]{"a-list-3.txt", "b-list-3.txt", "c-list-3.txt"};

    List<String> expectedResultList = new ArrayList<>();
    for (String childFolder : nestedFolders) {
      String folderName = folder + (childFolder.length() == 0 ? "" : DELIMITER + childFolder);
      for (String fileName : originalFiles) {
        createEmptyFile(folderName, fileName);
        expectedResultList.add(String.format(FILE_FORMAT, scheme, BUCKET, folderName + DELIMITER + fileName));
      }
    }
    String[] actualFiles = _s3PinotFS.listFiles(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folder)), true);
    Assert.assertEquals(actualFiles.length, expectedResultList.size());
    actualFiles = Arrays.stream(actualFiles).filter(x -> x.contains("list-3")).toArray(String[]::new);
    Assert.assertEquals(actualFiles.length, expectedResultList.size());
    Assert.assertTrue(Arrays.equals(expectedResultList.toArray(), actualFiles));
  }

  @Test(dataProvider = "scheme")
  public void testListFilesWithMetadataInFolderNonRecursive(String scheme)
      throws Exception {
    String folder = "list-files-with-md";
    String[] originalFiles = new String[]{"a-list-2.txt", "b-list-2.txt", "c-list-2.txt"};
    List<String> expectedFilePaths = new ArrayList<>();
    List<Boolean> expectedIsDirectories = new ArrayList<>();

    for (String fileName : originalFiles) {
      createEmptyFile(folder, fileName);
      expectedFilePaths.add(String.format(FILE_FORMAT, scheme, BUCKET, folder + DELIMITER + fileName));
      expectedIsDirectories.add(false);
    }

    String[] subfolders = new String[]{"subfolder1", "subfolder2"};
    for (String subfolder : subfolders) {
      createEmptyFile(folder + DELIMITER + subfolder, "a-sub-file.txt");
      expectedFilePaths.add(String.format(FILE_FORMAT, scheme, BUCKET, folder + DELIMITER + subfolder));
      expectedIsDirectories.add(true);
    }

    List<FileMetadata> actualFiles =
        _s3PinotFS.listFilesWithMetadata(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folder)), false);

    List<String> actualFilePaths = actualFiles.stream().map(FileMetadata::getFilePath).collect(Collectors.toList());
    List<Boolean> actualIsDirectories = actualFiles.stream().map(FileMetadata::isDirectory)
        .collect(Collectors.toList());
    Assert.assertEquals(actualFilePaths, expectedFilePaths);
    Assert.assertEquals(actualIsDirectories, expectedIsDirectories);
  }

  @Test(dataProvider = "scheme")
  public void testListFilesWithMetadataInFolderRecursive(String scheme)
      throws Exception {
    String folder = "list-files-rec-with-md";
    String[] nestedFolders = new String[]{"", "list-files-child-1", "list-files-child-2"};
    String[] originalFiles = new String[]{"a-list-3.txt", "b-list-3.txt", "c-list-3.txt"};

    List<String> expectedResultList = new ArrayList<>();
    for (String childFolder : nestedFolders) {
      String folderName = folder + (childFolder.length() == 0 ? "" : DELIMITER + childFolder);
      for (String fileName : originalFiles) {
        createEmptyFile(folderName, fileName);
        expectedResultList.add(String.format(FILE_FORMAT, scheme, BUCKET, folderName + DELIMITER + fileName));
      }
    }
    List<FileMetadata> actualFiles =
        _s3PinotFS.listFilesWithMetadata(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folder)), true);
    Assert.assertEquals(actualFiles.size(), expectedResultList.size());
    List<String> actualFilePaths =
        actualFiles.stream().map(FileMetadata::getFilePath).filter(fp -> fp.contains("list-3"))
            .collect(Collectors.toList());
    Assert.assertEquals(actualFilePaths.size(), expectedResultList.size());
    Assert.assertEquals(expectedResultList, actualFilePaths);
  }

  @Test(dataProvider = "scheme")
  public void testDeleteFile(String scheme)
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

    boolean deleteResult = _s3PinotFS.delete(
        URI.create(String.format(FILE_FORMAT, scheme, BUCKET, fileToDelete)), false);

    Assert.assertTrue(deleteResult);

    ListObjectsV2Response listObjectsV2Response =
        _s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(BUCKET, "", true));
    String[] actualResponse =
        listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("delete"))
            .toArray(String[]::new);

    Assert.assertEquals(actualResponse.length, 2);
    Assert.assertTrue(Arrays.equals(actualResponse, expectedResultList.toArray()));
  }

  @Test(dataProvider = "scheme")
  public void testDeleteFolder(String scheme)
      throws Exception {
    String[] originalFiles = new String[]{"a-delete-2.txt", "b-delete-2.txt", "c-delete-2.txt"};
    String folderName = "my-files";

    for (String fileName : originalFiles) {
      createEmptyFile(folderName, fileName);
    }

    boolean deleteResult = _s3PinotFS.delete(
        URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folderName)), true);

    Assert.assertTrue(deleteResult);

    ListObjectsV2Response listObjectsV2Response =
        _s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(BUCKET, "", true));
    String[] actualResponse =
        listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("delete-2"))
            .toArray(String[]::new);

    Assert.assertEquals(0, actualResponse.length);
  }

  @Test(dataProvider = "scheme")
  public void testIsDirectory(String scheme)
      throws Exception {
    String[] originalFiles = new String[]{"a-dir.txt", "b-dir.txt", "c-dir.txt"};
    String folder = "my-files-dir";
    String childFolder = "my-files-dir-child";
    for (String fileName : originalFiles) {
      String folderName = folder + DELIMITER + childFolder;
      createEmptyFile(folderName, fileName);
    }

    boolean isBucketDir = _s3PinotFS.isDirectory(URI.create(String.format(DIR_FORMAT, scheme, BUCKET)));
    boolean isDir = _s3PinotFS.isDirectory(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folder)));
    boolean isDirChild = _s3PinotFS.isDirectory(
        URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folder + DELIMITER + childFolder)));
    boolean notIsDir = _s3PinotFS.isDirectory(URI.create(
        String.format(FILE_FORMAT, scheme, BUCKET, folder + DELIMITER + childFolder + DELIMITER + "a-delete.txt")));

    Assert.assertTrue(isBucketDir);
    Assert.assertTrue(isDir);
    Assert.assertTrue(isDirChild);
    Assert.assertFalse(notIsDir);
  }

  @Test(dataProvider = "scheme")
  public void testExists(String scheme)
      throws Exception {
    String[] originalFiles = new String[]{"a-ex.txt", "b-ex.txt", "c-ex.txt"};
    String folder = "my-files-dir";
    String childFolder = "my-files-dir-child";

    for (String fileName : originalFiles) {
      String folderName = folder + DELIMITER + childFolder;
      createEmptyFile(folderName, fileName);
    }

    boolean bucketExists = _s3PinotFS.exists(URI.create(String.format(DIR_FORMAT, scheme, BUCKET)));
    boolean dirExists = _s3PinotFS.exists(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folder)));
    boolean childDirExists =
        _s3PinotFS.exists(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folder + DELIMITER + childFolder)));
    boolean fileExists = _s3PinotFS.exists(URI.create(
        String.format(FILE_FORMAT, scheme, BUCKET, folder + DELIMITER + childFolder + DELIMITER + "a-ex.txt")));
    boolean fileNotExists = _s3PinotFS.exists(URI.create(
        String.format(FILE_FORMAT, scheme, BUCKET, folder + DELIMITER + childFolder + DELIMITER + "d-ex.txt")));

    Assert.assertTrue(bucketExists);
    Assert.assertTrue(dirExists);
    Assert.assertTrue(childDirExists);
    Assert.assertTrue(fileExists);
    Assert.assertFalse(fileNotExists);
  }

  @Test(dataProvider = "storageClasses")
  public void testCopyFromAndToLocal(StorageClass storageClass, String scheme)
      throws Exception {

    _s3PinotFS.setStorageClass(storageClass);

    String fileName = "copyFile.txt";
    File fileToCopy = new File(TEMP_FILE, fileName);
    File fileToDownload = new File(TEMP_FILE, "copyFile_download.txt").getAbsoluteFile();
    try {
      createDummyFile(fileToCopy, 1024);
      _s3PinotFS.copyFromLocalFile(fileToCopy, URI.create(String.format(FILE_FORMAT, scheme, BUCKET, fileName)));
      HeadObjectResponse headObjectResponse = _s3Client.headObject(S3TestUtils.getHeadObjectRequest(BUCKET, fileName));
      Assert.assertEquals(headObjectResponse.contentLength(), (Long) fileToCopy.length());
      _s3PinotFS.copyToLocalFile(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, fileName)), fileToDownload);
      Assert.assertEquals(fileToCopy.length(), fileToDownload.length());
    } finally {
      FileUtils.deleteQuietly(fileToCopy);
      FileUtils.deleteQuietly(fileToDownload);
    }
  }

  @Test(dataProvider = "storageClasses")
  public void testMultiPartUpload(StorageClass storageClass, String scheme)
      throws Exception {

    _s3PinotFS.setStorageClass(storageClass);

    String fileName = "copyFile_for_multipart.txt";
    File fileToCopy = new File(TEMP_FILE, fileName);
    File fileToDownload = new File(TEMP_FILE, "copyFile_download_multipart.txt").getAbsoluteFile();
    try {
      // Make a file of 11MB to upload in parts, whose min required size is 5MB.
      createDummyFile(fileToCopy, 11 * 1024 * 1024);
      _s3PinotFS.setMultiPartUploadConfigs(1, 5 * 1024 * 1024);
      try {
        _s3PinotFS.copyFromLocalFile(fileToCopy, URI.create(String.format(FILE_FORMAT, scheme, BUCKET, fileName)));
      } finally {
        // disable multipart upload again for the other UT cases.
        _s3PinotFS.setMultiPartUploadConfigs(-1, 128 * 1024 * 1024);
      }
      HeadObjectResponse headObjectResponse = _s3Client.headObject(S3TestUtils.getHeadObjectRequest(BUCKET, fileName));
      Assert.assertEquals(headObjectResponse.contentLength(), (Long) fileToCopy.length());
      _s3PinotFS.copyToLocalFile(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, fileName)), fileToDownload);
      Assert.assertEquals(fileToCopy.length(), fileToDownload.length());
    } finally {
      FileUtils.deleteQuietly(fileToCopy);
      FileUtils.deleteQuietly(fileToDownload);
    }
  }

  @Test(dataProvider = "scheme")
  public void testOpenFile(String scheme)
      throws Exception {
    String fileName = "sample.txt";
    String fileContent = "Hello, World";

    _s3Client.putObject(
        S3TestUtils.getPutObjectRequest(BUCKET, fileName, _s3PinotFS.getStorageClass()),
        RequestBody.fromString(fileContent));

    InputStream is = _s3PinotFS.open(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, fileName)));
    String actualContents = IOUtils.toString(is, StandardCharsets.UTF_8);
    Assert.assertEquals(actualContents, fileContent);
  }

  @Test(dataProvider = "scheme")
  public void testMkdir(String scheme)
      throws Exception {
    String folderName = "my-test-folder";

    _s3PinotFS.mkdir(URI.create(String.format(FILE_FORMAT, scheme, BUCKET, folderName)));

    HeadObjectResponse headObjectResponse =
        _s3Client.headObject(S3TestUtils.getHeadObjectRequest(BUCKET, folderName + "/"));
    Assert.assertTrue(headObjectResponse.sdkHttpResponse().isSuccessful());
  }

  @Test(dataProvider = "storageClasses")
  public void testMoveFile(StorageClass storageClass, String scheme)
      throws Exception {

    _s3PinotFS.setStorageClass(storageClass);

    String sourceFilename = "source-file-" + System.currentTimeMillis();
    String targetFilename = "target-file-" + System.currentTimeMillis();
    int fileSize = 5000;

    File file = new File(TEMP_FILE, sourceFilename);

    try {
      createDummyFile(file, fileSize);
      URI sourceUri = URI.create(String.format(FILE_FORMAT, scheme, BUCKET, sourceFilename));

      _s3PinotFS.copyFromLocalFile(file, sourceUri);

      HeadObjectResponse sourceHeadObjectResponse =
          _s3Client.headObject(S3TestUtils.getHeadObjectRequest(BUCKET, sourceFilename));

      URI targetUri = URI.create(String.format(FILE_FORMAT, scheme, BUCKET, targetFilename));

      boolean moveResult = _s3PinotFS.move(sourceUri, targetUri, false);
      Assert.assertTrue(moveResult);

      Assert.assertFalse(_s3PinotFS.exists(sourceUri));
      Assert.assertTrue(_s3PinotFS.exists(targetUri));

      HeadObjectResponse targetHeadObjectResponse =
          _s3Client.headObject(S3TestUtils.getHeadObjectRequest(BUCKET, targetFilename));
      Assert.assertEquals(targetHeadObjectResponse.contentLength(),
          fileSize);
      Assert.assertEquals(targetHeadObjectResponse.storageClass(),
          sourceHeadObjectResponse.storageClass());
      Assert.assertEquals(targetHeadObjectResponse.archiveStatus(),
          sourceHeadObjectResponse.archiveStatus());
      Assert.assertEquals(targetHeadObjectResponse.contentType(),
          sourceHeadObjectResponse.contentType());
      Assert.assertEquals(targetHeadObjectResponse.expiresString(),
          sourceHeadObjectResponse.expiresString());
      Assert.assertEquals(targetHeadObjectResponse.eTag(),
          sourceHeadObjectResponse.eTag());
      Assert.assertEquals(targetHeadObjectResponse.replicationStatusAsString(),
          sourceHeadObjectResponse.replicationStatusAsString());
      // Last modified time might not be exactly the same, hence we give 5 seconds buffer.
      Assert.assertTrue(Math.abs(
          targetHeadObjectResponse.lastModified().getEpochSecond() - sourceHeadObjectResponse.lastModified()
              .getEpochSecond()) < 5);
    } finally {
      FileUtils.deleteQuietly(file);
    }
  }

  public void testDeleteBatch()
      throws IOException {
    String[] originalFiles = new String[]{"a-delete-batch.txt", "b-delete-batch.txt", "c-delete-batch.txt"};
    String folderName = "my-files-batch";

    for (String fileName : originalFiles) {
      createEmptyFile(folderName, fileName);
    }
    // Create a sub folder to delete
    String subFolderName = folderName + DELIMITER + "subfolder";
    for (String fileName : new String[] {"subfolder-a-delete-batch.txt", "subfolder-b-delete-batch.txt"}) {
      createEmptyFile(subFolderName, fileName);
    }

    // Create a list of URIs to delete
    List<URI> filesToDelete = Arrays.stream(originalFiles)
        .map(fileName -> URI.create(String.format(FILE_FORMAT, S3_SCHEME, BUCKET, folderName + DELIMITER + fileName)))
        .collect(Collectors.toList());
    filesToDelete.add(URI.create(String.format(FILE_FORMAT, S3_SCHEME, BUCKET, subFolderName)));

    boolean deleteResult = _s3PinotFS.deleteBatch(filesToDelete, true);

    Assert.assertTrue(deleteResult);

    ListObjectsV2Response listObjectsV2Response =
        _s3Client.listObjectsV2(S3TestUtils.getListObjectRequest(BUCKET, "", true));
    String[] actualResponse =
        listObjectsV2Response.contents().stream().map(S3Object::key).filter(x -> x.contains("delete-batch"))
            .toArray(String[]::new);

    Assert.assertEquals(actualResponse.length, 0);
  }

  @DataProvider(name = "storageClasses")
  public Object[][] createStorageClasses() {
    return new Object[][]{
        {null, S3_SCHEME},
        {StorageClass.STANDARD, S3_SCHEME},
        {StorageClass.INTELLIGENT_TIERING, S3_SCHEME},
        {null, S3A_SCHEME},
        {StorageClass.STANDARD, S3A_SCHEME},
        {StorageClass.INTELLIGENT_TIERING, S3A_SCHEME},
    };
  }

  private static void createDummyFile(File file, int size)
      throws IOException {
    FileUtils.deleteQuietly(file);
    FileUtils.touch(file);
    try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
      for (int i = 0; i < size; i++) {
        out.write((byte) i);
      }
    }
  }

  private static S3Client createS3ClientV2(String endpoint) {
    return S3Client.builder().region(Region.of("us-east-1"))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .endpointOverride(URI.create(endpoint))
        .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
        .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
        .build();
  }
}
