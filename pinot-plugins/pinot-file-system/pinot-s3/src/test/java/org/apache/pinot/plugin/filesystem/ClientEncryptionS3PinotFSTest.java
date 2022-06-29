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

import cloud.localstack.Localstack;
import cloud.localstack.ServiceName;
import cloud.localstack.docker.annotation.LocalstackDockerAnnotationProcessor;
import cloud.localstack.docker.annotation.LocalstackDockerConfiguration;
import cloud.localstack.docker.annotation.LocalstackDockerProperties;
import cloud.localstack.docker.command.Command;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.CreateKeyRequest;
import com.amazonaws.services.kms.model.CreateKeyResult;
import com.amazonaws.services.kms.model.KeySpec;
import com.amazonaws.services.kms.model.KeyUsageType;
import com.amazonaws.services.s3.AmazonS3EncryptionClientV2Builder;
import com.amazonaws.services.s3.AmazonS3EncryptionV2;
import com.amazonaws.services.s3.model.CryptoConfigurationV2;
import com.amazonaws.services.s3.model.CryptoMode;
import com.amazonaws.services.s3.model.KMSEncryptionMaterials;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


@Test
@LocalstackDockerProperties(services = {ServiceName.S3, ServiceName.KMS}, imageTag = "0.12.15")
public class ClientEncryptionS3PinotFSTest {
  private static final String DELIMITER = "/";
  private static final String BUCKET = "test-bucket";
  private static final String SCHEME = "s3";
  private static final String FILE_FORMAT = "%s://%s/%s";
  private static final String DIR_FORMAT = "%s://%s";

  public static final String REGION = "us-east-1";
  public static final String LOCALSTACK_ENDPOINT = "http://localhost:4566";

  private static final LocalstackDockerAnnotationProcessor PROCESSOR = new LocalstackDockerAnnotationProcessor();
  private final Localstack _localstackDocker = Localstack.INSTANCE;

  private ClientEncryptionS3PinotFS _s3PinotFS;
  private AmazonS3EncryptionV2 _s3Client;

  @BeforeClass
  public void setUp()
      throws Exception {
    final LocalstackDockerConfiguration dockerConfig = PROCESSOR.process(this.getClass());
    StopAllLocalstackDockerCommand stopAllLocalstackDockerCommand = new StopAllLocalstackDockerCommand();
    stopAllLocalstackDockerCommand.execute();
    _localstackDocker.startup(dockerConfig);

    AWSKMS kmsClient = AWSKMSClientBuilder.standard()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(LOCALSTACK_ENDPOINT, REGION))
        .withCredentials(getLocalAWSCredentials())
        .build();

    CreateKeyResult createKeyResult = kmsClient.createKey(
        new CreateKeyRequest().withKeyUsage(KeyUsageType.ENCRYPT_DECRYPT).withKeySpec(KeySpec.SYMMETRIC_DEFAULT));

    _s3Client = AmazonS3EncryptionClientV2Builder.standard().withClientConfiguration(new ClientConfiguration())
        .withCredentials(getLocalAWSCredentials())
        .withCryptoConfiguration(new CryptoConfigurationV2().withCryptoMode(CryptoMode.StrictAuthenticatedEncryption))
        .withKmsClient(kmsClient).withPathStyleAccessEnabled(true).withEncryptionMaterialsProvider(
            new KMSEncryptionMaterialsProvider(new KMSEncryptionMaterials(createKeyResult.getKeyMetadata().getKeyId())))
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(LOCALSTACK_ENDPOINT, REGION)).build();

    _s3PinotFS = new ClientEncryptionS3PinotFS();
    _s3PinotFS.init(_s3Client);
    _s3Client.createBucket(BUCKET);
  }

  private static AWSCredentialsProvider getLocalAWSCredentials() {
    return new AWSStaticCredentialsProvider(new BasicAWSCredentials("access", "secret"));
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    if (_localstackDocker.isRunning()) {
      _localstackDocker.stop();
    }
    _s3PinotFS.close();
  }

  private void createEmptyFile(String folderName, String fileName) {
    String fileNameWithFolder = "";
    if (StringUtils.isNotEmpty(folderName)) {
      fileNameWithFolder = folderName + DELIMITER + fileName;
    } else {
      fileNameWithFolder = fileName;
    }

    _s3Client.putObject(BUCKET, fileNameWithFolder, "foo_bar");
  }

  @Test
  public void testTouchFileInBucket()
      throws Exception {

    String[] originalFiles = new String[]{"a-touch.txt", "b-touch.txt", "c-touch.txt"};

    for (String fileName : originalFiles) {
      _s3PinotFS.touch(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, fileName)));
    }

    ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request();
    listObjectsV2Request.withBucketName(BUCKET);
    ListObjectsV2Result listObjectsV2Result = _s3Client.listObjectsV2(listObjectsV2Request);
    String[] response =
        listObjectsV2Result.getObjectSummaries().stream().map(S3ObjectSummary::getKey).filter(x -> x.contains("touch"))
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

    ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request();
    listObjectsV2Request.withBucketName(BUCKET);
    listObjectsV2Request.withPrefix(folder);
    ListObjectsV2Result listObjectsV2Result = _s3Client.listObjectsV2(listObjectsV2Request);
    String[] response =
        listObjectsV2Result.getObjectSummaries().stream().map(S3ObjectSummary::getKey).filter(x -> x.contains("touch"))
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

    actualFiles = Arrays.stream(actualFiles).filter(x -> x.contains("list")).toArray(String[]::new);
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

    Assert.assertTrue(Arrays.equals(Arrays.stream(originalFiles)
            .map(fileName -> String.format(FILE_FORMAT, SCHEME, BUCKET, folder + DELIMITER + fileName)).toArray(),
        actualFiles));
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

    ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request();
    listObjectsV2Request.withBucketName(BUCKET);
    ListObjectsV2Result listObjectsV2Result = _s3Client.listObjectsV2(listObjectsV2Request);
    String[] actualResponse =
        listObjectsV2Result.getObjectSummaries().stream().map(S3ObjectSummary::getKey).filter(x -> x.contains("delete"))
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

    ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request();
    listObjectsV2Request.withBucketName(BUCKET);
    ListObjectsV2Result listObjectsV2Result = _s3Client.listObjectsV2(listObjectsV2Request);
    String[] actualResponse = listObjectsV2Result.getObjectSummaries().stream().map(S3ObjectSummary::getKey)
        .filter(x -> x.contains("delete-2")).toArray(String[]::new);

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
    boolean isDirChild = _s3PinotFS.isDirectory(
        URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folder + DELIMITER + childFolder)));
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

    File fileToDownload = new File("copyFile_download.txt").getAbsoluteFile();
    _s3PinotFS.copyToLocalFile(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, fileName)), fileToDownload);
    Assert.assertEquals(fileToCopy.length(), fileToDownload.length());

    fileToDownload.deleteOnExit();
  }

  @Test
  public void testOpenFile()
      throws Exception {
    String fileName = "sample.txt";
    String fileContent = "Hello, World";

    _s3Client.putObject(BUCKET, fileName, fileContent);

    InputStream is = _s3PinotFS.open(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, fileName)));
    String actualContents = IOUtils.toString(is, StandardCharsets.UTF_8);
    Assert.assertEquals(actualContents, fileContent);
  }

  @Test
  public void testMkdir()
      throws Exception {
    String folderName = "my-test-folder";

    _s3PinotFS.mkdir(URI.create(String.format(FILE_FORMAT, SCHEME, BUCKET, folderName)));

    try {
      ObjectMetadata objectMetadata = _s3Client.getObjectMetadata(BUCKET, folderName + DELIMITER);
      Assert.assertEquals(objectMetadata.getContentLength(), 16);
    } catch (Exception e) {
      Assert.fail();
    }
  }

  public static class StopAllLocalstackDockerCommand extends Command {

    public void execute() {
      String runningDockerContainers =
          dockerExe.execute(Arrays.asList("ps", "-a", "-q", "-f", "ancestor=localstack/localstack"));
      if (StringUtils.isNotBlank(runningDockerContainers) && !runningDockerContainers.toLowerCase().contains("error")) {
        String[] containerList = runningDockerContainers.split("\n");

        for (String containerId : containerList) {
          dockerExe.execute(Arrays.asList("stop", containerId));
        }
      }
    }
  }

  public static class DockerInfoCommand extends Command {

    public void execute() {
      String dockerInfo = dockerExe.execute(Collections.singletonList("info"));

      if (dockerInfo.toLowerCase().contains("error")) {
        throw new IllegalStateException("Docker daemon is not running!");
      }
    }
  }
}
