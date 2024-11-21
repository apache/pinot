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
package org.apache.pinot.controller.api.resources;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.upload.SegmentMetadataInfo;
import org.apache.pinot.spi.crypt.NoOpPinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class PinotSegmentUploadDownloadRestletResourceTest {

  private static final String TABLE_NAME = "table_abc";
  private static final String SEGMENT_NAME = "segment_xyz";

  private PinotSegmentUploadDownloadRestletResource _resource = new PinotSegmentUploadDownloadRestletResource();
  private File _encryptedFile;
  private File _decryptedFile;
  private File _tempDir;

  @BeforeMethod
  public void setUp() throws IOException {
    _tempDir = new File(FileUtils.getTempDirectory(), "test-" + UUID.randomUUID());
    FileUtils.forceMkdir(_tempDir);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @BeforeClass
  public void setup()
      throws Exception {

    // create temp files
    _encryptedFile = File.createTempFile("segment", ".enc");
    _decryptedFile = File.createTempFile("segment", ".dec");
    _encryptedFile.deleteOnExit();
    _decryptedFile.deleteOnExit();

    // configure pinot crypter
    Map<String, Object> properties = new HashMap<>();
    properties.put("class.nooppinotcrypter", NoOpPinotCrypter.class.getName());
    PinotCrypterFactory.init(new PinotConfiguration(properties));
  }

  @Test
  public void testEncryptSegmentIfNeededCrypterInTableConfig() {

    // arrange
    boolean uploadedSegmentIsEncrypted = false;
    String crypterClassNameInTableConfig = "NoOpPinotCrypter";
    String crypterClassNameUsedInUploadedSegment = null;

    // act
    Pair<String, File> encryptionInfo = _resource
        .encryptSegmentIfNeeded(_decryptedFile, _encryptedFile, uploadedSegmentIsEncrypted,
            crypterClassNameUsedInUploadedSegment, crypterClassNameInTableConfig, SEGMENT_NAME, TABLE_NAME);

    // assert
    assertEquals("NoOpPinotCrypter", encryptionInfo.getLeft());
    assertEquals(_encryptedFile, encryptionInfo.getRight());
  }

  @Test
  public void testEncryptSegmentIfNeededUploadedSegmentIsEncrypted() {

    // arrange
    boolean uploadedSegmentIsEncrypted = true;
    String crypterClassNameInTableConfig = "NoOpPinotCrypter";
    String crypterClassNameUsedInUploadedSegment = "NoOpPinotCrypter";

    // act
    Pair<String, File> encryptionInfo = _resource
        .encryptSegmentIfNeeded(_decryptedFile, _encryptedFile, uploadedSegmentIsEncrypted,
            crypterClassNameUsedInUploadedSegment, crypterClassNameInTableConfig, SEGMENT_NAME, TABLE_NAME);

    // assert
    assertEquals("NoOpPinotCrypter", encryptionInfo.getLeft());
    assertEquals(_encryptedFile, encryptionInfo.getRight());
  }

  @Test(expectedExceptions = ControllerApplicationException.class, expectedExceptionsMessageRegExp = "Uploaded segment"
      + " is encrypted with 'FancyCrypter' while table config requires 'NoOpPinotCrypter' as crypter .*")
  public void testEncryptSegmentIfNeededDifferentCrypters() {

    // arrange
    boolean uploadedSegmentIsEncrypted = true;
    String crypterClassNameInTableConfig = "NoOpPinotCrypter";
    String crypterClassNameUsedInUploadedSegment = "FancyCrypter";

    // act
    _resource.encryptSegmentIfNeeded(_decryptedFile, _encryptedFile, uploadedSegmentIsEncrypted,
        crypterClassNameUsedInUploadedSegment, crypterClassNameInTableConfig, SEGMENT_NAME, TABLE_NAME);
  }

  @Test
  public void testEncryptSegmentIfNeededNoEncryption() {

    // arrange
    boolean uploadedSegmentIsEncrypted = false;
    String crypterClassNameInTableConfig = null;
    String crypterClassNameUsedInUploadedSegment = null;

    // act
    Pair<String, File> encryptionInfo = _resource
        .encryptSegmentIfNeeded(_decryptedFile, _encryptedFile, uploadedSegmentIsEncrypted,
            crypterClassNameUsedInUploadedSegment, crypterClassNameInTableConfig, SEGMENT_NAME, TABLE_NAME);

    // assert
    assertNull(encryptionInfo.getLeft());
    assertEquals(_decryptedFile, encryptionInfo.getRight());
  }

  @Test
  public void testCreateSegmentFileFromBodyPart() throws IOException {
    // Arrange
    FormDataBodyPart mockBodyPart = mock(FormDataBodyPart.class);
    File destFile = new File("testSegmentFile.txt");
    String testContent = "This is a test content";

    // Mock input stream to return the test content
    InputStream mockInputStream = new ByteArrayInputStream(testContent.getBytes());
    when(mockBodyPart.getValueAs(InputStream.class)).thenReturn(mockInputStream);

    // Act
    PinotSegmentUploadDownloadRestletResource.createSegmentFileFromBodyPart(mockBodyPart, destFile);

    // Assert
    try (BufferedReader reader = new BufferedReader(new FileReader(destFile))) {
      StringBuilder fileContent = new StringBuilder();
      String line;
      while ((line = reader.readLine()) != null) {
        fileContent.append(line);
      }
      Assert.assertEquals(fileContent.toString(), testContent);
    } finally {
      // Clean up
      destFile.delete();
    }

    // Verify that the cleanup method was called
    verify(mockBodyPart).cleanup();
  }

  @Test
  public void testCreateSegmentFileFromSegmentMetadataInfo()
      throws IOException {
    // setup
    SegmentMetadataInfo metadataInfo = new SegmentMetadataInfo();

    File segmentDir = new File(_tempDir, "segments");
    FileUtils.forceMkdir(segmentDir);
    File creationMetaFile = new File(segmentDir, "creation.meta");
    FileUtils.touch(creationMetaFile);
    File metadataPropertiesFile = new File(segmentDir, "metadata.properties");
    FileUtils.touch(metadataPropertiesFile);

    metadataInfo.setSegmentCreationMetaFile(creationMetaFile);
    metadataInfo.setSegmentMetadataPropertiesFile(metadataPropertiesFile);

    File destFile = new File(_tempDir, "outputSegment");

    // test
    PinotSegmentUploadDownloadRestletResource.createSegmentFileFromSegmentMetadataInfo(metadataInfo, destFile);

    // verify
    Assert.assertTrue(FileUtils.getFile(destFile).exists());
  }

  @Test
  public void testGetSegmentSizeFromFile()
      throws IOException {
    // setup
    File segmentDir = new File(_tempDir, "segments");
    FileUtils.forceMkdir(segmentDir);
    File creationMetaFile = new File(segmentDir, "creation.meta");
    FileUtils.touch(creationMetaFile);
    File metadataPropertiesFile = new File(segmentDir, "metadata.properties");
    FileUtils.touch(metadataPropertiesFile);

    File allSegmentsMetadataFile = new File(segmentDir, "all_segments_metadata");
    FileUtils.touch(allSegmentsMetadataFile);
    List<String> lines = List.of("mySegmentName", "/path/to/segment/download/uri");
    FileUtils.writeLines(allSegmentsMetadataFile, lines);

    File allSegmentsMetadataTarFile = new File(segmentDir, "allSegments.tar.gz");
    TarCompressionUtils.createCompressedTarFile(segmentDir, allSegmentsMetadataTarFile);

    // test
    long segmentSizeInBytes =
        PinotSegmentUploadDownloadRestletResource.getSegmentSizeFromFile(allSegmentsMetadataTarFile.toURI().toString());

    // verify
    Assert.assertTrue(segmentSizeInBytes > 0);
  }

  @Test
  public void testValidateMultiPartForBatchSegmentUpload() {
    // setup
    FileDataBodyPart bodyPart = new FileDataBodyPart("allSegments.tar.gz", new File(_tempDir, "dummyFile"));
    List<BodyPart> bodyParts = List.of(bodyPart);

    // validate – should not throw exception
    PinotSegmentUploadDownloadRestletResource.validateMultiPartForBatchSegmentUpload(bodyParts);
  }

//  @Test
//  public void testUploadSegmentWithMissingTmpDir() {
//    PinotSegmentUploadDownloadRestletResource _resource = new PinotSegmentUploadDownloadRestletResource();
//    _resource.uploadSegmentAsMultiPart();
//  }
}
