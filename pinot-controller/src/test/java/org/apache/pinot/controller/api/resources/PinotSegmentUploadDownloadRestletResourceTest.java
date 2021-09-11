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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.spi.crypt.NoOpPinotCrypter;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class PinotSegmentUploadDownloadRestletResourceTest {

  private static final String TABLE_NAME = "table_abc";
  private static final String SEGMENT_NAME = "segment_xyz";

  private PinotSegmentUploadDownloadRestletResource _resource = new PinotSegmentUploadDownloadRestletResource();
  private File _encryptedFile;
  private File _decryptedFile;

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
}
