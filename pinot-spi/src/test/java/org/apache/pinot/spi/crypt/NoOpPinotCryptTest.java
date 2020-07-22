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
package org.apache.pinot.spi.crypt;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class NoOpPinotCryptTest {

  PinotCrypter pinotCrypter;
  File encryptedFile;
  File decryptedFile;
  @BeforeTest
  public void init() throws IOException {
    pinotCrypter = new NoOpPinotCrypter();
    encryptedFile = File.createTempFile("encryptedFile","txt");
    encryptedFile.deleteOnExit();
    decryptedFile = File.createTempFile("decryptedFile","txt");
    decryptedFile.deleteOnExit();
    FileUtils.write(encryptedFile,"testData");
  }

  @Test
  public void testEncryption() throws IOException {
    pinotCrypter.encrypt(decryptedFile, encryptedFile);
    Assert.assertTrue(FileUtils.contentEquals(encryptedFile, decryptedFile));
    decryptedFile = new File("fake");
    pinotCrypter.encrypt(decryptedFile, encryptedFile);
    Assert.assertFalse(encryptedFile.exists());

  }

  @Test
  public void testDecryption() throws IOException {
    pinotCrypter.decrypt(encryptedFile, decryptedFile);
    Assert.assertTrue(FileUtils.contentEquals(encryptedFile, decryptedFile));
    encryptedFile = new File("fake");
    pinotCrypter.decrypt(encryptedFile, decryptedFile);
    Assert.assertFalse(decryptedFile.exists());
  }

}
