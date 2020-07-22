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
  File srcFile;
  File destinationFile;
  @BeforeTest
  public void init() throws IOException {
    pinotCrypter = new NoOpPinotCrypter();
    srcFile = File.createTempFile("srcFile","txt");
    srcFile.deleteOnExit();
    destinationFile = File.createTempFile("destFile","txt");
    destinationFile.deleteOnExit();
    FileUtils.write(srcFile,"testData");
  }

  @Test
  public void testEncryption() throws IOException {
    pinotCrypter.encrypt(srcFile,destinationFile);
    Assert.assertTrue(FileUtils.contentEquals(srcFile,destinationFile));

    srcFile = new File("fake");
    pinotCrypter.encrypt(srcFile,destinationFile);
    Assert.assertFalse(destinationFile.exists());

  }

  @Test
  public void testDecryption() throws IOException {
    pinotCrypter.decrypt(destinationFile,srcFile);
    Assert.assertTrue(FileUtils.contentEquals(srcFile,destinationFile));
    srcFile = new File("fake");
    pinotCrypter.decrypt(srcFile,destinationFile);
    Assert.assertFalse(destinationFile.exists());
  }

}
