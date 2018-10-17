/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.filesystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class LocalPinotFSTest {
  private File testFile;
  private File _absoluteTmpDirPath;
  private File _newTmpDir;

  @BeforeClass
  public void setUp() {
    _absoluteTmpDirPath = new File(System.getProperty("java.io.tmpdir"), LocalPinotFSTest.class.getSimpleName() + "first");
    FileUtils.deleteQuietly(_absoluteTmpDirPath);
    Assert.assertTrue(_absoluteTmpDirPath.mkdir(), "Could not make directory " + _absoluteTmpDirPath.getPath());
    try {
      testFile = new File(_absoluteTmpDirPath, "testFile");
      Assert.assertTrue(testFile.createNewFile(), "Could not create file " + testFile.getPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    _newTmpDir = new File(System.getProperty("java.io.tmpdir"), LocalPinotFSTest.class.getSimpleName() + "second");
    FileUtils.deleteQuietly(_newTmpDir);
    Assert.assertTrue(_newTmpDir.mkdir(), "Could not make directory " + _newTmpDir.getPath());

    _absoluteTmpDirPath.deleteOnExit();
    _newTmpDir.deleteOnExit();
  }

  @AfterClass
  public void tearDown() {
    _absoluteTmpDirPath.delete();
    _newTmpDir.delete();
  }

  @Test
  public void testFS() throws Exception {
    LocalPinotFS localPinotFS = new LocalPinotFS();
    URI testFileUri = testFile.toURI();
    // Check whether a directory exists
    Assert.assertTrue(localPinotFS.exists(_absoluteTmpDirPath.toURI()));
    Assert.assertTrue(localPinotFS.isDirectory(_absoluteTmpDirPath.toURI()));
    // Check whether a file exists
    Assert.assertTrue(localPinotFS.exists(testFileUri));
    Assert.assertFalse(localPinotFS.isDirectory(testFileUri));

    File file = new File(_absoluteTmpDirPath, "secondTestFile");
    URI secondTestFileUri = file.toURI();
    // Check that file does not exist
    Assert.assertTrue(!localPinotFS.exists(secondTestFileUri));

    localPinotFS.copy(testFileUri, secondTestFileUri);
    Assert.assertEquals(2, localPinotFS.listFiles(_absoluteTmpDirPath.toURI()).length);

    // Check file copy worked when file was not created
    Assert.assertTrue(localPinotFS.exists(secondTestFileUri));

    // Create another file in the same path
    File thirdTestFile = new File(_absoluteTmpDirPath, "thirdTestFile");
    Assert.assertTrue(thirdTestFile.createNewFile(), "Could not create file " + thirdTestFile.getPath());

    // Check file copy to location where something already exists still works
    localPinotFS.copy(testFileUri, thirdTestFile.toURI());
    // Check length of file
    Assert.assertEquals(0, localPinotFS.length(secondTestFileUri));
    Assert.assertTrue(localPinotFS.exists(thirdTestFile.toURI()));

    // Check that method deletes dst directory during move and is successful by overwriting dir
    Assert.assertTrue(_newTmpDir.exists());
    localPinotFS.move(_absoluteTmpDirPath.toURI(), _newTmpDir.toURI());
    Assert.assertEquals(_absoluteTmpDirPath.length(), 0);

    localPinotFS.delete(secondTestFileUri);
    // Check deletion from final location worked
    Assert.assertTrue(!localPinotFS.exists(secondTestFileUri));

    File firstTempDir = new File(_absoluteTmpDirPath, "firstTempDir");
    File secondTempDir = new File(_absoluteTmpDirPath, "secondTempDir");
    localPinotFS.mkdir(firstTempDir.toURI());
    Assert.assertTrue(firstTempDir.exists(), "Could not make directory " + firstTempDir.getPath());

    // Check that directory only copy worked
    localPinotFS.copy(firstTempDir.toURI(), secondTempDir.toURI());
    Assert.assertTrue(localPinotFS.exists(secondTempDir.toURI()));

    // Copying directory with files to directory with files
    File testFile = new File(firstTempDir, "testFile");
    Assert.assertTrue(testFile.createNewFile(), "Could not create file " + testFile.getPath());
    File newTestFile = new File(secondTempDir, "newTestFile");
    Assert.assertTrue(newTestFile.createNewFile(), "Could not create file " + newTestFile.getPath());

    localPinotFS.copy(firstTempDir.toURI(), secondTempDir.toURI());
    localPinotFS.listFiles(secondTempDir.toURI());
    Assert.assertEquals(localPinotFS.listFiles(secondTempDir.toURI()).length, 1);

    // len of dir = exception
    try {
      localPinotFS.length(firstTempDir.toURI());
      fail();
    } catch (IllegalArgumentException e) {

    }

    Assert.assertTrue(testFile.exists());

    localPinotFS.copyFromLocalFile(testFile, secondTestFileUri);
    Assert.assertTrue(localPinotFS.exists(secondTestFileUri));
    localPinotFS.copyToLocalFile(testFile.toURI(), new File(secondTestFileUri));
    Assert.assertTrue(localPinotFS.exists(secondTestFileUri));

    // List files on a file - exception if file already exists
    try {
      localPinotFS.listFiles(thirdTestFile.toURI());
      fail();
    } catch (IllegalArgumentException e) {

    }
  }
}