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
package org.apache.pinot.spi.filesystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;


public class LocalPinotFSTest {
  private File testFile;
  private File _absoluteTmpDirPath;
  private File _newTmpDir;
  private File _nonExistentTmpFolder;

  @BeforeClass
  public void setUp() {
    _absoluteTmpDirPath =
        new File(System.getProperty("java.io.tmpdir"), LocalPinotFSTest.class.getSimpleName() + "first");
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

    _nonExistentTmpFolder = new File(System.getProperty("java.io.tmpdir"),
        LocalPinotFSTest.class.getSimpleName() + "nonExistentParent/nonExistent");

    _absoluteTmpDirPath.deleteOnExit();
    _newTmpDir.deleteOnExit();
    _nonExistentTmpFolder.deleteOnExit();
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
    Assert.assertTrue(localPinotFS.lastModified(_absoluteTmpDirPath.toURI()) > 0L);
    Assert.assertTrue(localPinotFS.isDirectory(_absoluteTmpDirPath.toURI()));
    // Check whether a file exists
    Assert.assertTrue(localPinotFS.exists(testFileUri));
    Assert.assertFalse(localPinotFS.isDirectory(testFileUri));

    File file = new File(_absoluteTmpDirPath, "secondTestFile");
    URI secondTestFileUri = file.toURI();
    // Check that file does not exist
    Assert.assertTrue(!localPinotFS.exists(secondTestFileUri));

    localPinotFS.copy(testFileUri, secondTestFileUri);
    Assert.assertEquals(2, localPinotFS.listFiles(_absoluteTmpDirPath.toURI(), true).length);

    // Check file copy worked when file was not created
    Assert.assertTrue(localPinotFS.exists(secondTestFileUri));

    // Create another file in the same path
    File thirdTestFile = new File(_absoluteTmpDirPath, "thirdTestFile");
    Assert.assertTrue(thirdTestFile.createNewFile(), "Could not create file " + thirdTestFile.getPath());

    File newAbsoluteTempDirPath = new File(_absoluteTmpDirPath, "absoluteTwo");
    Assert.assertTrue(newAbsoluteTempDirPath.mkdir());

    // Create a testDir and file underneath directory
    File testDir = new File(newAbsoluteTempDirPath, "testDir");
    Assert.assertTrue(testDir.mkdir(), "Could not make directory " + testDir.getAbsolutePath());
    File testDirFile = new File(testDir, "testFile");
    // Assert that recursive list files and nonrecursive list files are as expected
    Assert.assertTrue(testDirFile.createNewFile(), "Could not create file " + testDir.getAbsolutePath());
    Assert.assertEquals(localPinotFS.listFiles(newAbsoluteTempDirPath.toURI(), false),
        new String[]{testDir.getAbsolutePath()});
    Assert.assertEquals(localPinotFS.listFiles(newAbsoluteTempDirPath.toURI(), true),
        new String[]{testDir.getAbsolutePath(), testDirFile.getAbsolutePath()});

    // Create another parent dir so we can test recursive move
    File newAbsoluteTempDirPath3 = new File(_absoluteTmpDirPath, "absoluteThree");
    Assert.assertTrue(newAbsoluteTempDirPath3.mkdir());
    Assert.assertEquals(newAbsoluteTempDirPath3.listFiles().length, 0);

    localPinotFS.move(newAbsoluteTempDirPath.toURI(), newAbsoluteTempDirPath3.toURI(), true);
    Assert.assertFalse(localPinotFS.exists(newAbsoluteTempDirPath.toURI()));
    Assert.assertTrue(localPinotFS.exists(newAbsoluteTempDirPath3.toURI()));
    Assert.assertTrue(localPinotFS.exists(new File(newAbsoluteTempDirPath3, "testDir").toURI()));
    Assert.assertTrue(localPinotFS.exists(new File(new File(newAbsoluteTempDirPath3, "testDir"), "testFile").toURI()));

    // Check if using a different scheme on URI still works
    URI uri = URI.create("hdfs://localhost:9999" + newAbsoluteTempDirPath.getPath());
    localPinotFS.move(newAbsoluteTempDirPath3.toURI(), uri, true);
    Assert.assertFalse(localPinotFS.exists(newAbsoluteTempDirPath3.toURI()));
    Assert.assertTrue(localPinotFS.exists(newAbsoluteTempDirPath.toURI()));
    Assert.assertTrue(localPinotFS.exists(new File(newAbsoluteTempDirPath, "testDir").toURI()));
    Assert.assertTrue(localPinotFS.exists(new File(new File(newAbsoluteTempDirPath, "testDir"), "testFile").toURI()));

    // Check file copy to location where something already exists still works
    localPinotFS.copy(testFileUri, thirdTestFile.toURI());
    // Check length of file
    Assert.assertEquals(0, localPinotFS.length(secondTestFileUri));
    Assert.assertTrue(localPinotFS.exists(thirdTestFile.toURI()));

    // Check that method deletes dst directory during move and is successful by overwriting dir
    Assert.assertTrue(_newTmpDir.exists());
    // create a file in the dst folder
    File dstFile = new File(_newTmpDir.getPath() + "/newFile");
    dstFile.createNewFile();

    // Expected that a move without overwrite will not succeed
    Assert.assertFalse(localPinotFS.move(_absoluteTmpDirPath.toURI(), _newTmpDir.toURI(), false));

    int files = _absoluteTmpDirPath.listFiles().length;
    Assert.assertTrue(localPinotFS.move(_absoluteTmpDirPath.toURI(), _newTmpDir.toURI(), true));
    Assert.assertEquals(_absoluteTmpDirPath.length(), 0);
    Assert.assertEquals(_newTmpDir.listFiles().length, files);
    Assert.assertFalse(dstFile.exists());

    // Check that a moving a file to a non-existent destination folder will work
    FileUtils.deleteQuietly(_nonExistentTmpFolder);
    Assert.assertFalse(_nonExistentTmpFolder.exists());
    File srcFile = new File(_absoluteTmpDirPath, "srcFile");
    localPinotFS.mkdir(_absoluteTmpDirPath.toURI());
    Assert.assertTrue(srcFile.createNewFile());
    dstFile = new File(_nonExistentTmpFolder.getPath() + "/newFile");
    Assert.assertFalse(dstFile.exists());
    Assert.assertTrue(localPinotFS.move(srcFile.toURI(), dstFile.toURI(), true)); // overwrite flag has no impact
    Assert.assertFalse(srcFile.exists());
    Assert.assertTrue(dstFile.exists());

    // Check that moving a folder to a non-existent destination folder works
    FileUtils.deleteQuietly(_nonExistentTmpFolder);
    Assert.assertFalse(_nonExistentTmpFolder.exists());
    srcFile = new File(_absoluteTmpDirPath, "srcFile");
    localPinotFS.mkdir(_absoluteTmpDirPath.toURI());
    Assert.assertTrue(srcFile.createNewFile());
    dstFile = new File(_nonExistentTmpFolder.getPath() + "/srcFile");
    Assert.assertFalse(dstFile.exists());
    Assert.assertTrue(localPinotFS.move(_absoluteTmpDirPath.toURI(), _nonExistentTmpFolder.toURI(), true)); // overwrite flag has no impact
    Assert.assertTrue(dstFile.exists());

    localPinotFS.delete(secondTestFileUri, true);
    // Check deletion from final location worked
    Assert.assertTrue(!localPinotFS.exists(secondTestFileUri));

    File firstTempDir = new File(_absoluteTmpDirPath, "firstTempDir");
    File secondTempDir = new File(_absoluteTmpDirPath, "secondTempDir");
    localPinotFS.mkdir(firstTempDir.toURI());
    Assert.assertTrue(firstTempDir.exists(), "Could not make directory " + firstTempDir.getPath());

    // Check that touching a file works
    File nonExistingFile = new File(_absoluteTmpDirPath, "nonExistingFile");
    Assert.assertFalse(nonExistingFile.exists());
    localPinotFS.touch(nonExistingFile.toURI());
    Assert.assertTrue(nonExistingFile.exists());
    long currentTime = System.currentTimeMillis();
    Assert.assertTrue(localPinotFS.lastModified(nonExistingFile.toURI()) <= currentTime);
    Thread.sleep(1000L);
    // update last modified.
    localPinotFS.touch(nonExistingFile.toURI());
    Assert.assertTrue(localPinotFS.lastModified(nonExistingFile.toURI()) > currentTime);
    FileUtils.deleteQuietly(nonExistingFile);

    // Check that touch an file in a directory that doesn't exist should throw an exception.
    File nonExistingFileUnderNonExistingDir = new File(_absoluteTmpDirPath, "nonExistingDir/nonExistingFile");
    Assert.assertFalse(nonExistingFileUnderNonExistingDir.exists());
    try {
      localPinotFS.touch(nonExistingFileUnderNonExistingDir.toURI());
      Assert.fail("Touch method should throw an IOException");
    } catch (IOException e) {
      // Expected.
    }

    // Check that directory only copy worked
    localPinotFS.copy(firstTempDir.toURI(), secondTempDir.toURI());
    Assert.assertTrue(localPinotFS.exists(secondTempDir.toURI()));

    // Copying directory with files to directory with files
    File testFile = new File(firstTempDir, "testFile");
    Assert.assertTrue(testFile.createNewFile(), "Could not create file " + testFile.getPath());
    File newTestFile = new File(secondTempDir, "newTestFile");
    Assert.assertTrue(newTestFile.createNewFile(), "Could not create file " + newTestFile.getPath());

    localPinotFS.copy(firstTempDir.toURI(), secondTempDir.toURI());
    Assert.assertEquals(localPinotFS.listFiles(secondTempDir.toURI(), true).length, 1);

    // Copying directory with files under another directory.
    File firstTempDirUnderSecondTempDir = new File(secondTempDir, firstTempDir.getName());
    localPinotFS.copy(firstTempDir.toURI(), firstTempDirUnderSecondTempDir.toURI());
    Assert.assertTrue(localPinotFS.exists(firstTempDirUnderSecondTempDir.toURI()));
    // There're two files/directories under secondTempDir.
    Assert.assertEquals(localPinotFS.listFiles(secondTempDir.toURI(), false).length, 2);
    // The file under src directory also got copied under dst directory.
    Assert.assertEquals(localPinotFS.listFiles(firstTempDirUnderSecondTempDir.toURI(), true).length, 1);

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
  }
}
