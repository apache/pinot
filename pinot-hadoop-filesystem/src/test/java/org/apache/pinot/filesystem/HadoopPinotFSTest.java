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
package org.apache.pinot.filesystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class HadoopPinotFSTest {
  private File testFile;
  private File _absoluteTmpDirPath;
  private File _newTmpDir;
  private File _nonExistentTmpFolder;
  private HadoopPinotFS _hadoopPinotFS;

  @BeforeClass
  public void setup() {
    _absoluteTmpDirPath =
        new File(System.getProperty("java.io.tmpdir"), HadoopPinotFSTest.class.getSimpleName() + "first");
    FileUtils.deleteQuietly(_absoluteTmpDirPath);
    Assert.assertTrue(_absoluteTmpDirPath.mkdir(), "Could not make directory " + _absoluteTmpDirPath.getPath());
    try {
      testFile = new File(_absoluteTmpDirPath, "testFile");
      Assert.assertTrue(testFile.createNewFile(), "Could not create file " + testFile.getPath());
      Assert.assertTrue(testFile.exists());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    _newTmpDir = new File(System.getProperty("java.io.tmpdir"), HadoopPinotFSTest.class.getSimpleName() + "second");
    FileUtils.deleteQuietly(_newTmpDir);
    Assert.assertTrue(_newTmpDir.mkdir(), "Could not make directory " + _newTmpDir.getPath());

    _nonExistentTmpFolder = new File(System.getProperty("java.io.tmpdir"),
        HadoopPinotFSTest.class.getSimpleName() + "nonExistentParent/nonExistent");

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
  public void testHadoopPinotFS() throws Exception {
    _hadoopPinotFS = new HadoopPinotFS();

    final Configuration conf = new PropertiesConfiguration();
    _hadoopPinotFS.init(conf);

    // Check whether a directory exists
    Assert.assertTrue(_hadoopPinotFS.exists(_absoluteTmpDirPath.toURI()));
    Assert.assertTrue(_hadoopPinotFS.lastModified(_absoluteTmpDirPath.toURI()) > 0L);
    Assert.assertTrue(_hadoopPinotFS.isDirectory(_absoluteTmpDirPath.toURI()));

    URI testFileUri = testFile.toURI();
    // Check whether a file exists
    Assert.assertTrue(_hadoopPinotFS.exists(testFileUri));
    Assert.assertFalse(_hadoopPinotFS.isDirectory(testFileUri));

    File file = new File(_absoluteTmpDirPath, "secondTestFile");
    URI secondTestFileUri = file.toURI();
    // Check that file does not exist
    Assert.assertFalse(_hadoopPinotFS.exists(secondTestFileUri));

    String[] files = _hadoopPinotFS.listFiles(_absoluteTmpDirPath.toURI(), true);
    Assert.assertEquals(files.length, 1);
    _hadoopPinotFS.copy(testFileUri, secondTestFileUri);
    files = _hadoopPinotFS.listFiles(_absoluteTmpDirPath.toURI(), true);
    Assert.assertEquals(files.length, 2);
    // Check file copy worked when file was not created
    Assert.assertTrue(_hadoopPinotFS.exists(secondTestFileUri));

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
    Assert.assertEquals(_hadoopPinotFS.listFiles(newAbsoluteTempDirPath.toURI(), false),
        new String[]{testDir.getAbsolutePath()});
    Assert.assertEquals(_hadoopPinotFS.listFiles(newAbsoluteTempDirPath.toURI(), true),
        new String[]{testDir.getAbsolutePath(), testDirFile.getAbsolutePath()});

    // Create another parent dir so we can test recursive move
    File newAbsoluteTempDirPath3 = new File(_absoluteTmpDirPath, "absoluteThree");
    Assert.assertTrue(newAbsoluteTempDirPath3.mkdir());
    Assert.assertEquals(newAbsoluteTempDirPath3.listFiles().length, 0);

    _hadoopPinotFS.move(newAbsoluteTempDirPath.toURI(), newAbsoluteTempDirPath3.toURI(), true);
    Assert.assertFalse(_hadoopPinotFS.exists(newAbsoluteTempDirPath.toURI()));
    Assert.assertTrue(_hadoopPinotFS.exists(newAbsoluteTempDirPath3.toURI()));
    File testDirUnderNewAbsoluteTempDirPath3 = new File(newAbsoluteTempDirPath3, "testDir");
    Assert.assertTrue(_hadoopPinotFS.exists(testDirUnderNewAbsoluteTempDirPath3.toURI()));
    Assert.assertTrue(_hadoopPinotFS.exists(new File(testDirUnderNewAbsoluteTempDirPath3, "testFile").toURI()));

    // Check file copy to location where something already exists still works
    _hadoopPinotFS.copy(testFileUri, thirdTestFile.toURI());
    // Check length of file
    Assert.assertEquals(_hadoopPinotFS.length(secondTestFileUri), 0);
    Assert.assertTrue(_hadoopPinotFS.exists(thirdTestFile.toURI()));

    // Check that destination being directory within the source directory still works
    File anotherFileUnderAbsoluteThreeDir = new File(newAbsoluteTempDirPath3, "anotherFile");
    Assert.assertFalse(_hadoopPinotFS.exists(anotherFileUnderAbsoluteThreeDir.toURI()));
    Assert.assertTrue(anotherFileUnderAbsoluteThreeDir.createNewFile());
    Assert.assertTrue(_hadoopPinotFS.exists(anotherFileUnderAbsoluteThreeDir.toURI()));
    _hadoopPinotFS.copy(newAbsoluteTempDirPath3.toURI(), testDirUnderNewAbsoluteTempDirPath3.toURI());
    Assert.assertEquals(_hadoopPinotFS.listFiles(testDirUnderNewAbsoluteTempDirPath3.toURI(), false).length, 3);

    // Check that method deletes dst directory during move and is successful by overwriting dir
    Assert.assertTrue(_newTmpDir.exists());
    // create a file in the dst folder
    File dstFile = new File(_newTmpDir.getPath() + "/newFile");
    dstFile.createNewFile();

    // Expected that if the target already exists, a move without overwrite will not succeed
    Assert.assertFalse(_hadoopPinotFS.move(_absoluteTmpDirPath.toURI(), _newTmpDir.toURI(), false));

    int numFiles = _absoluteTmpDirPath.listFiles().length;
    Assert.assertTrue(_hadoopPinotFS.move(_absoluteTmpDirPath.toURI(), _newTmpDir.toURI(), true));
    Assert.assertEquals(_absoluteTmpDirPath.length(), 0);
    Assert.assertEquals(_newTmpDir.listFiles().length, numFiles);
    Assert.assertFalse(dstFile.exists());

    // Check that copying a file to a non-existent destination folder will work
    FileUtils.deleteQuietly(_nonExistentTmpFolder);
    Assert.assertFalse(_nonExistentTmpFolder.exists());
    File srcFile = new File(_absoluteTmpDirPath, "srcFile");
    _hadoopPinotFS.mkdir(_absoluteTmpDirPath.toURI());
    Assert.assertTrue(srcFile.createNewFile());
    dstFile = new File(_nonExistentTmpFolder.getPath() + "/newFile");
    Assert.assertFalse(dstFile.exists());
    Assert.assertTrue(_hadoopPinotFS.copy(srcFile.toURI(), dstFile.toURI()));
    Assert.assertTrue(srcFile.exists());
    Assert.assertTrue(dstFile.exists());

    // Check that copying a folder to a non-existent destination folder works
    FileUtils.deleteQuietly(_nonExistentTmpFolder);
    Assert.assertFalse(_nonExistentTmpFolder.exists());
    dstFile = new File(_nonExistentTmpFolder.getPath() + "/srcFile");
    Assert.assertFalse(dstFile.exists());
    Assert.assertTrue(_hadoopPinotFS.copy(_absoluteTmpDirPath.toURI(), _nonExistentTmpFolder.toURI()));
    Assert.assertTrue(dstFile.exists());
    FileUtils.deleteQuietly(srcFile);
    Assert.assertFalse(srcFile.exists());

    // Check that moving a file to a non-existent destination folder will work
    FileUtils.deleteQuietly(_nonExistentTmpFolder);
    Assert.assertFalse(_nonExistentTmpFolder.exists());
    srcFile = new File(_absoluteTmpDirPath, "srcFile");
    _hadoopPinotFS.mkdir(_absoluteTmpDirPath.toURI());
    Assert.assertTrue(srcFile.createNewFile());
    dstFile = new File(_nonExistentTmpFolder.getPath() + "/newFile");
    Assert.assertFalse(dstFile.exists());
    Assert.assertTrue(_hadoopPinotFS.move(srcFile.toURI(), dstFile.toURI(), true)); // overwrite flag has no impact
    Assert.assertFalse(srcFile.exists());
    Assert.assertTrue(dstFile.exists());

    // Check that moving a folder to a non-existent destination folder works
    FileUtils.deleteQuietly(_nonExistentTmpFolder);
    Assert.assertFalse(_nonExistentTmpFolder.exists());
    srcFile = new File(_absoluteTmpDirPath, "srcFile");
    _hadoopPinotFS.mkdir(_absoluteTmpDirPath.toURI());
    Assert.assertTrue(srcFile.createNewFile());
    dstFile = new File(_nonExistentTmpFolder.getPath() + "/srcFile");
    Assert.assertFalse(dstFile.exists());
    Assert.assertTrue(_hadoopPinotFS.move(_absoluteTmpDirPath.toURI(), _nonExistentTmpFolder.toURI(),
        true)); // overwrite flag has no impact
    Assert.assertTrue(dstFile.exists());

    _hadoopPinotFS.delete(secondTestFileUri, true);
    // Check deletion from final location worked
    Assert.assertFalse(_hadoopPinotFS.exists(secondTestFileUri));

    File firstTempDir = new File(_absoluteTmpDirPath, "firstTempDir");
    File secondTempDir = new File(_absoluteTmpDirPath, "secondTempDir");
    _hadoopPinotFS.mkdir(firstTempDir.toURI());
    Assert.assertTrue(firstTempDir.exists(), "Could not make directory " + firstTempDir.getPath());

    // Check that directory only copy worked
    _hadoopPinotFS.copy(firstTempDir.toURI(), secondTempDir.toURI());
    Assert.assertTrue(_hadoopPinotFS.exists(secondTempDir.toURI()));

    // Copying directory with files to directory with files
    File testFile = new File(firstTempDir, "testFile");
    Assert.assertTrue(testFile.createNewFile(), "Could not create file " + testFile.getPath());
    File newTestFile = new File(secondTempDir, "newTestFile");
    Assert.assertTrue(newTestFile.createNewFile(), "Could not create file " + newTestFile.getPath());

    _hadoopPinotFS.copy(firstTempDir.toURI(), secondTempDir.toURI());
    files = _hadoopPinotFS.listFiles(secondTempDir.toURI(), true);
    Assert.assertEquals(files.length, 2);

    // len of a directory should throw an exception.
    try {
      _hadoopPinotFS.length(firstTempDir.toURI());
      Assert.fail();
    } catch (IllegalArgumentException e) {

    }

    Assert.assertTrue(testFile.exists());

    _hadoopPinotFS.copyFromLocalFile(testFile, secondTestFileUri);
    Assert.assertTrue(_hadoopPinotFS.exists(secondTestFileUri));
    _hadoopPinotFS.copyToLocalFile(testFile.toURI(), new File(secondTestFileUri));
    Assert.assertTrue(_hadoopPinotFS.exists(secondTestFileUri));
  }
}
