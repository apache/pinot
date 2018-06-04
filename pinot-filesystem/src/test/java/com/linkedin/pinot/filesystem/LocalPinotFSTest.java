package com.linkedin.pinot.filesystem;
/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.net.URI;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class LocalPinotFSTest {
  private String _tmpDir;
  private File _originalFile;
  private File _absoluteTmpDirPath;

  @BeforeClass
  public void setUp() {
    _tmpDir = System.getProperty("java.io.tmpdir") + LocalPinotFSTest.class.getSimpleName();
    _absoluteTmpDirPath = new File(_tmpDir);
    FileUtils.deleteQuietly(_absoluteTmpDirPath);
    _absoluteTmpDirPath.mkdir();
    _originalFile = new File(_absoluteTmpDirPath, "testFile");
    try {
      _originalFile.createNewFile();
    } catch (Exception e) {

    }
    _absoluteTmpDirPath.deleteOnExit();
  }

  @AfterClass
  public void tearDown() {
    new File(_tmpDir).delete();
  }

  @Test
  public void testFS() {
    LocalPinotFS localPinotFS = new LocalPinotFS();
    try {
      URI originalLocationUri = _originalFile.toURI();
      // Check whether a directory exists
      Assert.assertTrue(localPinotFS.exists(_absoluteTmpDirPath.toURI()));
      // Check whether a file exists
      Assert.assertTrue(localPinotFS.exists(originalLocationUri));
      File file = new File(_tmpDir, "secondTestFile");
      URI finalLocationUri = file.toURI();
      // Check that file does not exist
      Assert.assertTrue(!localPinotFS.exists(finalLocationUri));

      localPinotFS.copy(originalLocationUri, finalLocationUri);
      // Check file copy worked when file was not created
      Assert.assertTrue(localPinotFS.exists(finalLocationUri));
      // Check file copy to location it already exists still works
      localPinotFS.copy(originalLocationUri, finalLocationUri);
      // Check length of file
      Assert.assertEquals(0, localPinotFS.length(finalLocationUri));
      Assert.assertTrue(localPinotFS.exists(finalLocationUri));
      Assert.assertEquals(2, localPinotFS.listFiles(_absoluteTmpDirPath.toURI()).length);

      localPinotFS.delete(finalLocationUri);
      // Check deletion from final location worked
      Assert.assertTrue(!localPinotFS.exists(finalLocationUri));

      File firstTempDir = new File(_tmpDir, "firstTempDir");
      File secondTempDir = new File(_tmpDir, "secondTempDir");

      firstTempDir.mkdirs();
      // Check that directory only copy worked
      localPinotFS.copy(firstTempDir.toURI(), secondTempDir.toURI());
      Assert.assertTrue(localPinotFS.exists(secondTempDir.toURI()));
      long secondDirLength = localPinotFS.length(secondTempDir.toURI());
      // Check length of directory
      Assert.assertEquals(0, secondDirLength);

      localPinotFS.copyFromLocalFile(originalLocationUri, finalLocationUri);
      Assert.assertTrue(localPinotFS.exists(finalLocationUri));
      localPinotFS.copyToLocalFile(originalLocationUri, finalLocationUri);
      Assert.assertTrue(localPinotFS.exists(finalLocationUri));
    } catch (Exception e) {
      Assert.fail();
    }

  }
}
