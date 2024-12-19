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
package org.apache.pinot.tools.predownload;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.tools.predownload.TestUtil.CLUSTER_NAME;
import static org.apache.pinot.tools.predownload.TestUtil.INSTANCE_ID;
import static org.apache.pinot.tools.predownload.TestUtil.SEGMENT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;


public class StatusRecorderTest {

  private File _temporaryFolder;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _temporaryFolder = new File(FileUtils.getTempDirectory(), getClass().getName());
    FileUtils.deleteQuietly(_temporaryFolder);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    if (_temporaryFolder != null && _temporaryFolder.exists()) {
      try {
        FileUtils.deleteDirectory(_temporaryFolder);
        System.out.println("Temporary folder deleted: " + _temporaryFolder.getAbsolutePath());
      } catch (IOException e) {
        System.err.println("Failed to delete temporary folder: " + e.getMessage());
      }
    }
  }

  @Test
  public void testPredownloadComplete()
      throws Exception {
    StatusRecorder.registerMetrics(mock(PredownloadMetrics.class));
    File testFolder = new File(_temporaryFolder, "test");
    testFolder.mkdir();
    StatusRecorder.setStatusRecordFolder(testFolder.getAbsolutePath());

    SecurityManager originalSecurityManager = System.getSecurityManager();
    try {
      // Set a custom SecurityManager to test system.exit()
      System.setSecurityManager(new SecurityManager() {
        @Override
        public void checkPermission(java.security.Permission perm) {
        }

        @Override
        public void checkExit(int status) {
          // Intercept System.exit calls
          throw new ExitException(status);
        }
      });
      trySucceed(testFolder);
      tryRetriableFailure(testFolder);
      tryNonRetriableFailure(testFolder);
    } finally {
      // Restore the original SecurityManager
      System.setSecurityManager(originalSecurityManager);
    }
  }

  private void trySucceed(File testFolder) {
    try {
      StatusRecorder.predownloadComplete(PredownloadCompleteReason.NO_SEGMENT_TO_PREDOWNLOAD, CLUSTER_NAME, INSTANCE_ID,
          SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (SecurityException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(listFiles.length, 1);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("SUCCESS"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      StatusRecorder.predownloadComplete(PredownloadCompleteReason.ALL_SEGMENTS_DOWNLOADED, CLUSTER_NAME, INSTANCE_ID,
          SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (SecurityException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(listFiles.length, 1);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("SUCCESS"));
    }
  }

  private void tryRetriableFailure(File testFolder)
      throws InterruptedException {
    try {
      StatusRecorder.predownloadComplete(PredownloadCompleteReason.CANNOT_CONNECT_TO_DEEPSTORE, CLUSTER_NAME,
          INSTANCE_ID, SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (SecurityException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(listFiles.length, 1);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("FAILURE"));
      // rename to ensure the second test case won't create a file with exact same name
      listFiles[0].renameTo(new File(testFolder, "FAILURE_12345"));
    }

    try {
      StatusRecorder.predownloadComplete(PredownloadCompleteReason.SOME_SEGMENTS_DOWNLOAD_FAILED, CLUSTER_NAME,
          INSTANCE_ID, SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (SecurityException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(2, listFiles.length);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("FAILURE"));
      assertTrue(listFiles[1].isFile());
      assertTrue(listFiles[1].getName().startsWith("FAILURE"));
    }
  }

  private void tryNonRetriableFailure(File testFolder) {
    try {
      StatusRecorder.predownloadComplete(PredownloadCompleteReason.INSTANCE_NON_EXISTENT, CLUSTER_NAME, INSTANCE_ID,
          SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (SecurityException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(listFiles.length, 1);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("NONRETRIABLEFAILURE"));
    }

    try {
      StatusRecorder.predownloadComplete(PredownloadCompleteReason.INSTANCE_NOT_ALIVE, CLUSTER_NAME, INSTANCE_ID,
          SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (SecurityException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(listFiles.length, 1);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("NONRETRIABLEFAILURE"));
    }
  }
}

class ExitException extends SecurityException {
  private final int _status;

  public ExitException(int status) {
    _status = status;
  }

  public int getStatus() {
    return _status;
  }
}
