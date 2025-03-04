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
package org.apache.pinot.server.predownload;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.server.predownload.PredownloadTestUtil.CLUSTER_NAME;
import static org.apache.pinot.server.predownload.PredownloadTestUtil.INSTANCE_ID;
import static org.apache.pinot.server.predownload.PredownloadTestUtil.SEGMENT_NAME;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;


public class PredownloadPredownloadStatusRecorderTest {

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
    PredownloadStatusRecorder.registerMetrics(mock(PredownloadMetrics.class));
    File testFolder = new File(_temporaryFolder, "test");
    testFolder.mkdir();
    PredownloadStatusRecorder.setStatusRecordFolder(testFolder.getAbsolutePath());

    SecurityManager originalSecurityManager = System.getSecurityManager();
    try {
      ExitHelper.setExitAction(status -> {
        throw new ExitException(status);
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
      PredownloadStatusRecorder.predownloadComplete(PredownloadCompletionReason.NO_SEGMENT_TO_PREDOWNLOAD, CLUSTER_NAME,
          INSTANCE_ID,
          SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (ExitException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(listFiles.length, 1);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("SUCCESS"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      PredownloadStatusRecorder.predownloadComplete(PredownloadCompletionReason.ALL_SEGMENTS_DOWNLOADED, CLUSTER_NAME,
          INSTANCE_ID,
          SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (ExitException e) {
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
      PredownloadStatusRecorder.predownloadComplete(PredownloadCompletionReason.CANNOT_CONNECT_TO_DEEPSTORE,
          CLUSTER_NAME,
          INSTANCE_ID, SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (ExitException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(listFiles.length, 1);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("FAILURE"));
      // rename to ensure the second test case won't create a file with exact same name
      listFiles[0].renameTo(new File(testFolder, "FAILURE_12345"));
    }

    try {
      PredownloadStatusRecorder.predownloadComplete(PredownloadCompletionReason.SOME_SEGMENTS_DOWNLOAD_FAILED,
          CLUSTER_NAME,
          INSTANCE_ID, SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (ExitException e) {
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
      PredownloadStatusRecorder.predownloadComplete(PredownloadCompletionReason.INSTANCE_NON_EXISTENT, CLUSTER_NAME,
          INSTANCE_ID,
          SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (ExitException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(listFiles.length, 1);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("NONRETRIABLEFAILURE"));
    }

    try {
      PredownloadStatusRecorder.predownloadComplete(PredownloadCompletionReason.INSTANCE_NOT_ALIVE, CLUSTER_NAME,
          INSTANCE_ID,
          SEGMENT_NAME);
      Assert.fail("No exception indicates we never called System.exit");
    } catch (ExitException e) {
      assertNotNull(e);
      File[] listFiles = testFolder.listFiles();
      assertEquals(listFiles.length, 1);
      assertTrue(listFiles[0].isFile());
      assertTrue(listFiles[0].getName().startsWith("NONRETRIABLEFAILURE"));
    }
  }
}

class ExitException extends RuntimeException {
  private final int _status;

  public ExitException(int status) {
    _status = status;
  }

  public int getStatus() {
    return _status;
  }
}
