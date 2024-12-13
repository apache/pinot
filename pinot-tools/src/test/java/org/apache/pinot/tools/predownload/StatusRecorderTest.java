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

  private File temporaryFolder;

  @BeforeMethod
  public void setUp()
      throws IOException {
    temporaryFolder = new File(FileUtils.getTempDirectory(), this.getClass().getName());
    FileUtils.deleteQuietly(temporaryFolder);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    if (temporaryFolder != null && temporaryFolder.exists()) {
      try {
        FileUtils.deleteDirectory(temporaryFolder);
        System.out.println("Temporary folder deleted: " + temporaryFolder.getAbsolutePath());
      } catch (IOException e) {
        System.err.println("Failed to delete temporary folder: " + e.getMessage());
      }
    }
  }

  @Test
  public void testPredownloadComplete()
      throws Exception {
    StatusRecorder.registerMetrics(mock(PredownloadMetrics.class));
    File testFolder = new File(temporaryFolder, "test");
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
  private final int status;

  public ExitException(int status) {
    this.status = status;
  }

  public int getStatus() {
    return status;
  }
}
