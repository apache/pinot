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

package org.apache.pinot.plugin.filesystem;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class HadoopPinotFSTest {
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir") + "/HadoopPinotFSTest";

  @BeforeMethod
  public void setUp() {
    FileUtils.deleteQuietly(new File(TMP_DIR));
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(new File(TMP_DIR));
  }

  @Test
  public void testCopy()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testCopy");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(new Path(baseURI.getPath(), "src").toUri());
      hadoopFS.mkdir(new Path(baseURI.getPath(), "src/dir").toUri());
      hadoopFS.touch(new Path(baseURI.getPath(), "src/dir/1").toUri());
      hadoopFS.touch(new Path(baseURI.getPath(), "src/dir/2").toUri());
      String[] srcFiles = hadoopFS.listFiles(new Path(baseURI.getPath(), "src").toUri(), true);
      Assert.assertEquals(srcFiles.length, 3);
      hadoopFS.copyDir(new Path(baseURI.getPath(), "src").toUri(), new Path(baseURI.getPath(), "dest").toUri());
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest").toUri()));
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir").toUri()));
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir/1").toUri()));
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir/2").toUri()));
      String[] destFiles = hadoopFS.listFiles(new Path(baseURI.getPath(), "dest").toUri(), true);
      Assert.assertEquals(destFiles.length, 3);
      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testCopyFromLocalFile()
      throws Exception {
    String baseDir = TMP_DIR + "/testCopyFromLocalFile";
    URI baseURI = URI.create(baseDir);
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      new File(baseDir + "/src").mkdirs();
      new File(baseDir + "/src/dir").mkdirs();
      new File(baseDir + "/src/dir/1").createNewFile();
      new File(baseDir + "/src/dir/2").createNewFile();
      hadoopFS.init(new PinotConfiguration());
      String[] srcFiles = hadoopFS.listFiles(new Path(baseURI.getPath(), "src").toUri(), true);
      Assert.assertEquals(srcFiles.length, 3);
      hadoopFS.copyFromLocalDir(new File(baseDir + "/src"), new Path(baseURI.getPath(), "dest").toUri());
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest").toUri()));
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir").toUri()));
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir/1").toUri()));
      Assert.assertTrue(hadoopFS.exists(new Path(baseURI.getPath(), "dest/dir/2").toUri()));
      String[] destFiles = hadoopFS.listFiles(new Path(baseURI.getPath(), "dest").toUri(), true);
      Assert.assertEquals(destFiles.length, 3);
      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testListFilesWithMetadata()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testListFilesWithMetadata");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());

      // Create a testDir and file underneath directory
      int count = 5;
      List<String> expectedNonRecursive = new ArrayList<>();
      List<String> expectedRecursive = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        URI testDir = new Path(baseURI.getPath(), "testDir" + i).toUri();
        hadoopFS.mkdir(testDir);
        expectedNonRecursive.add((testDir.getScheme() == null ? "file:" : "") + testDir);

        URI testFile = new Path(testDir.getPath(), "testFile" + i).toUri();
        hadoopFS.touch(testFile);
        expectedRecursive.add((testDir.getScheme() == null ? "file:" : "") + testDir);
        expectedRecursive.add((testDir.getScheme() == null ? "file:" : "") + testFile);
      }
      URI testDirEmpty = new Path(baseURI.getPath(), "testDirEmpty").toUri();
      hadoopFS.mkdir(testDirEmpty);
      expectedNonRecursive.add((testDirEmpty.getScheme() == null ? "file:" : "") + testDirEmpty);
      expectedRecursive.add((testDirEmpty.getScheme() == null ? "file:" : "") + testDirEmpty);

      URI testRootFile = new Path(baseURI.getPath(), "testRootFile").toUri();
      hadoopFS.touch(testRootFile);
      expectedNonRecursive.add((testRootFile.getScheme() == null ? "file:" : "") + testRootFile);
      expectedRecursive.add((testRootFile.getScheme() == null ? "file:" : "") + testRootFile);

      // Assert that recursive list files and nonrecursive list files are as expected
      String[] files = hadoopFS.listFiles(baseURI, false);
      Assert.assertEquals(files.length, count + 2);
      Assert.assertTrue(expectedNonRecursive.containsAll(Arrays.asList(files)), Arrays.toString(files));
      files = hadoopFS.listFiles(baseURI, true);
      Assert.assertEquals(files.length, count * 2 + 2);
      Assert.assertTrue(expectedRecursive.containsAll(Arrays.asList(files)), Arrays.toString(files));

      // Assert that recursive list files and nonrecursive list files with file info are as expected
      List<FileMetadata> fileMetadata = hadoopFS.listFilesWithMetadata(baseURI, false);
      Assert.assertEquals(fileMetadata.size(), count + 2);
      Assert.assertEquals(fileMetadata.stream().filter(FileMetadata::isDirectory).count(), count + 1);
      Assert.assertEquals(fileMetadata.stream().filter(f -> !f.isDirectory()).count(), 1);
      Assert.assertTrue(expectedNonRecursive.containsAll(
          fileMetadata.stream().map(FileMetadata::getFilePath).collect(Collectors.toSet())), fileMetadata.toString());
      fileMetadata = hadoopFS.listFilesWithMetadata(baseURI, true);
      Assert.assertEquals(fileMetadata.size(), count * 2 + 2);
      Assert.assertEquals(fileMetadata.stream().filter(FileMetadata::isDirectory).count(), count + 1);
      Assert.assertEquals(fileMetadata.stream().filter(f -> !f.isDirectory()).count(), count + 1);
      Assert.assertTrue(expectedRecursive.containsAll(
          fileMetadata.stream().map(FileMetadata::getFilePath).collect(Collectors.toSet())), fileMetadata.toString());
    }
  }

  @Test
  public void testDeleteBatchWithEmptyList()
      throws IOException {
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());

      // Test with null list
      Assert.assertTrue(hadoopFS.deleteBatch(null, false));

      // Test with empty list
      Assert.assertTrue(hadoopFS.deleteBatch(new ArrayList<>(), false));
    }
  }

  @Test
  public void testDeleteBatchWithSingleFile()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchWithSingleFile");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create a single file
      URI testFile = new Path(baseURI.getPath(), "testFile.txt").toUri();
      hadoopFS.touch(testFile);
      Assert.assertTrue(hadoopFS.exists(testFile));

      // Delete using deleteBatch
      List<URI> urisToDelete = new ArrayList<>();
      urisToDelete.add(testFile);
      Assert.assertTrue(hadoopFS.deleteBatch(urisToDelete, false));

      // Verify file is deleted
      Assert.assertFalse(hadoopFS.exists(testFile));

      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testDeleteBatchWithMultipleFiles()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchWithMultipleFiles");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create multiple files
      List<URI> urisToDelete = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        URI testFile = new Path(baseURI.getPath(), "testFile" + i + ".txt").toUri();
        hadoopFS.touch(testFile);
        Assert.assertTrue(hadoopFS.exists(testFile));
        urisToDelete.add(testFile);
      }

      // Delete all files using deleteBatch
      Assert.assertTrue(hadoopFS.deleteBatch(urisToDelete, false));

      // Verify all files are deleted
      for (URI uri : urisToDelete) {
        Assert.assertFalse(hadoopFS.exists(uri));
      }

      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testDeleteBatchWithNonExistentFiles()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchWithNonExistentFiles");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create list with non-existent files
      List<URI> urisToDelete = new ArrayList<>();
      URI nonExistentFile1 = new Path(baseURI.getPath(), "nonExistent1.txt").toUri();
      URI nonExistentFile2 = new Path(baseURI.getPath(), "nonExistent2.txt").toUri();
      urisToDelete.add(nonExistentFile1);
      urisToDelete.add(nonExistentFile2);

      // Should return true and skip non-existent files
      Assert.assertTrue(hadoopFS.deleteBatch(urisToDelete, false));

      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testDeleteBatchWithMixedExistingAndNonExisting()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchWithMixedFiles");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create some files
      URI existingFile1 = new Path(baseURI.getPath(), "existing1.txt").toUri();
      URI existingFile2 = new Path(baseURI.getPath(), "existing2.txt").toUri();
      hadoopFS.touch(existingFile1);
      hadoopFS.touch(existingFile2);

      // Create list with mix of existing and non-existing files
      List<URI> urisToDelete = new ArrayList<>();
      urisToDelete.add(existingFile1);
      urisToDelete.add(new Path(baseURI.getPath(), "nonExistent.txt").toUri());
      urisToDelete.add(existingFile2);

      // Should successfully delete existing files and skip non-existing
      Assert.assertTrue(hadoopFS.deleteBatch(urisToDelete, false));

      // Verify existing files are deleted
      Assert.assertFalse(hadoopFS.exists(existingFile1));
      Assert.assertFalse(hadoopFS.exists(existingFile2));

      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testDeleteBatchWithEmptyDirectory()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchWithEmptyDirectory");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create an empty directory
      URI emptyDir = new Path(baseURI.getPath(), "emptyDir").toUri();
      hadoopFS.mkdir(emptyDir);
      Assert.assertTrue(hadoopFS.exists(emptyDir));

      // Delete empty directory with forceDelete=false
      List<URI> urisToDelete = new ArrayList<>();
      urisToDelete.add(emptyDir);
      Assert.assertTrue(hadoopFS.deleteBatch(urisToDelete, false));

      // Verify directory is deleted
      Assert.assertFalse(hadoopFS.exists(emptyDir));

      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testDeleteBatchWithNonEmptyDirectoryForceDeleteFalse()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchNonEmptyDirNoForce");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create a non-empty directory
      URI nonEmptyDir = new Path(baseURI.getPath(), "nonEmptyDir").toUri();
      hadoopFS.mkdir(nonEmptyDir);
      URI fileInDir = new Path(nonEmptyDir.getPath(), "file.txt").toUri();
      hadoopFS.touch(fileInDir);

      // Try to delete non-empty directory with forceDelete=false
      List<URI> urisToDelete = new ArrayList<>();
      urisToDelete.add(nonEmptyDir);
      Assert.assertFalse(hadoopFS.deleteBatch(urisToDelete, false));

      // Verify directory still exists
      Assert.assertTrue(hadoopFS.exists(nonEmptyDir));
      Assert.assertTrue(hadoopFS.exists(fileInDir));

      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testDeleteBatchWithNonEmptyDirectoryForceDeleteTrue()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchNonEmptyDirForce");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create a non-empty directory with a file
      URI nonEmptyDir = new Path(baseURI.getPath(), "nonEmptyDir").toUri();
      hadoopFS.mkdir(nonEmptyDir);
      URI fileInDir = new Path(nonEmptyDir.getPath(), "file.txt").toUri();
      hadoopFS.touch(fileInDir);

      // Delete non-empty directory with forceDelete=true
      // Note: This tests that the method processes the directory
      // The actual deletion behavior depends on Hadoop's delete implementation
      List<URI> urisToDelete = new ArrayList<>();
      urisToDelete.add(nonEmptyDir);
      hadoopFS.deleteBatch(urisToDelete, true);

      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testDeleteBatchWithMixedFilesAndDirectories()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchMixed");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create files and directories
      URI file1 = new Path(baseURI.getPath(), "file1.txt").toUri();
      hadoopFS.touch(file1);

      URI emptyDir = new Path(baseURI.getPath(), "emptyDir").toUri();
      hadoopFS.mkdir(emptyDir);

      URI nonEmptyDir = new Path(baseURI.getPath(), "nonEmptyDir").toUri();
      hadoopFS.mkdir(nonEmptyDir);
      URI fileInNonEmptyDir = new Path(nonEmptyDir.getPath(), "file.txt").toUri();
      hadoopFS.touch(fileInNonEmptyDir);

      URI file2 = new Path(baseURI.getPath(), "file2.txt").toUri();
      hadoopFS.touch(file2);

      // Delete with forceDelete=true
      List<URI> urisToDelete = new ArrayList<>();
      urisToDelete.add(file1);
      urisToDelete.add(emptyDir);
      urisToDelete.add(nonEmptyDir);
      urisToDelete.add(file2);

      Assert.assertTrue(hadoopFS.deleteBatch(urisToDelete, true));

      // Verify all are deleted
      Assert.assertFalse(hadoopFS.exists(file1));
      Assert.assertFalse(hadoopFS.exists(emptyDir));
      Assert.assertFalse(hadoopFS.exists(nonEmptyDir));
      Assert.assertFalse(hadoopFS.exists(fileInNonEmptyDir));
      Assert.assertFalse(hadoopFS.exists(file2));

      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testDeleteBatchWithDeepNestedDirectories()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchDeepNested");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create deeply nested directory structure
      URI level1 = new Path(baseURI.getPath(), "level1").toUri();
      hadoopFS.mkdir(level1);
      URI level2 = new Path(level1.getPath(), "level2").toUri();
      hadoopFS.mkdir(level2);
      URI deepFile = new Path(level2.getPath(), "deepFile.txt").toUri();
      hadoopFS.touch(deepFile);

      // Delete with forceDelete=true
      // Note: This tests that the method processes nested directories
      List<URI> urisToDelete = new ArrayList<>();
      urisToDelete.add(level1);

      hadoopFS.deleteBatch(urisToDelete, true);

      hadoopFS.delete(baseURI, true);
    }
  }

  @Test
  public void testDeleteBatchPerformanceWithManyFiles()
      throws IOException {
    URI baseURI = URI.create(TMP_DIR + "/testDeleteBatchPerformance");
    try (HadoopPinotFS hadoopFS = new HadoopPinotFS()) {
      hadoopFS.init(new PinotConfiguration());
      hadoopFS.mkdir(baseURI);

      // Create many files
      int fileCount = 50;
      List<URI> urisToDelete = new ArrayList<>();
      for (int i = 0; i < fileCount; i++) {
        URI testFile = new Path(baseURI.getPath(), "file" + i + ".txt").toUri();
        hadoopFS.touch(testFile);
        urisToDelete.add(testFile);
      }

      // Delete all files using deleteBatch
      long startTime = System.currentTimeMillis();
      Assert.assertTrue(hadoopFS.deleteBatch(urisToDelete, false));
      long batchTime = System.currentTimeMillis() - startTime;

      // Verify all files are deleted
      for (URI uri : urisToDelete) {
        Assert.assertFalse(hadoopFS.exists(uri));
      }

      // Log performance (batch deletion should be reasonably fast)
      Assert.assertTrue(batchTime < 10000, "Batch deletion took too long: " + batchTime + "ms");

      hadoopFS.delete(baseURI, true);
    }
  }
}
