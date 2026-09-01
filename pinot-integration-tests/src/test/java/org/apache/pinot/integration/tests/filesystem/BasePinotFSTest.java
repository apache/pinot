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
package org.apache.pinot.integration.tests.filesystem;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.FileMetadata;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Base test class for PinotFS implementations.
 * This class provides methods to test all operations defined in the PinotFS interface.
 * To use this class:
 * 1. Create a subclass that extends BasePinotFSTest
 * 2. Implement the abstract methods to provide an instance of the PinotFS implementation to test
 * 3. Provide appropriate URIs for testing
 * 4. Override any test method if needed for specific implementation details
 */
public abstract class BasePinotFSTest {

  protected final String _uuid = UUID.randomUUID().toString();
  protected PinotFS _pinotFS;
  protected URI _baseDirectoryUri;
  protected File _localTempDir;
  protected PinotConfiguration _fsConfigs;

  protected PinotConfiguration getFsConfigs() {
    return new PinotConfiguration();
  }

  protected static String getEnvVar(String varName) {
    return System.getenv(varName);
  }

  /**
   * Provides an instance of the PinotFS implementation to test.
   * Implementations should initialize and return a properly configured PinotFS instance.
   *
   * @return PinotFS implementation to test
   */
  protected abstract PinotFS getPinotFS();

  /**
   * Provides the base URI where test files and directories will be created.
   * This should be a unique path to avoid interfering with other tests.
   *
   * @return Base URI for testing
   */
  protected abstract URI getBaseDirectoryUri()
      throws URISyntaxException;

  /**
   * Set up the test environment by creating a temporary directory
   * and initializing the PinotFS implementation.
   *
   * @throws Exception If setup fails
   */
  @BeforeClass
  public void setUp() throws Exception {
    _fsConfigs = getFsConfigs();
    _pinotFS = getPinotFS();
    _pinotFS.init(_fsConfigs);
    _baseDirectoryUri = getBaseDirectoryUri();
    _localTempDir = new File(FileUtils.getTempDirectory(), "pinot-fs-test-" + UUID.randomUUID());
    FileUtils.forceMkdir(_localTempDir);

    // Ensure base directory exists
    _pinotFS.mkdir(_baseDirectoryUri);
  }

  /**
   * Clean up resources after all tests.
   *
   * @throws Exception If cleanup fails
   */
  @AfterClass
  public void tearDown() throws Exception {
    // Clean up test files in the filesystem
    try {
      _pinotFS.delete(_baseDirectoryUri, true);
    } catch (Exception e) {
      // Ignore cleanup errors
    }

    // Clean up local temp directory
    FileUtils.deleteDirectory(_localTempDir);

    // Close the filesystem
    _pinotFS.close();
  }

  /**
   * Clean up resources after all test method.
   *
   * @throws Exception If cleanup fails
   */
  @AfterMethod
  public void cleanup() throws Exception {
    // Clean up test files in the filesystem
    try {
      _pinotFS.delete(_baseDirectoryUri, true);
      _pinotFS.mkdir(_baseDirectoryUri);
    } catch (Exception e) {
      // Ignore cleanup errors
    }
    // Clean up local temp directory
    FileUtils.deleteDirectory(_localTempDir);
    FileUtils.forceMkdir(_localTempDir);
  }

  @BeforeMethod(alwaysRun = true)
  protected void ensureEnabled(Method method) {
    if (disableTests()) {
      throw new SkipException("Skipping test " + method.getName());
    }
  }

  protected boolean disableTests() {
    return false;
  }

  /**
   * Test for the init method of PinotFS.
   * This method tests whether initialization works properly.
   */
  @Test
  public void testInit() {
    // This is implicitly tested in setUp()
    // Additional tests can be added by subclasses
    Assert.assertNotNull(_pinotFS, "PinotFS instance should be initialized");
  }

  /**
   * Test for the mkdir method of PinotFS.
   * Tests whether directories can be created properly.
   */
  @Test
  public void testMkdir() throws Exception {
    URI directoryUri = new URI(_baseDirectoryUri.toString() + "/testDir");
    URI nestedDirectoryUri = new URI(_baseDirectoryUri.toString() + "/testDir/nestedDir");

    // Create a directory
    boolean result = _pinotFS.mkdir(directoryUri);
    Assert.assertTrue(result, "mkdir should return true when successful");
    Assert.assertTrue(_pinotFS.exists(directoryUri), "Directory should exist after mkdir");
    Assert.assertTrue(_pinotFS.isDirectory(directoryUri), "URI should be a directory after mkdir");

    // Create a nested directory
    result = _pinotFS.mkdir(nestedDirectoryUri);
    Assert.assertTrue(result, "mkdir should return true when creating nested directories");
    Assert.assertTrue(_pinotFS.exists(nestedDirectoryUri), "Nested directory should exist after mkdir");

    // Ensure mkdir returns true for existing directory
    result = _pinotFS.mkdir(directoryUri);
    Assert.assertTrue(result, "mkdir should return true for existing directory");
  }

  /**
   * Test for the delete method of PinotFS.
   * Tests whether files and directories can be deleted properly.
   */
  @Test
  public void testDelete() throws Exception {
    // Create test directory and file
    URI directoryUri = new URI(_baseDirectoryUri.toString() + "/deleteTestDir");
    URI fileUri = new URI(_baseDirectoryUri.toString() + "/deleteTestDir/testFile");
    URI nestedDirUri = new URI(_baseDirectoryUri.toString() + "/deleteTestDir/nestedDir");
    URI nestedFileUri = new URI(_baseDirectoryUri.toString() + "/deleteTestDir/nestedDir/nestedFile");

    _pinotFS.mkdir(directoryUri);
    _pinotFS.mkdir(nestedDirUri);

    // Create a test file
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, fileUri);

    // Create a nested test file
    File nestedLocalFile = new File(_localTempDir, "nestedFile");
    FileUtils.writeStringToFile(nestedLocalFile, "nested content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(nestedLocalFile, nestedFileUri);

    // Delete a file
    boolean result = _pinotFS.delete(fileUri, false);
    Assert.assertTrue(result, "delete should return true when deleting a file");
    Assert.assertFalse(_pinotFS.exists(fileUri), "File should not exist after delete");

    // Try to delete a directory without force (should fail if not empty)
    result = _pinotFS.delete(directoryUri, false);
    Assert.assertFalse(result, "delete should return false when trying to delete non-empty directory without force");
    Assert.assertTrue(_pinotFS.exists(directoryUri), "Directory should still exist");

    // Force delete a directory with contents
    result = _pinotFS.delete(directoryUri, true);
    Assert.assertTrue(result, "delete should return true when force deleting a directory");
    Assert.assertFalse(_pinotFS.exists(directoryUri), "Directory should not exist after force delete");
    Assert.assertFalse(_pinotFS.exists(nestedDirUri), "Nested directory should not exist after force delete");
    Assert.assertFalse(_pinotFS.exists(nestedFileUri), "Nested file should not exist after force delete");
  }

  /**
   * Test for the deleteBatch method of PinotFS.
   * Tests whether multiple files can be deleted properly.
   */
  @Test
  public void testDeleteBatch() throws Exception {
    // Create test directory and files
    URI file1Uri = new URI(_baseDirectoryUri.toString() + "/batchDeleteTest1");
    URI file2Uri = new URI(_baseDirectoryUri.toString() + "/batchDeleteTest2");
    URI file3Uri = new URI(_baseDirectoryUri.toString() + "/batchDeleteTest3");

    // Create test files
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, file1Uri);
    _pinotFS.copyFromLocalFile(localFile, file2Uri);
    _pinotFS.copyFromLocalFile(localFile, file3Uri);

    // Delete batch
    boolean result = _pinotFS.deleteBatch(Arrays.asList(file1Uri, file2Uri, file3Uri), false);
    Assert.assertTrue(result, "deleteBatch should return true when all files are deleted");
    Assert.assertFalse(_pinotFS.exists(file1Uri), "File 1 should not exist after deleteBatch");
    Assert.assertFalse(_pinotFS.exists(file2Uri), "File 2 should not exist after deleteBatch");
    Assert.assertFalse(_pinotFS.exists(file3Uri), "File 3 should not exist after deleteBatch");
  }

  /**
   * Test for the move method of PinotFS.
   * Tests whether files and directories can be moved properly.
   */
  @Test
  public void testMove() throws Exception {
    // Create test files and directories
    URI srcFileUri = new URI(_baseDirectoryUri.toString() + "/moveSourceFile");
    URI dstFileUri = new URI(_baseDirectoryUri.toString() + "/moveDestFile");
    URI nonExistentParentUri = new URI(_baseDirectoryUri.toString() + "/nonExistentDir/moveDestFile");
    URI srcDirUri = new URI(_baseDirectoryUri.toString() + "/moveSourceDir");
    URI dstDirUri = new URI(_baseDirectoryUri.toString() + "/moveDestDir");

    // Create source file
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "move test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, srcFileUri);

    // Create source directory with a file
    _pinotFS.mkdir(srcDirUri);
    URI srcDirFileUri = new URI(srcDirUri + "/dirFile");
    _pinotFS.copyFromLocalFile(localFile, srcDirFileUri);

    // Test moving a file
    boolean result = _pinotFS.move(srcFileUri, dstFileUri, false);
    Assert.assertTrue(result, "move should return true when successful");
    Assert.assertFalse(_pinotFS.exists(srcFileUri), "Source file should not exist after move");
    Assert.assertTrue(_pinotFS.exists(dstFileUri), "Destination file should exist after move");

    // Test moving to a non-existent parent directory (should create parent)
    _pinotFS.copyFromLocalFile(localFile, srcFileUri); // Recreate source file
    result = _pinotFS.move(srcFileUri, nonExistentParentUri, false);
    Assert.assertTrue(result, "move should return true when moving to non-existent parent");
    Assert.assertFalse(_pinotFS.exists(srcFileUri), "Source file should not exist after move");
    Assert.assertTrue(_pinotFS.exists(nonExistentParentUri), "Destination file should exist after move");

    // Test moving a directory
    result = _pinotFS.move(srcDirUri, dstDirUri, false);
    Assert.assertTrue(result, "move should return true when moving a directory");
    Assert.assertFalse(_pinotFS.exists(srcDirUri), "Source directory should not exist after move");
    Assert.assertTrue(_pinotFS.exists(dstDirUri), "Destination directory should exist after move");
    Assert.assertTrue(_pinotFS.exists(new URI(dstDirUri + "/dirFile")),
        "Files within moved directory should exist at destination");

    // Test overwrite flag
    _pinotFS.copyFromLocalFile(localFile, srcFileUri); // Recreate source file
    result = _pinotFS.move(srcFileUri, dstFileUri, true); // dstFileUri already exists from previous test
    Assert.assertTrue(result, "move should return true when overwriting existing file");
    Assert.assertFalse(_pinotFS.exists(srcFileUri), "Source file should not exist after move with overwrite");
    Assert.assertTrue(_pinotFS.exists(dstFileUri), "Destination file should exist after move with overwrite");

    // Test without overwrite (should fail if destination exists)
    _pinotFS.copyFromLocalFile(localFile, srcFileUri); // Recreate source file
    result = _pinotFS.move(srcFileUri, dstFileUri, false); // dstFileUri already exists
    Assert.assertFalse(result, "move should return false when destination exists and overwrite is false");
    Assert.assertTrue(_pinotFS.exists(srcFileUri), "Source file should still exist when move fails");
  }

  /**
   * Test for the copy methods of PinotFS.
   * Tests whether files and directories can be copied properly.
   */
  @Test
  public void testCopy() throws Exception {
    // Create test files and directories
    URI srcFileUri = new URI(_baseDirectoryUri.toString() + "/copySourceFile");
    URI dstFileUri = new URI(_baseDirectoryUri.toString() + "/copyDestFile");
    URI srcDirUri = new URI(_baseDirectoryUri.toString() + "/copySourceDir");
    URI dstDirUri = new URI(_baseDirectoryUri.toString() + "/copyDestDir");
    URI nestedDirUri = new URI(srcDirUri + "/nestedDir");
    URI nestedFileUri = new URI(nestedDirUri + "/nestedFile");

    // Create source file
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "copy test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, srcFileUri);

    // Create source directory with nested structure
    _pinotFS.mkdir(srcDirUri);
    _pinotFS.mkdir(nestedDirUri);
    _pinotFS.copyFromLocalFile(localFile, nestedFileUri);

    // Test copying a file
    boolean result = _pinotFS.copy(srcFileUri, dstFileUri);
    Assert.assertTrue(result, "copy should return true when successful");
    Assert.assertTrue(_pinotFS.exists(srcFileUri), "Source file should still exist after copy");
    Assert.assertTrue(_pinotFS.exists(dstFileUri), "Destination file should exist after copy");

    // Test copying a directory
    result = _pinotFS.copyDir(srcDirUri, dstDirUri);
    Assert.assertTrue(result, "copyDir should return true when successful");
    Assert.assertTrue(_pinotFS.exists(srcDirUri), "Source directory should still exist after copyDir");
    Assert.assertTrue(_pinotFS.exists(dstDirUri), "Destination directory should exist after copyDir");
    Assert.assertTrue(_pinotFS.exists(new URI(dstDirUri + "/nestedDir/nestedFile")),
        "Nested files should be copied properly");

    // Verify content was copied correctly
    String srcContent;
    try (InputStream stream = _pinotFS.open(srcFileUri)) {
      srcContent = IOUtils.toString(stream, StandardCharsets.UTF_8);
    }

    String dstContent;
    try (InputStream stream = _pinotFS.open(dstFileUri)) {
      dstContent = IOUtils.toString(stream, StandardCharsets.UTF_8);
    }

    Assert.assertEquals(dstContent, srcContent, "Copied file content should match source");
  }

  /**
   * Test for the exists method of PinotFS.
   * Tests whether file existence can be checked properly.
   */
  @Test
  public void testExists() throws Exception {
    URI fileUri = new URI(_baseDirectoryUri.toString() + "/existsTestFile");
    URI dirUri = new URI(_baseDirectoryUri.toString() + "/existsTestDir");
    URI nonExistentUri = new URI(_baseDirectoryUri.toString() + "/nonExistentFile");

    // Create a test file
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, fileUri);

    // Create a test directory
    _pinotFS.mkdir(dirUri);

    // Test existing file and directory
    Assert.assertTrue(_pinotFS.exists(fileUri), "exists should return true for existing file");
    Assert.assertTrue(_pinotFS.exists(dirUri), "exists should return true for existing directory");

    // Test non-existent file
    Assert.assertFalse(_pinotFS.exists(nonExistentUri), "exists should return false for non-existent file");
  }

  /**
   * Test for the length method of PinotFS.
   * Tests whether file length can be obtained properly.
   */
  @Test
  public void testLength() throws Exception {
    URI fileUri = new URI(_baseDirectoryUri.toString() + "/lengthTestFile");
    URI dirUri = new URI(_baseDirectoryUri.toString() + "/lengthTestDir");

    // Create a test file with known content
    String fileContent = "test content for length";
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, fileContent, StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, fileUri);

    // Create a test directory
    _pinotFS.mkdir(dirUri);

    // Test file length
    long length = _pinotFS.length(fileUri);
    Assert.assertEquals(length, fileContent.getBytes(StandardCharsets.UTF_8).length,
        "length should return correct file size");

    // Test length on directory (should throw exception)
    try {
      _pinotFS.length(dirUri);
      Assert.fail("length should throw exception when called on a directory");
    } catch (Exception e) {
      // Expected exception
    }
  }

  /**
   * Test for the listFiles method of PinotFS.
   * Tests whether files and directories can be listed properly.
   */
  @Test
  public void testListFiles() throws Exception {
    URI dirUri = new URI(_baseDirectoryUri.toString() + "/listTestDir");
    URI nestedDirUri = new URI(dirUri + "/nestedDir");
    URI file1Uri = new URI(dirUri + "/file1");
    URI file2Uri = new URI(dirUri + "/file2");
    URI nestedFileUri = new URI(nestedDirUri + "/nestedFile");

    // Create test directories and files
    _pinotFS.mkdir(dirUri);
    _pinotFS.mkdir(nestedDirUri);

    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, file1Uri);
    _pinotFS.copyFromLocalFile(localFile, file2Uri);
    _pinotFS.copyFromLocalFile(localFile, nestedFileUri);

    // Test non-recursive listing
    String[] files = _pinotFS.listFiles(dirUri, false);
    Assert.assertEquals(files.length, 3, "listFiles should return 3 entries for non-recursive listing");

    // Verify the files exist in the listing
    boolean foundFile1 = false;
    boolean foundFile2 = false;
    boolean foundNestedDir = false;

    for (String file : files) {
      if (file.endsWith("/file1")) {
        foundFile1 = true;
      } else if (file.endsWith("/file2")) {
        foundFile2 = true;
      } else if (file.endsWith("/nestedDir")) {
        foundNestedDir = true;
      }
    }

    Assert.assertTrue(foundFile1, "listFiles should include file1");
    Assert.assertTrue(foundFile2, "listFiles should include file2");
    Assert.assertTrue(foundNestedDir, "listFiles should include nestedDir");

    // Test recursive listing
    files = _pinotFS.listFiles(dirUri, true);
    Assert.assertEquals(files.length, 4, "listFiles should return 4 entries for recursive listing");

    // Verify nested file is included
    boolean foundNestedFile = false;

    for (String file : files) {
      if (file.endsWith("/nestedDir/nestedFile")) {
        foundNestedFile = true;
        break;
      }
    }

    Assert.assertTrue(foundNestedFile, "recursive listFiles should include nestedFile");
  }

  /**
   * Test for the listFilesWithMetadata method of PinotFS.
   * Tests whether files and directories can be listed with metadata properly.
   */
  @Test
  public void testListFilesWithMetadata() throws Exception {
    // Skip test if not implemented
    try {
      _pinotFS.listFilesWithMetadata(URI.create("dummy://uri"), false);
    } catch (UnsupportedOperationException e) {
      return; // Skip test
    } catch (Exception e) {
      // Continue with test
    }

    URI dirUri = new URI(_baseDirectoryUri.toString() + "/listMetadataTestDir");
    URI fileUri = new URI(dirUri + "/testFile");
    URI nestedDirUri = new URI(dirUri + "/nestedDir");
    URI nestedFileUri = new URI(nestedDirUri + "/nestedFile");

    // Create test directories and files
    _pinotFS.mkdir(dirUri);
    _pinotFS.mkdir(nestedDirUri);

    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, fileUri);
    _pinotFS.copyFromLocalFile(localFile, nestedFileUri);

    // Test non-recursive listing
    List<FileMetadata> metadata = _pinotFS.listFilesWithMetadata(dirUri, false);
    Assert.assertEquals(metadata.size(), 2, "listFilesWithMetadata should return 2 entries for non-recursive listing");

    // Verify file metadata
    for (FileMetadata entry : metadata) {
      if (entry.getFilePath().endsWith("/testFile")) {
        Assert.assertFalse(entry.isDirectory(), "File should not be marked as directory");
        Assert.assertEquals(entry.getLength(), "test content".getBytes(StandardCharsets.UTF_8).length,
            "File length should be correct");
        Assert.assertTrue(entry.getLastModifiedTime() > 0, "Last modified time should be positive");
      } else if (entry.getFilePath().endsWith("/nestedDir")) {
        Assert.assertTrue(entry.isDirectory(), "Directory should be marked as directory");
      } else {
        Assert.fail("Unexpected entry in metadata list: " + entry.getFilePath());
      }
    }

    // Test recursive listing
    metadata = _pinotFS.listFilesWithMetadata(dirUri, true);
    Assert.assertEquals(metadata.size(), 3, "listFilesWithMetadata should return 3 entries for recursive listing");

    // Verify nested file is included
    boolean foundNestedFile = false;

    for (FileMetadata entry : metadata) {
      if (entry.getFilePath().endsWith("/nestedDir/nestedFile")) {
        foundNestedFile = true;
        Assert.assertFalse(entry.isDirectory(), "Nested file should not be marked as directory");
        Assert.assertEquals(entry.getLength(), "test content".getBytes(StandardCharsets.UTF_8).length,
            "Nested file length should be correct");
        break;
      }
    }

    Assert.assertTrue(foundNestedFile, "recursive listFilesWithMetadata should include nestedFile");
  }

  /**
   * Test for the copyToLocalFile and copyFromLocalFile methods of PinotFS.
   * Tests whether files can be copied to and from the local filesystem properly.
   */
  @Test
  public void testCopyToFromLocalFile() throws Exception {
    URI fileUri = new URI(_baseDirectoryUri.toString() + "/localCopyTestFile");

    // Create a test file with known content
    String fileContent = "test content for local copy";
    File srcLocalFile = new File(_localTempDir, "srcLocalFile");
    FileUtils.writeStringToFile(srcLocalFile, fileContent, StandardCharsets.UTF_8);

    // Test copyFromLocalFile
    _pinotFS.copyFromLocalFile(srcLocalFile, fileUri);
    Assert.assertTrue(_pinotFS.exists(fileUri), "File should exist after copyFromLocalFile");

    // Test copyToLocalFile
    File dstLocalFile = new File(_localTempDir, "dstLocalFile");
    _pinotFS.copyToLocalFile(fileUri, dstLocalFile);
    Assert.assertTrue(dstLocalFile.exists(), "Local file should exist after copyToLocalFile");

    // Verify content
    String localContent = FileUtils.readFileToString(dstLocalFile, StandardCharsets.UTF_8);
    Assert.assertEquals(localContent, fileContent, "Content should be preserved in local copy");
  }

  /**
   * Test for the isDirectory method of PinotFS.
   * Tests whether directories can be identified properly.
   */
  @Test
  public void testIsDirectory() throws Exception {
    URI fileUri = new URI(_baseDirectoryUri.toString() + "/directoryTestFile");
    URI dirUri = new URI(_baseDirectoryUri.toString() + "/directoryTestDir");
    URI nonExistentUri = new URI(_baseDirectoryUri.toString() + "/nonExistentPath");

    // Create a test file
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, fileUri);

    // Create a test directory
    _pinotFS.mkdir(dirUri);

    // Test isDirectory
    Assert.assertTrue(_pinotFS.isDirectory(dirUri), "isDirectory should return true for directory");
    Assert.assertFalse(_pinotFS.isDirectory(fileUri), "isDirectory should return false for file");

    // Test non-existent path
    try {
      _pinotFS.isDirectory(nonExistentUri);
      // Some implementations might return false instead of throwing an exception
      // so we don't assert here
    } catch (IOException e) {
      // Expected exception in some implementations
    }
  }

  /**
   * Test for the lastModified method of PinotFS.
   * Tests whether the last modified time can be obtained properly.
   */
  @Test
  public void testLastModified() throws Exception {
    URI fileUri = new URI(_baseDirectoryUri.toString() + "/lastModifiedTestFile");

    // Create a test file
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, fileUri);

    // Test lastModified
    long lastModified = _pinotFS.lastModified(fileUri);
    Assert.assertTrue(lastModified > 0, "lastModified should return a positive timestamp");

    // Wait a moment and modify the file
    Thread.sleep(1000); // Sleep to ensure timestamp difference

    // Update the file
    FileUtils.writeStringToFile(localFile, "updated content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, fileUri);

    // Check lastModified is updated
    long newLastModified = _pinotFS.lastModified(fileUri);
    Assert.assertTrue(newLastModified >= lastModified,
        "New lastModified should be greater than or equal to previous value");
  }

  /**
   * Test for the touch method of PinotFS.
   * Tests whether files can be touched (created or updated timestamp) properly.
   */
  @Test
  public void testTouch() throws Exception {
    URI fileUri = new URI(_baseDirectoryUri.toString() + "/touchTestFile");
    URI newFileUri = new URI(_baseDirectoryUri.toString() + "/touchNewFile");

    // Create a test file
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, "test content", StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, fileUri);

    // Get initial last modified time
    long initialModTime = _pinotFS.lastModified(fileUri);

    // Wait a moment to ensure timestamp difference
    Thread.sleep(1000);

    // Test touch on existing file
    boolean result = _pinotFS.touch(fileUri);
    Assert.assertTrue(result, "touch should return true when successful");

    // Verify timestamp was updated
    long newModTime = _pinotFS.lastModified(fileUri);
    Assert.assertTrue(newModTime > initialModTime, "touch should update last modified time");

    // Test touch on new file
    result = _pinotFS.touch(newFileUri);
    Assert.assertTrue(result, "touch should return true when creating new file");
    Assert.assertTrue(_pinotFS.exists(newFileUri), "touch should create file if it doesn't exist");

    // Test touch on file with non-existent parent
    URI nonExistentParentUri = new URI(_baseDirectoryUri.toString() + "/nonExistentDir/touchFile");
    try {
      _pinotFS.touch(nonExistentParentUri);
      // Some implementations might create parent directories
      if (_pinotFS.exists(nonExistentParentUri)) {
        // If touch succeeded, the parent directory should have been created
        Assert.assertTrue(_pinotFS.exists(new URI(_baseDirectoryUri.toString() + "/nonExistentDir")),
            "Parent directory should exist if touch succeeded");
      }
    } catch (IOException e) {
      // Expected exception in some implementations that don't create parent directories
    }
  }

  /**
   * Test for the open method of PinotFS.
   * Tests whether files can be opened and read properly.
   */
  @Test
  public void testOpen() throws Exception {
    URI fileUri = new URI(_baseDirectoryUri.toString() + "/openTestFile");

    // Create a test file with known content
    String fileContent = "test content for open";
    File localFile = new File(_localTempDir, "testFile");
    FileUtils.writeStringToFile(localFile, fileContent, StandardCharsets.UTF_8);
    _pinotFS.copyFromLocalFile(localFile, fileUri);

    // Test opening and reading the file
    try (InputStream inputStream = _pinotFS.open(fileUri)) {
      String readContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
      Assert.assertEquals(readContent, fileContent, "Content read from input stream should match original content");
    }

    // Test open on non-existent file
    URI nonExistentUri = new URI(_baseDirectoryUri.toString() + "/nonExistentFile");
    try {
      _pinotFS.open(nonExistentUri);
      Assert.fail("open should throw exception for non-existent file");
    } catch (IOException e) {
      // Expected exception
    }

    // Test open on directory
    URI dirUri = new URI(_baseDirectoryUri.toString() + "/openTestDir");
    _pinotFS.mkdir(dirUri);

    try {
      _pinotFS.open(dirUri);
      // Some implementations might not throw an exception, but return an empty stream
    } catch (IOException e) {
      // Expected exception in some implementations
    }
  }
}
