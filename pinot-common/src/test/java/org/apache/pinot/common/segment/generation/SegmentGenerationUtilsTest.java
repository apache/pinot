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

package org.apache.pinot.common.segment.generation;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SegmentGenerationUtilsTest {

  @Test
  public void testExtractFileNameFromURI() {
    Assert.assertEquals(SegmentGenerationUtils.getFileName(URI.create("file:/var/data/myTable/2020/04/06/input.data")),
        "input.data");
    Assert.assertEquals(SegmentGenerationUtils.getFileName(URI.create("/var/data/myTable/2020/04/06/input.data")),
        "input.data");
    Assert.assertEquals(SegmentGenerationUtils
            .getFileName(URI.create("dbfs:/mnt/mydb/mytable/pt_year=2020/pt_month=4/pt_date=2020-04-01/input.data")),
        "input.data");
    Assert.assertEquals(SegmentGenerationUtils.getFileName(URI.create("hdfs://var/data/myTable/2020/04/06/input.data")),
        "input.data");

    Assert.assertEquals(SegmentGenerationUtils.getFileName(
            URI.create(SegmentGenerationUtils.sanitizeURIString("hdfs://var/data/my Table/2020/04/06/input.data"))),
        "input.data");
    Assert.assertEquals(SegmentGenerationUtils.getFileName(
            URI.create(SegmentGenerationUtils.sanitizeURIString("hdfs://var/data/my Table/2020/04/06/input 2.data"))),
        "input 2.data");
  }

  // Confirm output path generation works with URIs that have authority/userInfo.
  @Test
  public void testRelativeURIs()
      throws URISyntaxException {
    URI inputDirURI = new URI("hdfs://namenode1:9999/path/to/");
    URI inputFileURI = new URI("hdfs://namenode1:9999/path/to/subdir/file");
    URI outputDirURI = new URI("hdfs://namenode2/output/dir/");
    URI segmentTarFileName = new URI("file.tar.gz");
    URI outputSegmentTarURI = SegmentGenerationUtils.getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI)
        .resolve(segmentTarFileName);
    Assert.assertEquals(outputSegmentTarURI.toString(), "hdfs://namenode2/output/dir/subdir/file.tar.gz");
  }

  // Invalid segment tar name with space
  @Test
  public void testInvalidRelativeURIs()
      throws URISyntaxException {
    URI inputDirURI = new URI("hdfs://namenode1:9999/path/to/");
    URI inputFileURI = new URI("hdfs://namenode1:9999/path/to/subdir/file");
    URI outputDirURI = new URI("hdfs://namenode2/output/dir/");
    try {
      SegmentGenerationUtils.getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI)
          .resolve(new URI("table_OFFLINE_2021-02-01_09:39:00.000_2021-02-01_11:59:00.000_2.tar.gz"));
      Assert.fail("Expected an error thrown for uri resolve with space in segment name");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof URISyntaxException);
    }
    URI outputSegmentTarURI = SegmentGenerationUtils.getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI)
        .resolve(new URI(URIUtils.encode("table_OFFLINE_2021-02-01_09:39:00.000_2021-02-01_11:59:00.000_2.tar.gz")));
    Assert.assertEquals(outputSegmentTarURI.toString(),
        "hdfs://namenode2/output/dir/subdir/table_OFFLINE_2021-02-01_09%3A39%3A00.000_2021-02-01_11%3A59%3A00.000_2"
            + ".tar.gz");
  }

  // Don't lose authority portion of inputDirURI when creating output files
  // https://github.com/apache/pinot/issues/6355

  @Test
  public void testGetFileURI()
      throws Exception {
    // Raw path without scheme
    Assert.assertEquals(SegmentGenerationUtils.getFileURI("/path/to/file", new URI("file:/path/to")).toString(),
        "file:/path/to/file");
    Assert
        .assertEquals(SegmentGenerationUtils.getFileURI("/path/to/file", new URI("hdfs://namenode/path/to")).toString(),
            "hdfs://namenode/path/to/file");
    Assert.assertEquals(SegmentGenerationUtils.getFileURI("/path/to/file", new URI("hdfs:///path/to")).toString(),
        "hdfs:/path/to/file");

    // Typical local file URI
    validateFileURI(new URI("file:/path/to/"));
    // Remote hostname.
    validateFileURI(new URI("file://hostname/path/to/"));
    // Local hostname
    validateFileURI(new URI("file:///path/to/"), "file:/path/to/");

    // Regular HDFS path
    validateFileURI(new URI("hdfs:///path/to/"), "hdfs:/path/to/");

    // Namenode as authority, plus non-standard port
    validateFileURI(new URI("hdfs://namenode:9999/path/to/"));

    // S3 bucket + path
    validateFileURI(new URI("s3://bucket/path/to/"));

    // S3 URI with userInfo (username/password)
    validateFileURI(new URI("s3://username:password@bucket/path/to/"));
  }

  private void validateFileURI(URI directoryURI, String expectedPrefix)
      throws URISyntaxException {
    URI fileURI = new URI(directoryURI.toString() + "file");
    String rawPath = fileURI.getRawPath();

    Assert.assertEquals(SegmentGenerationUtils.getFileURI(rawPath, fileURI).toString(), expectedPrefix + "file");
  }

  private void validateFileURI(URI directoryURI)
      throws URISyntaxException {
    validateFileURI(directoryURI, directoryURI.toString());
  }

  // Test that we search files recursively when recursiveSearch option is set to true.
  @Test
  public void testMatchFilesRecursiveSearchOnRecursiveInputFilePattern()
      throws Exception {
    File testDir = makeTestDir();
    File inputDir = new File(testDir, "input");
    File inputSubDir1 = new File(inputDir, "2009");
    inputSubDir1.mkdirs();

    File inputFile1 = new File(inputDir, "input.csv");
    FileUtils.writeLines(inputFile1, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    File inputFile2 = new File(inputSubDir1, "input.csv");
    FileUtils.writeLines(inputFile2, Lists.newArrayList("col1,col2", "value3,3", "value4,4"));
    URI inputDirURI = new URI(inputDir.getAbsolutePath());
    if (inputDirURI.getScheme() == null) {
      inputDirURI = new File(inputDir.getAbsolutePath()).toURI();
    }
    PinotFS inputDirFS = PinotFSFactory.create(inputDirURI.getScheme());
    String includePattern = "glob:" + inputDir.getAbsolutePath() + "/**.csv";
    List<String> files =
        SegmentGenerationUtils.listMatchedFilesWithRecursiveOption(inputDirFS, inputDirURI, includePattern, null, true);
    Assert.assertEquals(files.size(), 2);
  }

  // Test that we don't search files recursively when recursiveSearch option is set to false.
  @Test
  public void testMatchFilesRecursiveSearchOnNonRecursiveInputFilePattern()
      throws Exception {
    File testDir = makeTestDir();
    File inputDir = new File(testDir, "dir");
    File inputSubDir1 = new File(inputDir, "2009");
    inputSubDir1.mkdirs();

    File inputFile1 = new File(inputDir, "input.csv");
    FileUtils.writeLines(inputFile1, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    File inputFile2 = new File(inputSubDir1, "input.csv");
    FileUtils.writeLines(inputFile2, Lists.newArrayList("col1,col2", "value3,3", "value4,4"));
    URI inputDirURI = new URI(inputDir.getAbsolutePath());
    if (inputDirURI.getScheme() == null) {
      inputDirURI = new File(inputDir.getAbsolutePath()).toURI();
    }
    PinotFS inputDirFS = PinotFSFactory.create(inputDirURI.getScheme());
    String includePattern = "glob:" + inputDir.getAbsolutePath() + "/*.csv";

    List<String> files =
        SegmentGenerationUtils.listMatchedFilesWithRecursiveOption(inputDirFS, inputDirURI, includePattern, null,
            false);
    Assert.assertEquals(files.size(), 1);
  }

  // Test that we exclude files that match exclude pattern.
  @Test
  public void testMatchFilesRecursiveSearchExcludeFilePattern()
      throws Exception {
    File testDir = makeTestDir();
    File inputDir = new File(testDir, "dir");
    File inputSubDir1 = new File(inputDir, "2009");
    inputSubDir1.mkdirs();

    File inputFile1 = new File(inputDir, "input1.csv");
    FileUtils.writeLines(inputFile1, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    File inputFile2 = new File(inputSubDir1, "input2.csv");
    FileUtils.writeLines(inputFile2, Lists.newArrayList("col1,col2", "value3,3", "value4,4"));
    URI inputDirURI = new URI(inputDir.getAbsolutePath());
    if (inputDirURI.getScheme() == null) {
      inputDirURI = new File(inputDir.getAbsolutePath()).toURI();
    }
    PinotFS inputDirFS = PinotFSFactory.create(inputDirURI.getScheme());
    String includePattern = "glob:" + inputDir.getAbsolutePath() + "/**.csv";
    String excludePattern = "glob:" + inputDir.getAbsolutePath() + "/2009/input2.csv";

    List<String> files =
        SegmentGenerationUtils.listMatchedFilesWithRecursiveOption(inputDirFS, inputDirURI, includePattern,
            excludePattern, true);
    Assert.assertEquals(files.size(), 1);
  }

  // Test that we throw an exception when there is no file matching.
  @Test
  public void testEmptyMatchFiles()
      throws Exception {
    File testDir = makeTestDir();
    File inputDir = new File(testDir, "dir");
    File inputSubDir1 = new File(inputDir, "2009");
    inputSubDir1.mkdirs();

    File inputFile1 = new File(inputDir, "input1.csv");
    FileUtils.writeLines(inputFile1, Lists.newArrayList("col1,col2", "value1,1", "value2,2"));

    File inputFile2 = new File(inputSubDir1, "input2.csv");
    FileUtils.writeLines(inputFile2, Lists.newArrayList("col1,col2", "value3,3", "value4,4"));
    URI inputDirURI = new File(inputDir.getAbsolutePath()).toURI();
    PinotFS inputDirFS = PinotFSFactory.create(inputDirURI.getScheme());
    String includePattern = "glob:" + inputDir.getAbsolutePath() + "/**.json";
    Assert.assertThrows(RuntimeException.class, () -> {
      SegmentGenerationUtils.listMatchedFilesWithRecursiveOption(inputDirFS, inputDirURI, includePattern, null, true);
    });
  }

  @Test
  public void testRelativeURIsWithSpaces()
          throws URISyntaxException {
    URI inputDirURI = new URI("hdfs://namenode1:9999/path/to/");
    URI inputFileURI = new URI(SegmentGenerationUtils.sanitizeURIString("hdfs://namenode1:9999/path/to/sub dir/file"));
    URI outputDirURI = new URI("hdfs://namenode2/output/dir/");
    URI segmentTarFileName = new URI("file.tar.gz");
    URI outputSegmentTarURI = SegmentGenerationUtils.getRelativeOutputPath(inputDirURI, inputFileURI, outputDirURI)
            .resolve(segmentTarFileName);
    Assert.assertEquals(outputSegmentTarURI.toString(), "hdfs://namenode2/output/dir/sub_dir/file.tar.gz");
  }

  private File makeTestDir()
      throws IOException {
    File testDir = Files.createTempDirectory("testSegmentGeneration-").toFile();
    testDir.delete();
    testDir.mkdirs();
    return testDir;
  }
}
