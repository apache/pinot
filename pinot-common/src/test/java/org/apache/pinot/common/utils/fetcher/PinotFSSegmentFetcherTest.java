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
package org.apache.pinot.common.utils.fetcher;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PinotFSSegmentFetcherTest {
  private static final String SEGMENT_NAME = "testSegment";
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(),
      PinotFSSegmentFetcherTest.class.getName());
  private static final File DATA_DIR = new File(TEMP_DIR, "dataDir");
  private static final File TAR_DIR = new File(TEMP_DIR, "tarDir");
  private static final File DOWNLOAD_DIR = new File(TEMP_DIR, "downloadDir");

  private PinotFSSegmentFetcher _segmentFetcher;

  @BeforeMethod
  public void setUp()
      throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(DATA_DIR);
    FileUtils.forceMkdir(TAR_DIR);
    FileUtils.forceMkdir(DOWNLOAD_DIR);

    // Initialize LocalPinotFS
    PinotFSFactory.register(PinotFSFactory.LOCAL_PINOT_FS_SCHEME, LocalPinotFS.class.getName(),
        new PinotConfiguration());

    // Setup fetcher config with retry settings
    PinotConfiguration fetcherConfig = new PinotConfiguration();
    fetcherConfig.setProperty(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 3);
    fetcherConfig.setProperty(BaseSegmentFetcher.RETRY_WAIT_MS_CONFIG_KEY, 10);
    fetcherConfig.setProperty(BaseSegmentFetcher.RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, 1.1);

    _segmentFetcher = new PinotFSSegmentFetcher();
    _segmentFetcher.init(fetcherConfig);
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  /**
   * Creates a tar.gz file containing a test segment directory with some test data
   */
  private File createTestSegmentTar(String segmentName, String fileContent)
      throws Exception {
    // Create a segment directory with a test file
    File segmentDir = new File(DATA_DIR, segmentName);
    FileUtils.forceMkdir(segmentDir);

    File dataFile = new File(segmentDir, "index");
    FileUtils.write(dataFile, fileContent, Charset.defaultCharset());

    // Create another file to simulate a real segment structure
    File metadataFile = new File(segmentDir, "metadata.properties");
    FileUtils.write(metadataFile, "segment.name=" + segmentName, Charset.defaultCharset());

    // Create tar.gz file
    File tarFile = new File(TAR_DIR, segmentName + ".tar.gz");
    TarCompressionUtils.createCompressedTarFile(segmentDir, tarFile);

    return tarFile;
  }

  @Test
  public void testFetchUntarSegmentToLocalStreamedSuccessFirstAttempt()
      throws Exception {
    // Create test segment tar
    String testContent = "This is test segment data";
    File segmentTar = createTestSegmentTar(SEGMENT_NAME, testContent);

    // Create URI for the tar file
    URI segmentUri = segmentTar.toURI();

    // Download and untar
    AtomicInteger failedAttempts = new AtomicInteger(0);
    File untarredSegment = _segmentFetcher.fetchUntarSegmentToLocalStreamed(
        segmentUri, DOWNLOAD_DIR, -1, failedAttempts);

    // Verify
    assertNotNull(untarredSegment, "Untarred segment should not be null");
    assertTrue(untarredSegment.exists(), "Untarred segment directory should exist");
    assertTrue(untarredSegment.isDirectory(), "Untarred segment should be a directory");
    assertEquals(untarredSegment.getName(), SEGMENT_NAME, "Segment directory name should match");
    assertEquals(failedAttempts.get(), 0, "Should succeed on first attempt");

    // Verify content
    File indexFile = new File(untarredSegment, "index");
    assertTrue(indexFile.exists(), "Index file should exist");
    String actualContent = FileUtils.readFileToString(indexFile, Charset.defaultCharset());
    assertEquals(actualContent, testContent, "File content should match");

    // Verify metadata file
    File metadataFile = new File(untarredSegment, "metadata.properties");
    assertTrue(metadataFile.exists(), "Metadata file should exist");
  }

  @Test
  public void testFetchUntarSegmentToLocalStreamedWithRateLimit()
      throws Exception {
    // Create test segment tar
    String testContent = "This is test segment data with rate limiting";
    File segmentTar = createTestSegmentTar(SEGMENT_NAME + "_ratelimited", testContent);

    // Create URI for the tar file
    URI segmentUri = segmentTar.toURI();

    // Download and untar with rate limit (1 MB/s)
    AtomicInteger failedAttempts = new AtomicInteger(0);
    long rateLimit = 1024 * 1024; // 1 MB/s
    File untarredSegment = _segmentFetcher.fetchUntarSegmentToLocalStreamed(
        segmentUri, DOWNLOAD_DIR, rateLimit, failedAttempts);

    // Verify
    assertNotNull(untarredSegment, "Untarred segment should not be null");
    assertTrue(untarredSegment.exists(), "Untarred segment directory should exist");
    assertEquals(failedAttempts.get(), 0, "Should succeed on first attempt");
  }

  @Test
  public void testFetchUntarSegmentToLocalStreamedWithNullScheme()
      throws Exception {
    // Create test segment tar
    String testContent = "Test content for null scheme";
    File segmentTar = createTestSegmentTar(SEGMENT_NAME + "_nullscheme", testContent);

    // Create URI without scheme (tests the if (uri.getScheme() == null) branch)
    URI segmentUri = new URI(null, segmentTar.getAbsolutePath(), null);

    // Download and untar
    AtomicInteger failedAttempts = new AtomicInteger(0);
    File untarredSegment = _segmentFetcher.fetchUntarSegmentToLocalStreamed(
        segmentUri, DOWNLOAD_DIR, -1, failedAttempts);

    // Verify
    assertNotNull(untarredSegment, "Untarred segment should not be null");
    assertTrue(untarredSegment.exists(), "Untarred segment directory should exist");
    assertEquals(failedAttempts.get(), 0, "Should succeed on first attempt");
  }

  @Test(expectedExceptions = AttemptsExceededException.class)
  public void testFetchUntarSegmentToLocalStreamedFailureExceedsRetries()
      throws Exception {
    // Use a non-existent file to force failures
    URI nonExistentUri = new File(TAR_DIR, "nonexistent.tar.gz").toURI();

    AtomicInteger failedAttempts = new AtomicInteger(0);
    try {
      _segmentFetcher.fetchUntarSegmentToLocalStreamed(
          nonExistentUri, DOWNLOAD_DIR, -1, failedAttempts);
    } catch (AttemptsExceededException e) {
      // Verify that all retries were exhausted
      assertEquals(failedAttempts.get(), 3, "Should have 3 failed retries");
      throw e;
    }
  }

  @Test
  public void testFetchUntarSegmentToLocalStreamedLargeFile()
      throws Exception {
    // Create a larger segment to test streaming behavior
    String segmentName = SEGMENT_NAME + "_large";
    File segmentDir = new File(DATA_DIR, segmentName);
    FileUtils.forceMkdir(segmentDir);

    // Create a file with more content (1KB of data)
    StringBuilder largeContent = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      largeContent.append("This is line ").append(i).append(" of test data.\n");
    }

    File dataFile = new File(segmentDir, "largeIndex");
    FileUtils.write(dataFile, largeContent.toString(), Charset.defaultCharset());

    // Create tar.gz file
    File tarFile = new File(TAR_DIR, segmentName + ".tar.gz");
    TarCompressionUtils.createCompressedTarFile(segmentDir, tarFile);

    // Download and untar
    URI segmentUri = tarFile.toURI();
    AtomicInteger attempts = new AtomicInteger(0);
    File untarredSegment = _segmentFetcher.fetchUntarSegmentToLocalStreamed(
        segmentUri, DOWNLOAD_DIR, -1, attempts);

    // Verify
    assertNotNull(untarredSegment);
    File largeIndexFile = new File(untarredSegment, "largeIndex");
    assertTrue(largeIndexFile.exists());
    String actualContent = FileUtils.readFileToString(largeIndexFile, Charset.defaultCharset());
    assertEquals(actualContent, largeContent.toString());
  }
}