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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.pinot.plugin.filesystem.GcsPinotFS.GCP_KEY;
import static org.apache.pinot.plugin.filesystem.GcsPinotFS.PROJECT_ID;
import static org.apache.pinot.plugin.filesystem.GcsUri.createGcsUri;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for GcsPinotFS
 *
 * Credentials to connect to gcs must be supplied via environment variables.
 * The following environment variables are used to connect to gcs:
 * GOOGLE_APPLICATION_CREDENTIALS: path to gcp json key file
 * GCP_PROJECT: the name of the project to use
 * GCS_BUCKET: the name of the bucket to use
 *
 * The reason we do not use RemoteStorageHelper is that create bucket
 * permissions are required. Pinot only needs to test creating objects.
 * The bucket should already exist.
 *
 * If credentials are not supplied then all tests are skipped.
 */
@Test(singleThreaded = true)
public class TestGcsPinotFS {
  private static final String DATA_DIR_PREFIX = "testing-data";

  private GcsPinotFS _pinotFS;
  private GcsUri _dataDir;
  private final Closer _closer = Closer.create();

  @BeforeClass
  public void setup() {
    String keyFile = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    String projectId = System.getenv("GCP_PROJECT");
    String bucket = System.getenv("GCS_BUCKET");
    if (keyFile != null && projectId != null && bucket != null) {
      _pinotFS = new GcsPinotFS();
      _pinotFS.init(new PinotConfiguration(
          ImmutableMap.<String, Object>builder().put(PROJECT_ID, projectId).put(GCP_KEY, keyFile).build()));
      _dataDir = createGcsUri(bucket, DATA_DIR_PREFIX + randomUUID());
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    if (_pinotFS != null) {
      _pinotFS.delete(_dataDir.getUri(), true);
      _closer.close();
    }
  }

  private void skipIfNotConfigured() {
    if (_pinotFS == null) {
      throw new SkipException("No google credentials supplied.");
    }
  }

  private Path createLocalTempDirectory()
      throws IOException {
    Path temporaryDirectory = Files.createDirectory(Paths.get("/tmp/" + DATA_DIR_PREFIX + "-" + randomUUID()));
    _closer.register(() -> deleteDirectory(temporaryDirectory.toFile()));
    return temporaryDirectory;
  }

  private GcsUri createTempDirectoryGcsUri() {
    return _dataDir.resolve("dir-" + randomUUID());
  }

  /**
   * Resolved gcs uri does not contain trailing delimiter, e.g. "/",
   * as the GcsUri.resolve() method uses Path.resolve() semantics.
   *
   * @param gcsUri
   * @return path with trailing delimiter
   */
  private static GcsUri appendSlash(GcsUri gcsUri) {
    return createGcsUri(gcsUri.getBucketName(), gcsUri.getPrefix());
  }

  private List<String> writeToFile(Path file, int count) {
    List<String> lines = IntStream.range(0, count).mapToObj(n -> "line " + n).collect(toList());

    try (BufferedWriter writer = Files.newBufferedWriter(file, UTF_8)) {
      lines.forEach(line -> {
        try {
          writer.write(line);
          writer.newLine();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return lines;
  }

  private Stream<GcsUri> listFilesToStream(GcsUri gcsUri)
      throws IOException {
    return Arrays.asList(_pinotFS.listFiles(gcsUri.getUri(), true)).stream().map(URI::create).map(GcsUri::new);
  }

  @Test
  public void testGcs()
      throws Exception {
    skipIfNotConfigured();
    // Create empty file
    Path localTmpDir = createLocalTempDirectory();
    Path emptyFile = localTmpDir.resolve("empty");
    emptyFile.toFile().createNewFile();

    // Create non-empty file
    Path file1 = localTmpDir.resolve("file1");
    List<String> expectedLinesFromFile = writeToFile(file1, 10);
    List<String> actualLinesFromFile = Files.readAllLines(file1, UTF_8);
    // Sanity check
    assertEquals(actualLinesFromFile, expectedLinesFromFile);

    // Gcs Temporary Directory
    GcsUri gcsDirectoryUri = createTempDirectoryGcsUri();
    Set<GcsUri> expectedElements = new HashSet<>();
    expectedElements.add(appendSlash(gcsDirectoryUri));

    // Test mkdir()
    // Create the temp directory, which also creates any missing parent paths
    _pinotFS.mkdir(gcsDirectoryUri.getUri());

    GcsUri emptyFileGcsUri = gcsDirectoryUri.resolve("empty");
    expectedElements.add(emptyFileGcsUri);

    // Copy the empty file
    _pinotFS.copyFromLocalFile(emptyFile.toFile(), emptyFileGcsUri.getUri());
    expectedElements.add(appendSlash(emptyFileGcsUri));

    // Test making a subdirectory with the same name as an object.
    // This is allowed in gcs
    _pinotFS.mkdir(emptyFileGcsUri.getUri());

    GcsUri nonEmptyFileGcsUri = gcsDirectoryUri.resolve("empty/file1");
    expectedElements.add(nonEmptyFileGcsUri);
    // Copy the non empty file to the new folder
    _pinotFS.copyFromLocalFile(file1.toFile(), nonEmptyFileGcsUri.getUri());

    // Test listFiles()
    // Check that all the files are there
    assertEquals(listFilesToStream(_dataDir).collect(toSet()), expectedElements);
    // Check that the non-empty file has the expected contents
    Path nonEmptyFileFromGcs = localTmpDir.resolve("nonEmptyFileFromGcs");
    _pinotFS.copyToLocalFile(nonEmptyFileGcsUri.getUri(), nonEmptyFileFromGcs.toFile());
    assertEquals(Files.readAllLines(nonEmptyFileFromGcs), expectedLinesFromFile);

    // Test gcs copy single file to file
    GcsUri nonEmptyFileGcsUriCopy = gcsDirectoryUri.resolve("empty/file2");
    _pinotFS.copy(nonEmptyFileGcsUri.getUri(), nonEmptyFileGcsUriCopy.getUri());
    assertTrue(listFilesToStream(gcsDirectoryUri).anyMatch(uri -> uri.equals(nonEmptyFileGcsUriCopy)),
        format("Cannot find file '%s'", nonEmptyFileGcsUriCopy));

    // Test gcs delete single file
    _pinotFS.delete(nonEmptyFileGcsUriCopy.getUri(), false);
    assertTrue(listFilesToStream(gcsDirectoryUri).allMatch(uri -> !uri.equals(nonEmptyFileGcsUriCopy)),
        format("Unexpected: found file '%s'", nonEmptyFileGcsUriCopy));

    // Test copy directory -> directory
    GcsUri gcsDirectoryUriCopy = createTempDirectoryGcsUri();
    _pinotFS.copy(gcsDirectoryUri.getUri(), gcsDirectoryUriCopy.getUri());

    Set<GcsUri> expectedElementsCopy = new HashSet<>();
    String directoryName = Paths.get(gcsDirectoryUri.getPath()).getFileName().toString();
    String directoryCopyName = Paths.get(gcsDirectoryUriCopy.getPath()).getFileName().toString();
    for (GcsUri element : ImmutableList.copyOf(expectedElements)) {
      expectedElementsCopy
          .add(createGcsUri(element.getBucketName(), element.getPath().replace(directoryName, directoryCopyName)));
    }
    expectedElementsCopy.addAll(expectedElements);
    assertEquals(listFilesToStream(_dataDir).collect(toSet()), expectedElementsCopy);
    // Test delete directory
    _pinotFS.delete(gcsDirectoryUriCopy.getUri(), true);
    assertEquals(listFilesToStream(_dataDir).collect(toSet()), expectedElements);

    // Test move directory
    _pinotFS.move(gcsDirectoryUri.getUri(), gcsDirectoryUriCopy.getUri(), true);
    expectedElementsCopy.removeAll(expectedElements);
    assertEquals(listFilesToStream(_dataDir).collect(toSet()), expectedElementsCopy);

    // Test move file to different directory
    GcsUri movedFileGcsUri = gcsDirectoryUriCopy.resolve("empty/file1");
    assertTrue(listFilesToStream(gcsDirectoryUri).allMatch(uri -> !uri.equals(nonEmptyFileGcsUri)));
    _pinotFS.move(movedFileGcsUri.getUri(), nonEmptyFileGcsUri.getUri(), false);
    assertTrue(listFilesToStream(gcsDirectoryUri).anyMatch(uri -> uri.equals(nonEmptyFileGcsUri)));
  }
}
