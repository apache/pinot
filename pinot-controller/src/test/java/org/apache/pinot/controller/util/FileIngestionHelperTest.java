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
package org.apache.pinot.controller.util;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


@Test(groups = "stateless")
public class FileIngestionHelperTest {
  private static final File DEST_FILE = new File("unused");

  @Test
  public void testCopiesLocalFileUriWhenLocalFileSystemIsEnabled()
      throws Exception {
    Path tempDir = Files.createTempDirectory("pinot-ingest-uri-test");
    try {
      Path inputFile = tempDir.resolve("input.csv");
      Path destFile = tempDir.resolve("dest.csv");
      Files.writeString(inputFile, "col\nvalue\n", StandardCharsets.UTF_8);

      FileIngestionHelper.copyURIToLocal(new HashMap<>(), inputFile.toUri(), destFile.toFile(), true);

      assertEquals(Files.readString(destFile, StandardCharsets.UTF_8), "col\nvalue\n");
    } finally {
      FileUtils.deleteQuietly(tempDir.toFile());
    }
  }

  @Test
  public void testRejectsLocalFileUriWhenLocalFileSystemIsDisabled() {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(new HashMap<>(), URI.create("file:///tmp/input.csv"), DEST_FILE,
            false));

    assertTrue(exception.getMessage().contains("Local filesystem URIs are not allowed"));
  }

  @Test
  public void testRejectsSchemeLessUriWhenLocalFileSystemIsDisabled() {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(new HashMap<>(), URI.create("/tmp/input.csv"), DEST_FILE, false));

    assertTrue(exception.getMessage().contains("Source URI must include a scheme"));
  }

  @Test
  public void testPreservesSchemeLessUriHandlingWhenLocalFileSystemIsEnabled() {
    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(new HashMap<>(), URI.create("/tmp/input.csv"), DEST_FILE, true));

    assertTrue(exception.getMessage().contains("Must provide input.fs.className"));
  }

  @Test
  public void testRejectsLocalFileSystemClassForCustomSchemeWhenLocalFileSystemIsDisabled() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_CLASS, LocalPinotFS.class.getName());

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(batchConfigMap, URI.create("customlocalfstest:///tmp/input.csv"),
            DEST_FILE, false));

    assertTrue(exception.getMessage().contains("Local filesystem implementation is not allowed"));
  }

  @Test
  public void testRejectsInvalidFileSystemClassForCustomScheme() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_CLASS,
        "org.apache.pinot.spi.filesystem.DoesNotExistPinotFS");

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(batchConfigMap, URI.create("invalidlocalfstest:///tmp/input.csv"),
            DEST_FILE, false));

    assertTrue(exception.getMessage().contains("Invalid input.fs.className for /ingestFromURI"));
  }

  @Test
  public void testRejectsLegacyLocalFileSystemClassForCustomSchemeWhenLocalFileSystemIsDisabled() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_CLASS, "org.apache.pinot.filesystem.LocalPinotFS");

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(batchConfigMap, URI.create("legacylocalfstest:///tmp/input.csv"),
            DEST_FILE, false));

    assertTrue(exception.getMessage().contains("Local filesystem implementation is not allowed"));
  }

  @Test
  public void testRejectsRegisteredLocalFileSystemSchemeWhenLocalFileSystemIsDisabled() {
    PinotFSFactory.register("registeredlocalfstest", LocalPinotFS.class.getName(), new PinotConfiguration());

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(new HashMap<>(), URI.create("registeredlocalfstest:///tmp/input.csv"),
            DEST_FILE, false));

    assertTrue(exception.getMessage().contains("Local filesystem implementation is not allowed"));
  }

  @Test
  public void testRejectsLocalFileSystemSubclassForCustomSchemeWhenLocalFileSystemIsDisabled() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.INPUT_FS_CLASS, TestLocalPinotFS.class.getName());

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> FileIngestionHelper.copyURIToLocal(batchConfigMap,
            URI.create("customlocalfssubclasstest:///tmp/input.csv"), DEST_FILE, false));

    assertTrue(exception.getMessage().contains("Local filesystem implementation is not allowed"));
  }

  public static class TestLocalPinotFS extends LocalPinotFS {
  }
}
