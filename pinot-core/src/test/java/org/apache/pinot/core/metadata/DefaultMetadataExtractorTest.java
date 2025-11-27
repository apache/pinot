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
package org.apache.pinot.core.metadata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Properties;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.core.metadata.DefaultMetadataExtractor.SEGMENT_METADATA_DIR_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class DefaultMetadataExtractorTest {

  private static final String SEGMENT_NAME = "testSegment_0";

  private File _tempDir;
  private File _tarredSegmentFile;
  private File _unzippedSegmentDir;
  private DefaultMetadataExtractor _metadataExtractor;

  @BeforeMethod
  public void setUp() throws IOException {
    _tempDir = Files.createTempDirectory("DefaultMetadataExtractorTest").toFile();
    _tarredSegmentFile = new File(_tempDir, "testSegment.tar.gz");
    _unzippedSegmentDir = new File(_tempDir, "unzipped");
    _metadataExtractor = new DefaultMetadataExtractor();
  }

  @AfterMethod
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testExtractMetadataWithDirectoryStructure() throws Exception {
    createTarWithDirectoryStructure();
    SegmentMetadata metadata = _metadataExtractor.extractMetadata(_tarredSegmentFile, _unzippedSegmentDir);

    assertNotNull(metadata);
    assertEquals(metadata.getName(), SEGMENT_NAME);

    // Verify that the segment directory was used directly (not moved to segment_metadata)
    File segmentDir = new File(_unzippedSegmentDir, SEGMENT_METADATA_DIR_NAME);
    assertTrue(segmentDir.exists());
    assertTrue(segmentDir.isDirectory());
    assertTrue(new File(segmentDir, "metadata.properties").exists());
  }

  @Test
  public void testExtractMetadataWithFlatFileStructure() throws Exception {
    createTarWithFlatFileStructure();

    SegmentMetadata metadata = _metadataExtractor.extractMetadata(_tarredSegmentFile, _unzippedSegmentDir);

    assertNotNull(metadata);
    assertEquals(metadata.getName(), SEGMENT_NAME);

    // Verify that files were moved to segment_metadata subdirectory
    File segmentMetadataDir = new File(_unzippedSegmentDir, SEGMENT_METADATA_DIR_NAME);
    assertTrue(segmentMetadataDir.exists());
    assertTrue(segmentMetadataDir.isDirectory());
    assertTrue(new File(segmentMetadataDir, "metadata.properties").exists());
    assertTrue(new File(segmentMetadataDir, "creation.meta").exists());
  }

  @Test(expectedExceptions = Exception.class)
  public void testExtractMetadataWithInvalidTarFile() throws Exception {
    Files.write(_tarredSegmentFile.toPath(), "invalid tar content".getBytes());

    _metadataExtractor.extractMetadata(_tarredSegmentFile, _unzippedSegmentDir);
  }

  @Test(expectedExceptions = Exception.class)
  public void testExtractMetadataWithMissingMetadataProperties() throws Exception {
    createTarWithoutMetadataProperties();

    _metadataExtractor.extractMetadata(_tarredSegmentFile, _unzippedSegmentDir);
  }

  private void createTarWithDirectoryStructure() throws IOException {
    File segmentDir = new File(_tempDir, "v3");
    assertTrue(segmentDir.mkdirs());

    createSegmentMetadataFiles(segmentDir);

    try (OutputStream fos = Files.newOutputStream(_tarredSegmentFile.toPath());
         GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(fos);
         TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)) {

      addDirectoryToTar(taos, segmentDir, "");
    }
  }

  private void createTarWithFlatFileStructure() throws IOException {
    File tempSegmentDir = new File(_tempDir, "temp_segment");
    assertTrue(tempSegmentDir.mkdirs());

    createSegmentMetadataFiles(tempSegmentDir);

    try (OutputStream fos = Files.newOutputStream(_tarredSegmentFile.toPath());
         GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(fos);
         TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)) {

      // Add files directly to tar root (no directory structure)
      for (File file : tempSegmentDir.listFiles()) {
        if (file.isFile()) {
          TarArchiveEntry entry = new TarArchiveEntry(file, file.getName());
          taos.putArchiveEntry(entry);
          Files.copy(file.toPath(), taos);
          taos.closeArchiveEntry();
        }
      }
    }
  }

  private void createTarWithoutMetadataProperties() throws IOException {
    File segmentDir = new File(_tempDir, SEGMENT_NAME);
    assertTrue(segmentDir.mkdirs());

    File indexFile = new File(segmentDir, "index.properties");
    try (FileWriter writer = new FileWriter(indexFile)) {
      writer.write("some.property=value\n");
    }

    try (OutputStream fos = Files.newOutputStream(_tarredSegmentFile.toPath());
         GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(fos);
         TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)) {

      addDirectoryToTar(taos, segmentDir, "");
    }
  }

  private void createSegmentMetadataFiles(File segmentDir) throws IOException {
    Long creationTime = System.currentTimeMillis();
    Long startTime = creationTime - 3600000; // 1 hour before creation
    Long endTime = creationTime + 3600000;   // 1 hour after creation
    Properties metadata = new Properties();
    metadata.setProperty("segment.name", SEGMENT_NAME);
    metadata.setProperty("segment.crc", "1234567890");
    metadata.setProperty("segment.creation.time", String.valueOf(creationTime));
    metadata.setProperty("segment.start.time", String.valueOf(startTime));
    metadata.setProperty("segment.end.time", String.valueOf(endTime));
    metadata.setProperty("segment.time.unit", "MILLISECONDS");
    metadata.setProperty("segment.total.docs", "1000");

    File metadataFile = new File(segmentDir, "metadata.properties");
    try (OutputStream os = Files.newOutputStream(metadataFile.toPath())) {
      metadata.store(os, "Segment metadata for testing");
    }

    File creationMetaFile = new File(segmentDir, "creation.meta");
    Properties creationMeta = new Properties();
    creationMeta.setProperty("creator.name", "test-creator");
    creationMeta.setProperty("creation.time", String.valueOf(System.currentTimeMillis()));
    try (OutputStream os = Files.newOutputStream(creationMetaFile.toPath())) {
      creationMeta.store(os, "Creation metadata for testing");
    }
  }

  private void addDirectoryToTar(TarArchiveOutputStream taos, File dir, String basePath) throws IOException {
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        String entryName = basePath.isEmpty() ? file.getName() : basePath + "/" + file.getName();

        if (file.isDirectory()) {
          TarArchiveEntry dirEntry = new TarArchiveEntry(file, entryName + "/");
          taos.putArchiveEntry(dirEntry);
          taos.closeArchiveEntry();
          addDirectoryToTar(taos, file, entryName);
        } else {
          TarArchiveEntry fileEntry = new TarArchiveEntry(file, entryName);
          taos.putArchiveEntry(fileEntry);
          Files.copy(file.toPath(), taos);
          taos.closeArchiveEntry();
        }
      }
    }
  }
}
