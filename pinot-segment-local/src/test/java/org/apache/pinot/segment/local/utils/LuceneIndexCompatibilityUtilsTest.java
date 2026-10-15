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
package org.apache.pinot.segment.local.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies that format recovery does not mask corrupt commits or ordinary I/O failures.
public class LuceneIndexCompatibilityUtilsTest {
  private File _fixtures;

  @BeforeMethod
  public void setUp()
      throws IOException {
    _fixtures = Files.createTempDirectory("lucene-compatibility").toFile();
    // Generated with Lucene 10.0.0 (text/multi) and Pinot f9cd6821's HnswCodec (hnsw).
    // created9 variants set the creation major to 9, with Lucene912 and Lucene100 codecs respectively.
    try (ZipInputStream zip = new ZipInputStream(getClass().getResourceAsStream("/data/lucene10-rollback.zip"))) {
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null) {
        File output = new File(_fixtures, entry.getName());
        FileUtils.forceMkdirParent(output);
        Files.copy(zip, output.toPath());
      }
    }
  }

  @AfterMethod
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(_fixtures);
  }

  @Test
  public void testNewerFormatsIncludingOlderCreationVersion()
      throws IOException {
    for (String name : new String[]{"text", "multi", "hnsw", "created9", "created9-unknowncodec"}) {
      assertTrue(LuceneIndexCompatibilityUtils.isIncompatible(new File(_fixtures, name)), name);
    }
    try (Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
        writer.commit();
      }
      assertFalse(LuceneIndexCompatibilityUtils.isIncompatible(directory));
    }
  }

  @Test
  public void testCorruptFutureCommitIsNotRecoverable()
      throws IOException {
    File directory = new File(_fixtures, "text");
    File commit = new File(directory, "segments_1");
    byte[] bytes = Files.readAllBytes(commit.toPath());
    bytes[bytes.length - 1] ^= 1;
    Files.write(commit.toPath(), bytes);
    assertThrows(CorruptIndexException.class, () -> LuceneIndexCompatibilityUtils.isIncompatible(directory));
  }

  @Test
  public void testMissingFilesPropagate()
      throws IOException {
    assertThrows(IOException.class,
        () -> LuceneIndexCompatibilityUtils.isIncompatible(new File(_fixtures, "does-not-exist")));
    try (Directory directory = new ByteBuffersDirectory()) {
      assertThrows(IndexNotFoundException.class, () -> LuceneIndexCompatibilityUtils.isIncompatible(directory));
    }
    File directory = new File(_fixtures, "created9");
    File[] compoundFiles = directory.listFiles((parent, name) -> name.endsWith(".cfs"));
    assertTrue(compoundFiles != null && compoundFiles.length == 1);
    Files.delete(compoundFiles[0].toPath());
    assertThrows(IOException.class, () -> LuceneIndexCompatibilityUtils.isIncompatible(directory));
  }
}
