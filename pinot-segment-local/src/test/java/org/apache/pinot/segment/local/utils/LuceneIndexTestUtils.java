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
import java.io.InputStream;
import java.nio.file.Files;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.io.FileUtils;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Extracts the shared Lucene 10 rollback fixtures into a caller-owned directory, without shared state.
public final class LuceneIndexTestUtils {
  private LuceneIndexTestUtils() {
  }

  public static File extractLucene10Fixtures(File directory)
      throws IOException {
    FileUtils.forceMkdir(directory);
    // Generated with Lucene 10.0.0 (text/multi) and Pinot f9cd6821's HnswCodec (hnsw).
    // created9 variants set the creation major to 9, with Lucene912 and Lucene100 codecs respectively.
    try (InputStream input = LuceneIndexTestUtils.class.getResourceAsStream("/data/lucene10-rollback.zip")) {
      assertNotNull(input);
      try (ZipInputStream zip = new ZipInputStream(input)) {
        ZipEntry entry;
        while ((entry = zip.getNextEntry()) != null) {
          File output = new File(directory, entry.getName());
          assertTrue(output.toPath().normalize().startsWith(directory.toPath().normalize()));
          if (entry.isDirectory()) {
            FileUtils.forceMkdir(output);
          } else {
            FileUtils.forceMkdirParent(output);
            Files.copy(zip, output.toPath());
          }
        }
      }
    }
    return directory;
  }
}
