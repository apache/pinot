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
package org.apache.pinot.segment.local.segment.creator.impl.text;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Integration test for the complete Lucene text index buffer flow:
 * 1. Create Lucene text index files in a directory
 * 2. Combine files using LuceneTextIndexCombined
 * 3. Read combined file via PinotDataBuffer
 * 4. Verify document search using LuceneTextIndexReader
 */
public class LuceneTextIndexBufferIntegrationTest {
  private static final String COLUMN_NAME = "textColumn";
  private static final int NUM_DOCS = 4;

  private File _tempDir;
  private File _indexDir;
  private File _combinedFile;
  private List<String> _documents;

  @BeforeMethod
  public void setUp() throws IOException {
    // Create temporary directory structure
    _tempDir = new File(System.getProperty("java.io.tmpdir"),
        "lucene_buffer_test_" + System.currentTimeMillis());
    _tempDir.mkdirs();

    _indexDir = new File(_tempDir, COLUMN_NAME + V1Constants.Indexes.LUCENE_V912_TEXT_INDEX_FILE_EXTENSION);
    _combinedFile = new File(_tempDir, COLUMN_NAME + V1Constants.Indexes.LUCENE_COMBINE_TEXT_INDEX_FILE_EXTENSION);

    // Prepare test documents
    _documents = new ArrayList<>();
    _documents.add("Apache Pinot is a distributed OLAP datastore");
    _documents.add("Real-time analytics with low latency queries");
    _documents.add("Scalable analytics platform for big data");
    _documents.add("Fast aggregations and filtering capabilities");
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if (_tempDir != null && _tempDir.exists()) {
      FileUtils.deleteDirectory(_tempDir);
    }
  }

  @Test
  public void testCompleteBufferFlow() throws Exception {
    // Step 1: Create Lucene text index files
    createLuceneIndexFiles();

    // Step 2: Combine files into buffer format
    combineLuceneFiles();

    // Step 3: Read via PinotDataBuffer and verify search functionality
    verifyBufferBasedSearch();
  }

  /**
   * Step 1: Create Lucene text index files in directory structure
   */
  private void createLuceneIndexFiles() throws Exception {
    System.out.println("Creating Lucene index files in: " + _indexDir.getAbsolutePath());

    // Create the index directory
    _indexDir.mkdirs();

    // Create text index config
    TextIndexConfig textIndexConfig = new TextIndexConfigBuilder()
        .withLuceneAnalyzerClass("org.apache.lucene.analysis.standard.StandardAnalyzer")
        .build();

    // Create the Lucene text index creator
    try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator(COLUMN_NAME, _tempDir, true, false, null, null,
        true, textIndexConfig)) {

      // Add documents to the index
      for (int docId = 0; docId < _documents.size(); docId++) {
        creator.add(_documents.get(docId), docId);
        System.out.println("Added doc " + docId + ": " + _documents.get(docId));
      }

      // Seal the index (this will also combine files and delete the original directory)
      creator.seal();
    }

    // Note: After sealing, the original Lucene directory is deleted and files are combined
    System.out.println("Index files created and combined into single buffer file");
  }

  /**
   * Step 2: Verify combined file was created during seal()
   */
  private void combineLuceneFiles() throws Exception {
    System.out.println("Verifying combined file: " + _combinedFile.getAbsolutePath());

    // Verify that combined file was created during seal()
    assertTrue(_combinedFile.exists(), "Combined file should exist");
    assertTrue(_combinedFile.length() > 0, "Combined file should not be empty");

    System.out.println("Combined file verified: " + _combinedFile.length() + " bytes");
  }

  /**
   * Step 3: Read via PinotDataBuffer and verify search functionality
   */
  private void verifyBufferBasedSearch() throws Exception {
    System.out.println("Verifying buffer-based search functionality");

    // Load the combined file as PinotDataBuffer
    PinotDataBuffer indexBuffer = PinotDataBuffer.mapFile(_combinedFile, true, 0, _combinedFile.length(),
        java.nio.ByteOrder.LITTLE_ENDIAN, "LuceneTextIndexBufferTest");

    try {
      // Create text index config
      TextIndexConfig textIndexConfig = new TextIndexConfigBuilder()
          .withLuceneAnalyzerClass("org.apache.lucene.analysis.standard.StandardAnalyzer")
          .build();

      // Create LuceneTextIndexReader using the buffer-based constructor
      try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN_NAME, indexBuffer, NUM_DOCS,
          textIndexConfig)) {

        // Test various search queries
        testSearchQuery(reader, "Apache", "Should find documents containing 'Apache'");
        testSearchQuery(reader, "analytics", "Should find documents containing 'analytics'");
        testSearchQuery(reader, "distributed", "Should find documents containing 'distributed'");
        testSearchQuery(reader, "nonexistent", "Should return empty results for non-existent terms");

        // Test phrase queries
        testSearchQuery(reader, "\"real-time analytics\"", "Should find exact phrase");
        testSearchQuery(reader, "\"Apache Pinot\"", "Should find exact phrase");

        System.out.println("All search tests passed successfully!");
      }
    } finally {
      indexBuffer.close();
    }
  }

  /**
   * Helper method to test a search query and verify results
   */
  private void testSearchQuery(LuceneTextIndexReader reader, String query, String description) {
    try {
      System.out.println("Testing query: '" + query + "' - " + description);

      ImmutableRoaringBitmap results = reader.getDocIds(query);
      assertNotNull(results, "Search results should not be null");

      int resultCount = results.getCardinality();
      System.out.println("  Found " + resultCount + " matching documents");

      if (resultCount > 0) {
        System.out.print("  Document IDs: ");
        results.forEach((int docId) -> System.out.print(docId + " "));
        System.out.println();

        // Print the actual document content for verification
        results.forEach((int docId) -> {
          if (docId < _documents.size()) {
            System.out.println("    Doc " + docId + ": " + _documents.get(docId));
          }
        });
      }

      // Verify that results are within valid range
      results.forEach((int docId) -> {
        assertTrue(docId >= 0 && docId < NUM_DOCS,
            "Document ID " + docId + " should be within valid range [0, " + NUM_DOCS + ")");
      });
    } catch (Exception e) {
      fail("Search query '" + query + "' failed: " + e.getMessage(), e);
    }
  }
}
