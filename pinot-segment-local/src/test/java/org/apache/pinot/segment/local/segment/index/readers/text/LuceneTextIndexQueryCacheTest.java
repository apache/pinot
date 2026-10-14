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
package org.apache.pinot.segment.local.segment.index.readers.text;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.Query;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;


/// Covers the parsed-query cache in [LuceneTextIndexReader].
///
/// The cache is static, so the column name is deliberately unique to this class: a key that collided with
/// another test class's column would make these assertions depend on test ordering.
public class LuceneTextIndexQueryCacheTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(),
      LuceneTextIndexQueryCacheTest.class.getSimpleName() + "-" + UUID.randomUUID());
  private static final File INDEX_DIR = new File(TEMP_DIR, "index");
  private static final String COLUMN = "querycachebody";

  private static final String[] DOCS = {
      "failed to place order for user alice",
      "failed to charge card for user bob",
      "connection refused by payment service",
      "order confirmation email sent to carol"
  };

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.forceMkdir(INDEX_DIR);
    TextIndexConfig config = new TextIndexConfigBuilder().build();
    try (LuceneTextIndexCreator creator = new LuceneTextIndexCreator(COLUMN, INDEX_DIR, true, false, null, null,
        config)) {
      for (String doc : DOCS) {
        creator.add(doc);
      }
      creator.seal();
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  private static TextIndexConfig config(boolean useAndForMultiTerm) {
    return (TextIndexConfig) new TextIndexConfigBuilder().withProperties(
        Map.of(FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES, String.valueOf(useAndForMultiTerm))).build();
  }

  @Test
  public void testRepeatedParseReturnsTheCachedInstance()
      throws Exception {
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, config(false))) {
      Query first = reader.buildQuery("order", null);
      Query second = reader.buildQuery("order", null);
      assertSame(second, first, "a repeated parse should be served from the cache");
      assertNotSame(reader.parseQuery("order", null), first, "parseQuery must bypass the cache");
      assertEquals(reader.parseQuery("order", null), first, "the cached query must equal a fresh parse");
    }
  }

  /// Regression test: the cache is process-wide, so two tables that share a column name but parse
  /// differently must not share an entry. Keying on the column name alone would hand the second
  /// reader the first one's query and silently return the wrong rows.
  @Test
  public void testConfigDifferenceIsNotSharedAcrossReaders()
      throws Exception {
    try (LuceneTextIndexReader orReader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, config(false));
        LuceneTextIndexReader andReader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, config(true))) {
      Query asOr = orReader.buildQuery("failed order", null);
      Query asAnd = andReader.buildQuery("failed order", null);
      assertNotEquals(asAnd, asOr, "OR and AND multi-term configs must not share a cache entry");
      assertEquals(asOr, orReader.parseQuery("failed order", null));
      assertEquals(asAnd, andReader.parseQuery("failed order", null));

      // and the difference must still be visible in the answer
      assertEquals(orReader.getNumMatchingDocs("failed order", null), 3);
      assertEquals(andReader.getNumMatchingDocs("failed order", null), 1);
    }
  }

  @Test
  public void testOptionsStringIsPartOfTheKey()
      throws Exception {
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, config(false))) {
      Query withoutOptions = reader.buildQuery("failed order", null);
      Query withOptions = reader.buildQuery("failed order", "parser=MATCH,operator=AND");
      assertNotEquals(withOptions, withoutOptions, "different options must not share a cache entry");
    }
  }
}
