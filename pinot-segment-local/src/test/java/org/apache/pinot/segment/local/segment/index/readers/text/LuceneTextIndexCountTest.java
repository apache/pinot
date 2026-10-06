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
import java.util.HashMap;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.segment.index.text.TextIndexConfigBuilder;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Pins the invariant behind the `count(*)` short-circuit: [LuceneTextIndexReader#getNumMatchingDocs] must
/// return exactly what counting the materialized doc ids would return, for every query shape.
///
/// The short-circuit reads a count from Lucene's index metadata where it can and falls back to materializing
/// doc ids where it cannot, so the two paths have to agree or a `count(*)` would silently disagree with the
/// rows its own filter returns.
public class LuceneTextIndexCountTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), LuceneTextIndexCountTest.class.getSimpleName());
  private static final String COLUMN = "body";
  private static final String[] DOCS = {
      "failed to place order for user alice",
      "failed to charge card for user bob",
      "connection refused by payment service",
      "connection established to cache service",
      "order confirmation email sent to carol",
      "cache miss while loading order details",
      "payment accepted for order 4815",
      "unrelated housekeeping log line"
  };

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.deleteDirectory(INDEX_DIR);
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
    FileUtils.deleteDirectory(INDEX_DIR);
  }

  @DataProvider(name = "queries")
  public Object[][] queries() {
    return new Object[][]{
        // Single terms: the shape Lucene can count from metadata alone, i.e. the case the optimization targets.
        {"order", null},
        {"connection", null},
        {"housekeeping", null},
        // No match, and a term in every document: the boundary cases.
        {"nonexistentterm", null},
        {"to", null},
        // Boolean, phrase and multi-term shapes, which generally take the fallback.
        {"failed AND order", null},
        {"failed OR connection", null},
        {"order AND NOT failed", null},
        {"\"place order\"", null},
        {"\"failed to place order\"", null},
        // Prefix, wildcard, fuzzy and regexp.
        {"conn*", null},
        {"c?che", null},
        {"connection~1", null},
        {"/conn.*/", null},
        // Option-driven parsing must agree too.
        {"failed order connection", "parser=MATCH,minimumShouldMatch=2"},
        {"*tion", "allowLeadingWildcard=true"}
    };
  }

  @Test(dataProvider = "queries")
  public void testCountMatchesMaterializedDocIds(String query, String options)
      throws IOException {
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, new HashMap<>())) {
      int expected = reader.getDocIds(query, options).getCardinality();
      int actual = reader.getNumMatchingDocs(query, options);
      assertEquals(actual, expected, "count disagreed with materialized doc ids for query: " + query);
    }
  }

  @Test
  public void testCountIsExactForKnownQueries()
      throws IOException {
    try (LuceneTextIndexReader reader = new LuceneTextIndexReader(COLUMN, INDEX_DIR, DOCS.length, new HashMap<>())) {
      assertEquals(reader.getNumMatchingDocs("order", null), 4);
      assertEquals(reader.getNumMatchingDocs("connection", null), 2);
      assertEquals(reader.getNumMatchingDocs("housekeeping", null), 1);
      assertEquals(reader.getNumMatchingDocs("nonexistentterm", null), 0);
    }
  }
}
