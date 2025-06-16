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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Lucene QueryParser Concepts:
 *
 * 1. Default Behavior:
 *    - Terms without operators are treated as OR
 *    - "term1 term2" is same as "term1 OR term2"
 *    - Case-insensitive by default
 *    - Whitespace is used to separate terms
 *
 * 2. Keywords and Operators:
 *    AND     - Both terms must match
 *    OR      - Either term can match
 *    NOT     - Term must not match
 *    +       - Term must match (same as AND)
 *    -       - Term must not match (same as NOT)
 *
 * 3. Wildcards:
 *    *       - Matches any number of characters
 *    ?       - Matches single character
 *
 * 4. Grouping:
 *    ( )     - Groups terms together
 *    " "     - Phrase query (terms must be in exact order)
 *
 * 5. Example Queries:
 *    "power"                     - Matches documents containing "power"
 *    "power free"               - Matches documents containing either "power" OR "free"
 *    "power AND free"           - Matches documents containing both "power" AND "free"
 *    "power NOT free"           - Matches documents containing "power" but NOT "free"
 *    "power*"                   - Matches "power", "powerful", etc.
 *    "*power"                   - Matches "power", "superpower", etc. (if leading wildcards enabled)
 *    "p?wer"                    - Matches "power", "pawer", etc.
 */
public class LuceneTest {
  private static final String COLUMN_NAME = "text";
  private static final String[] TEST_DOCUMENTS = {
      "power active logged user123", "power inactive logged user456", "free active logged user789",
      "free inactive " + "logged user101", "power active notlogged user202", "free active notlogged user303",
      "power inactive " + "notlogged user404", "free inactive notlogged user505", "power active logged user606",
      "free active logged "
          + "user707", "power active logged user808", "free active logged user909", "power active logged user1010",
      "free" + " active logged user1111", "power active logged user1212", "free active logged user1313", "power active "
      + "logged user1414", "free active logged user1515", "power active logged user1616", "free active logged user1717"
  };

  private Directory _indexDir;
  private IndexWriter _indexWriter;
  private DirectoryReader _indexReader;
  private IndexSearcher _indexSearcher;
  private QueryParser _classicQueryParser;
  private ComplexPhraseQueryParser _complexPhraseQueryParser;
  private StandardQueryParser _standardQueryParser;
  private Path _tempDir;

  // Custom analyzer with stop words
  private static class CustomAnalyzer extends StopwordAnalyzerBase {
    public CustomAnalyzer() {
      super(CharArraySet.EMPTY_SET);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      Tokenizer source = new WhitespaceTokenizer();
      TokenStream tokenStream = new LowerCaseFilter(source);
      Set<String> stopWords = new HashSet<>(
          Arrays.asList("and", "or", "(", ")", "\\", "/", "-", "+", "=", "{", "}", "[", "]", "\"", "'", "<", ">", "?"));
      tokenStream = new StopFilter(tokenStream, new CharArraySet(stopWords, false));
      return new TokenStreamComponents(source, tokenStream);
    }
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    // Create temporary directory for index
    _tempDir = Files.createTempDirectory("lucene-test-index");

    // Create index directory
    _indexDir = FSDirectory.open(_tempDir);

    // Configure index writer with custom analyzer
    IndexWriterConfig config = new IndexWriterConfig(new CustomAnalyzer());
    _indexWriter = new IndexWriter(_indexDir, config);

    // Index test documents
    for (String doc : TEST_DOCUMENTS) {
      Document document = new Document();
      // Store the text content so we can retrieve it later
      document.add(new TextField(COLUMN_NAME, doc, TextField.Store.YES));
      _indexWriter.addDocument(document);
    }
    _indexWriter.commit();

    // Initialize reader and searcher
    _indexReader = DirectoryReader.open(_indexDir);
    _indexSearcher = new IndexSearcher(_indexReader);

    // Initialize different query parsers
    _classicQueryParser = new QueryParser(COLUMN_NAME, new CustomAnalyzer());
    _classicQueryParser.setAllowLeadingWildcard(true);

    _complexPhraseQueryParser = new ComplexPhraseQueryParser(COLUMN_NAME, new CustomAnalyzer());
    _complexPhraseQueryParser.setAllowLeadingWildcard(true);

    _standardQueryParser = new StandardQueryParser(new CustomAnalyzer());
    _standardQueryParser.setAllowLeadingWildcard(true);
  }

  @Test
  public void testAllQueryTypesWithAllParsers()
      throws Exception {
    // Test cases with expected results
    Object[][] testCases = {
        // Query string, expected results for Classic, Complex Phrase, Standard
        {"power OR *free", 20, 20, 20},           // OR with wildcard
        {"power AND *free", 0, 0, 0},             // AND with wildcard
        {"power* OR *free", 20, 20, 20},          // Wildcard OR wildcard
        {"power NOT free", 10, 10, 10},           // Simple NOT
        {"power NOT *free", 10, 10, 10},          // NOT with wildcard
        {"power* NOT *free", 10, 10, 10},         // Wildcard NOT wildcard
    };

    for (Object[] testCase : testCases) {
      String queryString = (String) testCase[0];
      int expectedClassic = (Integer) testCase[1];
      int expectedComplex = (Integer) testCase[2];
      int expectedStandard = (Integer) testCase[3];

      // Test with Classic Query Parser
      Query classicQuery = _classicQueryParser.parse(queryString);
      if (_classicQueryParser.getClass().equals(QueryParser.class)) {
        classicQuery = LuceneTextIndexUtils.convertToMultiTermSpanQuery(classicQuery);
      }
      var classicResults = _indexSearcher.search(classicQuery, 20);
      Assert.assertEquals(classicResults.totalHits.value, expectedClassic,
          "Classic parser failed for query: " + queryString);

      // Test with Complex Phrase Query Parser
      Query complexQuery = _complexPhraseQueryParser.parse(queryString);
      var complexResults = _indexSearcher.search(complexQuery, 20);
      Assert.assertEquals(complexResults.totalHits.value, expectedComplex,
          "Complex phrase parser failed for query: " + queryString);

      // Test with Standard Query Parser
      Query standardQuery = _standardQueryParser.parse(queryString, COLUMN_NAME);
      var standardResults = _indexSearcher.search(standardQuery, 20);
      Assert.assertEquals(standardResults.totalHits.value, expectedStandard,
          "Standard parser failed for query: " + queryString);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _indexReader.close();
    _indexWriter.close();
    _indexDir.close();
    // Clean up temporary directory
    Files.walk(_tempDir).sorted((a, b) -> b.compareTo(a)).forEach(path -> {
      try {
        Files.delete(path);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }
}
