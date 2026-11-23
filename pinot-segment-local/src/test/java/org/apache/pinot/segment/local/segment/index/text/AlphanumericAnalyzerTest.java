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
package org.apache.pinot.segment.local.segment.index.text;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.pinot.segment.local.segment.store.TextIndexUtils;
import org.apache.pinot.segment.spi.index.TextIndexConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class AlphanumericAnalyzerTest {

  private static final String FIELD = "body";

  private static List<String> tokenize(Analyzer analyzer, String input)
      throws IOException {
    List<String> tokens = new ArrayList<>();
    try (TokenStream stream = analyzer.tokenStream(FIELD, new StringReader(input))) {
      CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
      stream.reset();
      while (stream.incrementToken()) {
        tokens.add(term.toString());
      }
      stream.end();
    }
    return tokens;
  }

  @Test
  public void testSplitsOnNonAlphanumericAndLowercases()
      throws IOException {
    Analyzer analyzer = new AlphanumericAnalyzer();
    assertEquals(tokenize(analyzer, "email_server.rb"), List.of("email", "server", "rb"));
    assertEquals(tokenize(analyzer, "send_email"), List.of("send", "email"));
    assertEquals(tokenize(analyzer, "GET /api/v2/Orders?id=42"), List.of("get", "api", "v2", "orders", "id", "42"));
    assertEquals(tokenize(analyzer, "  leading and trailing  "), List.of("leading", "and", "trailing"));
    assertEquals(tokenize(analyzer, ""), List.of());
    assertEquals(tokenize(analyzer, "...---..."), List.of());
  }

  /// Digits are the reason this analyzer exists: `SimpleAnalyzer` drops them entirely, so a term
  /// made only of digits can never be matched, and one mixing digits with letters is shattered.
  @Test
  public void testDigitsArePreservedUnlikeSimpleAnalyzer()
      throws IOException {
    Analyzer alphanumeric = new AlphanumericAnalyzer();
    Analyzer simple = new SimpleAnalyzer();

    assertEquals(tokenize(simple, "200"), List.of());
    assertEquals(tokenize(alphanumeric, "200"), List.of("200"));

    assertEquals(tokenize(simple, "8080"), List.of());
    assertEquals(tokenize(alphanumeric, "8080"), List.of("8080"));

    assertEquals(tokenize(simple, "http2"), List.of("http"));
    assertEquals(tokenize(alphanumeric, "http2"), List.of("http2"));

    assertEquals(tokenize(simple, "1YMWWN1N4O"), List.of("ymwwn", "n", "o"));
    assertEquals(tokenize(alphanumeric, "1YMWWN1N4O"), List.of("1ymwwn1n4o"));

    assertEquals(tokenize(simple, "expected 200 got 500"), List.of("expected", "got"));
    assertEquals(tokenize(alphanumeric, "expected 200 got 500"), List.of("expected", "200", "got", "500"));
  }

  /// `StandardAnalyzer` follows UAX#29, which keeps these whole, so a search for `email` misses
  /// them. Splitting on every non-alphanumeric is the behaviour this analyzer adds.
  @Test
  public void testSplitsWhereStandardAnalyzerKeepsWhole()
      throws IOException {
    Analyzer standard = new StandardAnalyzer();
    assertEquals(tokenize(standard, "send_email"), List.of("send_email"));
    assertEquals(tokenize(standard, "email_server.rb"), List.of("email_server.rb"));
    assertEquals(tokenize(new AlphanumericAnalyzer(), "send_email"), List.of("send", "email"));
    assertEquals(tokenize(new AlphanumericAnalyzer(), "email_server.rb"), List.of("email", "server", "rb"));
  }

  @Test
  public void testCaseSensitiveModePreservesCase()
      throws IOException {
    Analyzer analyzer = new AlphanumericAnalyzer(true);
    assertEquals(tokenize(analyzer, "send_Email HTTP2"), List.of("send", "Email", "HTTP2"));
    assertTrue(new AlphanumericAnalyzer(true).isCaseSensitive());
    assertFalse(new AlphanumericAnalyzer().isCaseSensitive());
  }

  /// Lucene applies `normalize` to the terms of multi-term queries (wildcard, prefix, range,
  /// fuzzy), which are never tokenized. Without it `HTTP*` would be matched against a lowercased
  /// index and find nothing.
  @Test
  public void testNormalizeFoldsCaseForMultiTermQueries() {
    assertEquals(new AlphanumericAnalyzer().normalize(FIELD, "HTTP"), new BytesRef("http"));
    assertEquals(new AlphanumericAnalyzer(true).normalize(FIELD, "HTTP"), new BytesRef("HTTP"));
  }

  /// Token characters come from [Character#isLetterOrDigit(int)], so non-ASCII letters and digits
  /// are kept rather than treated as separators.
  @Test
  public void testNonAsciiLettersAndDigitsAreTokenCharacters()
      throws IOException {
    Analyzer analyzer = new AlphanumericAnalyzer();
    assertEquals(tokenize(analyzer, "Café-Zürich"), List.of("café", "zürich"));
    assertEquals(tokenize(analyzer, "日本語 text"), List.of("日本語", "text"));
    // U+2A2F VECTOR OR CROSS PRODUCT is a symbol, so it separates rather than joining.
    assertEquals(tokenize(analyzer, "a ⨯ b"), List.of("a", "b"));
  }

  /// The analyzer must be reachable through the same config path users write, including the
  /// boolean constructor argument.
  @Test
  public void testResolvedFromTextIndexConfig()
      throws ReflectiveOperationException {
    String className = AlphanumericAnalyzer.class.getName();

    TextIndexConfig defaultCase = new TextIndexConfigBuilder()
        .withProperties(Map.of("luceneAnalyzerClass", className))
        .build();
    Analyzer resolved = TextIndexUtils.getAnalyzer(defaultCase);
    assertTrue(resolved instanceof AlphanumericAnalyzer);
    assertFalse(((AlphanumericAnalyzer) resolved).isCaseSensitive());

    TextIndexConfig caseSensitive = new TextIndexConfigBuilder()
        .withProperties(Map.of(
            "luceneAnalyzerClass", className,
            "luceneAnalyzerClassArgs", "true",
            "luceneAnalyzerClassArgTypes", "java.lang.Boolean.TYPE"))
        .build();
    Analyzer resolvedCaseSensitive = TextIndexUtils.getAnalyzer(caseSensitive);
    assertTrue(resolvedCaseSensitive instanceof AlphanumericAnalyzer);
    assertTrue(((AlphanumericAnalyzer) resolvedCaseSensitive).isCaseSensitive());
  }

  /// A long run splits into multiple terms while preserving whole Unicode code points.
  @Test
  public void testTokenLengthBoundary()
      throws IOException {
    int threshold = AlphanumericAnalyzer.TOKEN_SPLIT_THRESHOLD;
    try (Analyzer analyzer = new AlphanumericAnalyzer()) {
      String head = "a".repeat(threshold);
      assertEquals(tokenize(analyzer, head), List.of(head));
      assertEquals(tokenize(analyzer, head + "a"), List.of(head, "a"));
      assertEquals(tokenize(analyzer, "A".repeat(threshold) + "BC"), List.of(head, "bc"));
      assertEquals(tokenize(analyzer, head + head), List.of(head, head));
      assertEquals(tokenize(analyzer, head + " b"), List.of(head, "b"));
      // A supplementary Deseret letter spans the threshold and lowercases as a whole code point.
      String beforeBoundary = "a".repeat(threshold - 1);
      assertEquals(tokenize(analyzer, beforeBoundary + "𐐀BC"), List.of(beforeBoundary + "𐐨", "bc"));
    }
  }

  /// Queries operate on the emitted terms, including the separate tail of an oversized identifier.
  @Test
  public void testLongIdentifierSearch()
      throws Exception {
    int threshold = AlphanumericAnalyzer.TOKEN_SPLIT_THRESHOLD;
    String head = "A".repeat(threshold);
    String supplementaryHead = "D".repeat(threshold - 1) + "𐐀";

    try (Analyzer analyzer = new AlphanumericAnalyzer(); Directory directory = new ByteBuffersDirectory()) {
      try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(analyzer))) {
        Document document = new Document();
        document.add(new TextField(FIELD, head + "BC " + supplementaryHead + "EF", Field.Store.NO));
        writer.addDocument(document);
      }
      try (DirectoryReader reader = DirectoryReader.open(directory)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        QueryParser parser = new QueryParser(FIELD, analyzer);

        // The head is indexed at exactly the limit and the tail becomes a term of its own, so the
        // last two characters alone match the document.
        String indexedHead = "a".repeat(threshold);
        assertEquals(searcher.count(new TermQuery(new Term(FIELD, indexedHead))), 1);
        assertEquals(searcher.count(new TermQuery(new Term(FIELD, "bc"))), 1);
        // The whole identifier is never a single term.
        assertEquals(searcher.count(new TermQuery(new Term(FIELD, indexedHead + "bc"))), 0);

        // A literal prefix cannot span a token split.
        String shorterHead = "A".repeat(threshold - 1);
        assertEquals(searcher.count(parser.parse(shorterHead + "*")), 1);
        assertEquals(searcher.count(parser.parse(head + "*")), 1);
        assertEquals(searcher.count(parser.parse(head + "B*")), 0);
        assertEquals(searcher.count(parser.parse(supplementaryHead + "*")), 1);

        // Wildcards can match an empty string, and fuzzy matching allows one edit.
        assertEquals(searcher.count(parser.parse(shorterHead + "*A*")), 1);
        assertEquals(searcher.count(parser.parse(head + "X~1")), 1);
      }
    }
  }

  /// Reusing one instance across many inputs must not leak state between token streams.
  @Test
  public void testInstanceIsReusable()
      throws IOException {
    Analyzer analyzer = new AlphanumericAnalyzer();
    for (int i = 0; i < 3; i++) {
      assertEquals(tokenize(analyzer, "status=200 path=/v1/orders"), List.of("status", "200", "path", "v1", "orders"));
      assertEquals(tokenize(analyzer, "no_digits_here"), List.of("no", "digits", "here"));
    }
  }
}
