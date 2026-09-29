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
package org.apache.pinot.segment.local.segment.index.text.lucene.parsers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.queryparser.charstream.CharStream;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;


/// A custom query parser that creates prefix phrase queries.
/// This parser tokenizes the input query and creates a SpanNearQuery where
/// all terms except the last one are exact matches, and the last term can optionally
/// have a wildcard suffix based on the enablePrefixMatch setting.
///
/// This parser is designed to support both exact phrase matching and prefix phrase matching:
///
/// - **Exact phrase matching (default):** All terms are matched exactly as they appear
/// - **Prefix phrase matching:** The last term is treated as a prefix with wildcard
///
/// **Example usage:**
///
/// - Input: 'java realtime streaming' with enablePrefixMatch=false (default)
///
///      Output: SpanNearQuery with exact matches for "java", "realtime", and "streaming"
/// - Input: 'java realtime streaming' with enablePrefixMatch=true
///
///      Output: SpanNearQuery with exact matches for "java" and "realtime",
///      and wildcard match for "streaming\*"
/// - Input: 'stream' with enablePrefixMatch=false (default)
///
///      Output: SpanTermQuery for exact match "stream"
/// - Input: 'stream' with enablePrefixMatch=true
///
///      Output: SpanMultiTermQueryWrapper for wildcard match "stream\*"
///
/// **Behavior:**
///
/// - Single term queries: Returns SpanTermQuery (exact) or SpanMultiTermQueryWrapper (prefix)
/// - Multiple term queries: Returns SpanNearQuery with all terms in exact order
/// - Null/empty queries: Throws ParseException
/// - Whitespace-only queries: Throws ParseException
///
/// This parser extends Lucene's QueryParserBase and implements the required abstract methods.
/// It uses the provided Analyzer for tokenization and creates appropriate Lucene Span queries.
public class PrefixPhraseQueryParser extends QueryParserBase {
  /// The field name to search in
  private final String _field;

  /// The analyzer used for tokenizing the query
  private final Analyzer _analyzer;

  /// Flag to control whether prefix matching is enabled on the last term
  private boolean _enablePrefixMatch = false;

  /// The slop (distance) allowed between terms in the phrase query. Default is 0 (exact order)
  private int _slop = 0;

  /// Whether terms must appear in the specified order. Default is true (exact order)
  private boolean _inOrder = true;

  /// Constructs a new PrefixPhraseQueryParser with the specified field and analyzer.
  ///
  /// @param field the field name to search in (must not be null)
  /// @param analyzer the analyzer to use for tokenizing queries (must not be null)
  /// @throws IllegalArgumentException if field or analyzer is null
  public PrefixPhraseQueryParser(String field, Analyzer analyzer) {
    super();
    _field = field;
    _analyzer = analyzer;
  }

    /// Sets whether to enable prefix matching on the last term.
    ///
    /// When enabled (`true`):
    ///
    /// - Single term queries: Returns a SpanMultiTermQueryWrapper with wildcard (\*)
    /// - Multiple term queries: The last term gets a wildcard suffix (\*)
    ///
    /// When disabled (`false`, default):
    ///
    /// - Single term queries: Returns a SpanTermQuery for exact match
    /// - Multiple term queries: All terms are matched exactly
    ///
    /// @param enablePrefixMatch true to enable prefix matching, false to disable (default)
  public void setEnablePrefixMatch(boolean enablePrefixMatch) {
    _enablePrefixMatch = enablePrefixMatch;
  }

  /// Sets the slop (distance) allowed between terms in the phrase query.
  ///
  /// The slop determines how many positions apart the terms can be while still matching.
  /// For example:
  ///
  /// - slop=0: Terms must be adjacent in exact order
  /// - slop=1: Terms can be 1 position apart
  /// - slop=2: Terms can be 2 positions apart
  ///
  /// This setting only affects multiple term queries that create SpanNearQuery.
  ///
  /// @param slop the number of positions allowed between terms (default is 0)
  /// @throws IllegalArgumentException if slop is negative
  public void setSlop(int slop) {
    if (slop < 0) {
      throw new IllegalArgumentException("Slop cannot be negative: " + slop);
    }
    _slop = slop;
  }

  /// Sets whether terms must appear in the specified order.
  ///
  /// When enabled (`true`, default):
  ///
  /// - Terms must appear in the exact order specified in the query
  /// - Example: "java realtime" matches "java realtime streaming" but not "realtime java streaming"
  ///
  /// When disabled (`false`):
  ///
  /// - Terms can appear in any order within the slop distance
  /// - Example: "java realtime" matches both "java realtime streaming" and "realtime java streaming"
  ///
  /// This setting only affects multiple term queries that create SpanNearQuery.
  ///
  /// @param inOrder true to require terms in exact order, false to allow any order
  public void setInOrder(boolean inOrder) {
    _inOrder = inOrder;
  }

  /// Parses the given query string and returns an appropriate Lucene Query.
  ///
  /// This method performs the following steps:
  ///
  /// 1. Validates the input query (null, empty, whitespace-only)
  /// 2. Tokenizes the query using the configured analyzer
  /// 3. Creates appropriate Lucene queries based on the number of tokens and enablePrefixMatch setting
  ///
  /// **Query Types Returned:**
  ///
  /// - **Single term:**
  ///   - If enablePrefixMatch=false: SpanTermQuery for exact match
  ///   - If enablePrefixMatch=true: SpanMultiTermQueryWrapper with wildcard
  /// - **Multiple terms:** SpanNearQuery with all terms in exact order
  ///   - All terms except the last: SpanTermQuery (exact match)
  ///   - Last term: SpanTermQuery (exact) or SpanMultiTermQueryWrapper (wildcard)
  ///        based on enablePrefixMatch
  ///
  /// @param query the query string to parse (must not be null or empty)
  /// @return a Lucene Query object representing the parsed query
  /// @throws ParseException if the query is null, empty, or contains no valid tokens after tokenization
  /// @throws RuntimeException if tokenization fails due to an IOException
  @Override
  public Query parse(String query) throws ParseException {
    if (query == null) {
      throw new ParseException("Query cannot be null");
    }

    if (query.trim().isEmpty()) {
      throw new ParseException("Query cannot be empty");
    }

    // Tokenize the query
    List<String> tokens = new ArrayList<>();
    try (TokenStream stream = _analyzer.tokenStream(_field, query)) {
      stream.reset();
      CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);

      while (stream.incrementToken()) {
        String token = charTermAttribute.toString();
        if (!token.trim().isEmpty()) {
          tokens.add(token);
        }
      }
      stream.end();
    } catch (IOException e) {
      throw new RuntimeException("Failed to tokenize query: " + query, e);
    }

    // Check if we have any valid tokens after tokenization
    if (tokens.isEmpty()) {
      throw new ParseException("Query tokenization resulted in no valid tokens");
    }

    // Handle single token case
    if (tokens.size() == 1) {
      String token = tokens.get(0);
      if (_enablePrefixMatch) {
        WildcardQuery wildcardQuery = new WildcardQuery(new Term(_field, token + "*"));
        return new SpanMultiTermQueryWrapper<>(wildcardQuery);
      } else {
        return new SpanTermQuery(new Term(_field, token));
      }
    }

    // Handle multiple tokens case
    List<SpanQuery> spanQueries = new ArrayList<>();

    // Add regular SpanTermQueries for all tokens except the last one
    for (int i = 0; i < tokens.size() - 1; i++) {
      spanQueries.add(new SpanTermQuery(new Term(_field, tokens.get(i))));
    }

    // Add query for the last token
    String lastToken = tokens.get(tokens.size() - 1);
    if (_enablePrefixMatch) {
      WildcardQuery wildcardQuery = new WildcardQuery(new Term(_field, lastToken + "*"));
      spanQueries.add(new SpanMultiTermQueryWrapper<>(wildcardQuery));
    } else {
      spanQueries.add(new SpanTermQuery(new Term(_field, lastToken)));
    }

    // Create SpanNearQuery with configurable slop and inOrder settings
    return new SpanNearQuery(spanQueries.toArray(new SpanQuery[0]), _slop, _inOrder);
  }

  /// Reinitializes the parser with a new CharStream.
  ///
  /// This method is required by QueryParserBase but is not used in this implementation
  /// since we override the parse(String) method directly. The method is left as a no-op.
  ///
  /// @param input the CharStream to reinitialize with (ignored in this implementation)
  @Override
  public void ReInit(CharStream input) {
    // This method is required by QueryParserBase but not used in our implementation
    // since we override parse(String) directly
  }

  /// Creates a top-level query for the specified field.
  ///
  /// This method is required by QueryParserBase but is not supported in this implementation.
  /// Use the parse(String) method instead for query parsing.
  ///
  /// @param field the field name (ignored in this implementation)
  /// @return never returns (always throws UnsupportedOperationException)
  /// @throws ParseException never thrown (method always throws UnsupportedOperationException)
  /// @throws UnsupportedOperationException always thrown, indicating this method is not supported
  @Override
  public Query TopLevelQuery(String field)
      throws ParseException {
    throw new UnsupportedOperationException(
        "TopLevelQuery is not supported in PrefixPhraseQueryParser. Use parse(String) method instead.");
  }
}
