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


/**
 * A custom query parser that creates prefix phrase queries.
 * This parser tokenizes the input query and creates a SpanNearQuery where
 * all terms except the last one are exact matches, and the last term has a wildcard suffix.
 *
 * Example usage:
 * - Input: 'java realtime streaming'
 * - Output: SpanNearQuery with exact matches in-order for "java" and "realtime",
 *           and wildcard match for "streaming*"
 */
public class PrefixPhraseQueryParser extends QueryParserBase {
  private final String _field;
  private final Analyzer _analyzer;

  public PrefixPhraseQueryParser(String field, Analyzer analyzer) {
    super();
    _field = field;
    _analyzer = analyzer;
  }

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
      WildcardQuery wildcardQuery = new WildcardQuery(new Term(_field, token + "*"));
      return new SpanMultiTermQueryWrapper<>(wildcardQuery);
    }

    // Handle multiple tokens case
    List<SpanQuery> spanQueries = new ArrayList<>();

    // Add regular SpanTermQueries for all tokens except the last one
    for (int i = 0; i < tokens.size() - 1; i++) {
      spanQueries.add(new SpanTermQuery(new Term(_field, tokens.get(i))));
    }

    // Add wildcard for the last token
    String lastToken = tokens.get(tokens.size() - 1);
    WildcardQuery wildcardQuery = new WildcardQuery(new Term(_field, lastToken + "*"));
    spanQueries.add(new SpanMultiTermQueryWrapper<>(wildcardQuery));

    // Create SpanNearQuery with 0 slop (exact order) and inOrder=true
    return new SpanNearQuery(spanQueries.toArray(new SpanQuery[0]), 0, true);
  }

  @Override
  public void ReInit(CharStream input) {
    // This method is required by QueryParserBase but not used in our implementation
    // since we override parse(String) directly
  }

  @Override
  public Query TopLevelQuery(String field)
      throws ParseException {
    return null;
  }
}
