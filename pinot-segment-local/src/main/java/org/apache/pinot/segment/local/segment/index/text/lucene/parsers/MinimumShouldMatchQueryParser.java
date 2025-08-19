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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.charstream.CharStream;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;


/**
 * A custom query parser that implements OpenSearch's minimum_should_match behavior.
 * This parser creates Boolean queries with should clauses and enforces a minimum
 * number of matches, similar to OpenSearch's minimum_should_match parameter.
 *
 * <p>This parser supports the following minimum_should_match formats:</p>
 * <ul>
 *   <li><strong>Positive integer:</strong> "3" - at least 3 should clauses must match</li>
 *   <li><strong>Negative integer:</strong> "-2" - at most 2 should clauses can be missing</li>
 *   <li><strong>Positive percentage:</strong> "80%" - at least 80% of should clauses must match</li>
 *   <li><strong>Negative percentage:</strong> "-20%" - at most 20% of should clauses can be missing</li>
 * </ul>
 *
 * <p><strong>Example usage:</strong></p>
 * <ul>
 *   <li>Input: 'java OR python OR scala' with minimumShouldMatch=2
 *       <br>Output: BooleanQuery with 3 should clauses, requiring at least 2 matches</li>
 *   <li>Input: 'machine learning OR deep learning OR neural networks' with minimumShouldMatch="80%"
 *       <br>Output: BooleanQuery with 3 should clauses, requiring at least 2 matches (80% of 3 = 2.4, rounded down
 *       to 2)</li>
 *   <li>Input: 'error OR warning OR critical' with minimumShouldMatch="-1"
 *       <br>Output: BooleanQuery with 3 should clauses, allowing at most 1 to be missing (requiring at least 2
 *       matches)</li>
 * </ul>
 *
 * <p><strong>Behavior:</strong></p>
 * <ul>
 *   <li>Single term queries: Returns TermQuery (minimum_should_match is ignored)</li>
 *   <li>Multiple term queries: Returns BooleanQuery with should clauses and minimum match requirement</li>
 *   <li>Null/empty queries: Throws ParseException</li>
 *   <li>Whitespace-only queries: Throws ParseException</li>
 * </ul>
 *
 * <p>This parser extends Lucene's QueryParserBase and implements the required abstract methods.
 * It uses the provided Analyzer for tokenization and creates appropriate Lucene Boolean queries.</p>
 */
public class MinimumShouldMatchQueryParser extends QueryParserBase {
  /** The field name to search in */
  private final String _field;

  /** The analyzer used for tokenizing the query */
  private final Analyzer _analyzer;

  /** The minimum should match specification (stored as string for dynamic calculation) */
  private String _minimumShouldMatch = "1";

  /** The default operator for combining terms */
  private BooleanClause.Occur _defaultOperator = BooleanClause.Occur.SHOULD;

  /** Pattern for parsing percentage values */
  private static final Pattern PERCENTAGE_PATTERN = Pattern.compile("^(-?\\d+)%$");

  /**
   * Constructs a new MinimumShouldMatchQueryParser with the specified field and analyzer.
   *
   * @param field the field name to search in (must not be null)
   * @param analyzer the analyzer to use for tokenizing queries (must not be null)
   * @throws IllegalArgumentException if field or analyzer is null
   */
  public MinimumShouldMatchQueryParser(String field, Analyzer analyzer) {
    super();
    _field = field;
    _analyzer = analyzer;
  }

  /**
   * Sets the minimum number of should clauses that must match.
   *
   * <p>This method supports the same formats as OpenSearch's minimum_should_match:</p>
   * <ul>
   *   <li><strong>Positive integer:</strong> "3" - at least 3 should clauses must match</li>
   *   <li><strong>Negative integer:</strong> "-2" - at most 2 should clauses can be missing</li>
   *   <li><strong>Positive percentage:</strong> "80%" - at least 80% of should clauses must match</li>
   *   <li><strong>Negative percentage:</strong> "-20%" - at most 20% of should clauses can be missing</li>
   * </ul>
   *
   * <p>Examples:</p>
   * <ul>
   *   <li>setMinimumShouldMatch("3") - requires at least 3 matches</li>
   *   <li>setMinimumShouldMatch("-1") - allows at most 1 to be missing</li>
   *   <li>setMinimumShouldMatch("80%") - requires at least 80% matches</li>
   *   <li>setMinimumShouldMatch("-20%") - allows at most 20% to be missing</li>
   * </ul>
   *
   * @param minimumShouldMatch the minimum should match specification (integer or percentage)
   * @throws IllegalArgumentException if the format is invalid or value is out of range
   */
  public void setMinimumShouldMatch(String minimumShouldMatch) {
    if (minimumShouldMatch == null || minimumShouldMatch.trim().isEmpty()) {
      _minimumShouldMatch = "1";
      return;
    }

    String value = minimumShouldMatch.trim();

    // Validate the format but store as string for dynamic calculation
    Matcher matcher = PERCENTAGE_PATTERN.matcher(value);
    if (matcher.matches()) {
      int percentage = Integer.parseInt(matcher.group(1));
      if (percentage < -100 || percentage > 100) {
        throw new IllegalArgumentException("Percentage must be between -100 and 100: " + percentage);
      }
      _minimumShouldMatch = value;
    } else {
      // Try to parse as integer to validate
      try {
        Integer.parseInt(value);
        _minimumShouldMatch = value;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid minimum_should_match format: " + value
            + ". Expected integer or percentage (e.g., '3', '-2', '80%', '-20%')");
      }
    }
  }

  /**
   * Sets the default operator for combining terms.
   *
   * @param defaultOperator the default operator (MUST for AND, SHOULD for OR)
   */
  public void setDefaultOperator(BooleanClause.Occur defaultOperator) {
    _defaultOperator = defaultOperator;
  }

  /**
   * Parses the given query string and returns an appropriate Lucene Query.
   *
   * <p>This method performs the following steps:</p>
   * <ol>
   *   <li>Validates the input query (null, empty, whitespace-only)</li>
   *   <li>Tokenizes the query using the configured analyzer</li>
   *   <li>Creates appropriate Lucene queries based on the number of tokens and minimum_should_match setting</li>
   * </ol>
   *
   * <p><strong>Query Types Returned:</strong></p>
   * <ul>
   *   <li><strong>Single term:</strong> TermQuery (minimum_should_match is ignored)</li>
   *   <li><strong>Multiple terms:</strong> BooleanQuery with should clauses and minimum match requirement</li>
   * </ul>
   *
   * @param query the query string to parse (must not be null or empty)
   * @return a Lucene Query object representing the parsed query
   * @throws ParseException if the query is null, empty, or contains no valid tokens after tokenization
   * @throws RuntimeException if tokenization fails due to an IOException
   */
  @Override
  public Query parse(String query)
      throws ParseException {
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
      return new TermQuery(new Term(_field, tokens.get(0)));
    }

    // Handle multiple tokens case - create BooleanQuery with should clauses
    BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();

    for (String token : tokens) {
      booleanQueryBuilder.add(new TermQuery(new Term(_field, token)), _defaultOperator);
    }

    // Calculate the actual minimum should match value
    int actualMinimumShouldMatch = calculateMinimumShouldMatch(tokens.size());

    // Set the minimum should match on the BooleanQuery
    booleanQueryBuilder.setMinimumNumberShouldMatch(actualMinimumShouldMatch);

    return booleanQueryBuilder.build();
  }

    /**
   * Calculates the actual minimum should match value based on the number of tokens.
   *
   * <p>This method handles the different formats of minimum_should_match:</p>
   * <ul>
   *   <li>Positive integer: returns the value directly</li>
   *   <li>Negative integer: returns (total tokens + negative value)</li>
   *   <li>Positive percentage: returns (total tokens * percentage / 100), rounded down</li>
   *   <li>Negative percentage: returns (total tokens * (100 + percentage) / 100), rounded down</li>
   * </ul>
   *
   * @param totalTokens the total number of tokens in the query
   * @return the calculated minimum should match value
   */
  private int calculateMinimumShouldMatch(int totalTokens) {
    String value = _minimumShouldMatch.trim();
    
    // Check if it's a percentage
    Matcher matcher = PERCENTAGE_PATTERN.matcher(value);
    if (matcher.matches()) {
      int percentage = Integer.parseInt(matcher.group(1));
      if (percentage > 0) {
        // Positive percentage: (total * percentage / 100)
        int minimumMatches = (totalTokens * percentage) / 100;
        return Math.max(1, minimumMatches);
      } else {
        // Negative percentage: (total * (100 + percentage) / 100)
        int minimumMatches = (totalTokens * (100 + percentage)) / 100;
        return Math.max(1, minimumMatches);
      }
    } else {
      // Integer calculation
      int intValue = Integer.parseInt(value);
      if (intValue > 0) {
        // Positive integer: return the value directly, but cap at total tokens
        return Math.min(intValue, totalTokens);
      } else {
        // Negative integer: calculate as (total + negative value)
        int minimumMatches = totalTokens + intValue;
        return Math.max(1, minimumMatches);
      }
    }
  }

  /**
   * Reinitializes the parser with a new CharStream.
   *
   * <p>This method is required by QueryParserBase but is not used in this implementation
   * since we override the parse(String) method directly. The method is left as a no-op.</p>
   *
   * @param input the CharStream to reinitialize with (ignored in this implementation)
   */
  @Override
  public void ReInit(CharStream input) {
    // This method is required by QueryParserBase but not used in our implementation
    // since we override parse(String) directly
  }

  /**
   * Creates a top-level query for the specified field.
   *
   * <p>This method is required by QueryParserBase but is not supported in this implementation.
   * Use the parse(String) method instead for query parsing.</p>
   *
   * @param field the field name (ignored in this implementation)
   * @return never returns (always throws UnsupportedOperationException)
   * @throws ParseException never thrown (method always throws UnsupportedOperationException)
   * @throws UnsupportedOperationException always thrown, indicating this method is not supported
   */
  @Override
  public Query TopLevelQuery(String field)
      throws ParseException {
    throw new UnsupportedOperationException(
        "TopLevelQuery is not supported in MinimumShouldMatchQueryParser. Use parse(String) method instead.");
  }
}
