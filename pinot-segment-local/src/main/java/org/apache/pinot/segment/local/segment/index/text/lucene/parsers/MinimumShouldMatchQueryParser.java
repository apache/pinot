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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.charstream.CharStream;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;


/**
 * A custom query parser that implements minimum_should_match behavior.
 * This parser creates Boolean queries with should clauses and enforces a minimum
 * number of matches.
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
   * Validates the minimum should match specification.
   *
   * <p>This method validates the format and range of the minimum_should_match value:</p>
   * <ul>
   *   <li><strong>Positive integer:</strong> "3" - at least 3 should clauses must match</li>
   *   <li><strong>Negative integer:</strong> "-2" - at most 2 should clauses can be missing</li>
   *   <li><strong>Positive percentage:</strong> "80%" - at least 80% of should clauses must match</li>
   *   <li><strong>Negative percentage:</strong> "-20%" - at most 20% of should clauses can be missing</li>
   * </ul>
   *
   * @param minimumShouldMatch the minimum should match specification to validate
   * @return the validated and trimmed value
   * @throws IllegalArgumentException if the format is invalid or value is out of range
   */
  private String validateMinimumShouldMatch(String minimumShouldMatch) {
    if (minimumShouldMatch == null || minimumShouldMatch.trim().isEmpty()) {
      return "1";
    }

    String value = minimumShouldMatch.trim();

    // Validate the format but store as string for dynamic calculation
    Matcher matcher = PERCENTAGE_PATTERN.matcher(value);
    if (matcher.matches()) {
      int percentage = Integer.parseInt(matcher.group(1));
      if (percentage < -100 || percentage > 100) {
        throw new IllegalArgumentException("Percentage must be between -100 and 100: " + percentage);
      }
      return value;
    } else {
      try {
        Integer.parseInt(value);
        return value;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid minimum_should_match format: " + value
            + ". Expected integer or percentage (e.g., '3', '-2', '80%', '-20%')");
      }
    }
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
    _minimumShouldMatch = validateMinimumShouldMatch(minimumShouldMatch);
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
   *   <li>Parses the query using Lucene's QueryParser</li>
   *   <li>Applies minimum_should_match behavior to Boolean queries</li>
   * </ol>
   *
   * @param query the query string to parse (must not be null or empty)
   * @return a Lucene Query object representing the parsed query
   * @throws ParseException if the query is null, empty, or parsing fails
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

    // Parse the query using Lucene's QueryParser
    QueryParser parser = new QueryParser(_field, _analyzer);
    Query parsedQuery = parser.parse(query);

    // If it's a Boolean query, apply minimum_should_match behavior
    if (parsedQuery instanceof BooleanQuery) {
      return applyMinimumShouldMatch((BooleanQuery) parsedQuery);
    }

    // For single term queries, convert to Boolean query with SHOULD clause
    // For single terms, minimum_should_match should always be 1
    if (parsedQuery instanceof TermQuery) {
      BooleanQuery.Builder builder = new BooleanQuery.Builder();
      builder.add(parsedQuery, BooleanClause.Occur.SHOULD);
      builder.setMinimumNumberShouldMatch(1);
      return builder.build();
    }

    // For other query types, throw exception
    throw new ParseException("MinimumShouldMatchQueryParser only supports Boolean queries and single term queries. "
        + "Received: " + parsedQuery.getClass().getSimpleName());
  }

  /**
   * Applies minimum_should_match behavior to a BooleanQuery.
   *
   * @param booleanQuery the BooleanQuery to modify
   * @return the modified BooleanQuery with minimum_should_match applied
   */
  private Query applyMinimumShouldMatch(BooleanQuery booleanQuery) {
    int shouldClauseCount = 0;
    boolean hasMustOrFilterClauses = false;

    for (BooleanClause clause : booleanQuery.clauses()) {
      if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
        shouldClauseCount++;
      } else if (clause.getOccur() == BooleanClause.Occur.MUST || clause.getOccur() == BooleanClause.Occur.FILTER) {
        hasMustOrFilterClauses = true;
      }
    }

    // If no should clauses, return the original query
    if (shouldClauseCount == 0) {
      return booleanQuery;
    }

    int minimumShouldMatch;
    if (hasMustOrFilterClauses) {
      minimumShouldMatch = 0;
    } else {
      minimumShouldMatch = 1;
    }

    // Override with configured minimum_should_match if set
    if (!_minimumShouldMatch.equals("1")) {
      minimumShouldMatch = calculateMinimumShouldMatch(shouldClauseCount, _minimumShouldMatch);
    }

    // Create a new BooleanQuery with the minimum should match
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (BooleanClause clause : booleanQuery.clauses()) {
      builder.add(clause);
    }
    builder.setMinimumNumberShouldMatch(minimumShouldMatch);

    return builder.build();
  }

  /**
   * Calculates the actual minimum should match value based on the number of tokens and the specified value.
   *
   * @param totalTokens the total number of tokens in the query
   * @param minimumShouldMatchValue the minimum should match specification
   * @return the calculated minimum should match value
   */
  private int calculateMinimumShouldMatch(int totalTokens, String minimumShouldMatchValue) {
    String value = minimumShouldMatchValue.trim();

    // Check if it's a percentage
    Matcher matcher = PERCENTAGE_PATTERN.matcher(value);
    if (matcher.matches()) {
      int percentage = Integer.parseInt(matcher.group(1));
      if (percentage > 0) {
        int minimumMatches = (totalTokens * percentage) / 100;
        return Math.max(0, minimumMatches);
      } else {
        int minimumMatches = (totalTokens * (100 + percentage)) / 100;
        return Math.max(0, minimumMatches);
      }
    } else {
      int intValue = Integer.parseInt(value);
      if (intValue > 0) {
        return Math.min(intValue, totalTokens);
      } else {
        int minimumMatches = totalTokens + intValue;
        return Math.max(0, minimumMatches);
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
