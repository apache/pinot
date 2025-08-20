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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.testng.Assert;
import org.testng.annotations.Test;


public class MinimumShouldMatchQueryParserTest {

  private static final String FIELD_NAME = "content";

  /**
   * Helper method to parse query with minimum_should_match option and return the result.
   *
   * @param query the query string to parse
   * @param minimumShouldMatch the minimum_should_match value (can be null)
   * @return the parsed Query
   * @throws ParseException if parsing fails
   */
  private Query parseQueryWithMinimumShouldMatch(String query, String minimumShouldMatch)
      throws ParseException {
    MinimumShouldMatchQueryParser parser = new MinimumShouldMatchQueryParser(FIELD_NAME, new StandardAnalyzer());
    if (minimumShouldMatch != null) {
      parser.setMinimumShouldMatch(minimumShouldMatch);
    }
    return parser.parse(query);
  }

  @Test
  public void testPositiveCases()
      throws ParseException {
    // Case 1: setMinimumShouldMatch("3") - requires at least 3 matches
    Query result1 = parseQueryWithMinimumShouldMatch("java OR python OR scala OR kotlin OR rust", "3");
    Assert.assertTrue(result1 instanceof BooleanQuery);
    BooleanQuery booleanQuery1 = (BooleanQuery) result1;
    Assert.assertEquals(booleanQuery1.clauses().size(), 5);
    Assert.assertEquals(booleanQuery1.getMinimumNumberShouldMatch(), 3);

    // Case 2: setMinimumShouldMatch("-1") - allows at most 1 to be missing
    Query result2 = parseQueryWithMinimumShouldMatch("java OR python OR scala OR kotlin", "-1");
    Assert.assertTrue(result2 instanceof BooleanQuery);
    BooleanQuery booleanQuery2 = (BooleanQuery) result2;
    Assert.assertEquals(booleanQuery2.clauses().size(), 4);
    // -1 means at most 1 can be missing, so 3 must match (4 - 1 = 3)
    Assert.assertEquals(booleanQuery2.getMinimumNumberShouldMatch(), 3);

    // Case 3: setMinimumShouldMatch("80%") - requires at least 80% matches
    Query result3 = parseQueryWithMinimumShouldMatch("java OR python OR scala OR kotlin OR rust", "80%");
    Assert.assertTrue(result3 instanceof BooleanQuery);
    BooleanQuery booleanQuery3 = (BooleanQuery) result3;
    Assert.assertEquals(booleanQuery3.clauses().size(), 5);
    // 80% of 5 = 4 matches required
    Assert.assertEquals(booleanQuery3.getMinimumNumberShouldMatch(), 4);

    // Case 4: setMinimumShouldMatch("-20%") - allows at most 20% to be missing
    Query result4 = parseQueryWithMinimumShouldMatch("java OR python OR scala OR kotlin OR rust", "-20%");
    Assert.assertTrue(result4 instanceof BooleanQuery);
    BooleanQuery booleanQuery4 = (BooleanQuery) result4;
    Assert.assertEquals(booleanQuery4.clauses().size(), 5);
    // -20% means at most 20% can be missing, so 80% must match
    // 80% of 5 = 4 matches required
    Assert.assertEquals(booleanQuery4.getMinimumNumberShouldMatch(), 4);
  }

  @Test
  public void testNegativeCases() {
    // Case 1: Invalid percentage value (> 100%)
    try {
      parseQueryWithMinimumShouldMatch("java OR python", "101%");
      Assert.fail("Should throw IllegalArgumentException for invalid percentage");
    } catch (IllegalArgumentException e) {
      // Expected
    } catch (ParseException e) {
      Assert.fail("Should throw IllegalArgumentException, not ParseException");
    }

    // Case 2: Invalid negative percentage value (< -100%)
    try {
      parseQueryWithMinimumShouldMatch("java OR python", "-101%");
      Assert.fail("Should throw IllegalArgumentException for invalid negative percentage");
    } catch (IllegalArgumentException e) {
      // Expected
    } catch (ParseException e) {
      Assert.fail("Should throw IllegalArgumentException, not ParseException");
    }

    // Case 3: Invalid format (not integer or percentage)
    try {
      parseQueryWithMinimumShouldMatch("java OR python", "abc");
      Assert.fail("Should throw IllegalArgumentException for invalid format");
    } catch (IllegalArgumentException e) {
      // Expected
    } catch (ParseException e) {
      Assert.fail("Should throw IllegalArgumentException, not ParseException");
    }

    // Case 4: Invalid decimal percentage
    try {
      parseQueryWithMinimumShouldMatch("java OR python", "50.5%");
      Assert.fail("Should throw IllegalArgumentException for invalid decimal percentage");
    } catch (IllegalArgumentException e) {
      // Expected
    } catch (ParseException e) {
      Assert.fail("Should throw IllegalArgumentException, not ParseException");
    }

    // Case 5: Null query
    try {
      parseQueryWithMinimumShouldMatch(null, null);
      Assert.fail("Should throw ParseException for null query");
    } catch (ParseException e) {
      // Expected
    }

    // Case 6: Empty query
    try {
      parseQueryWithMinimumShouldMatch("", null);
      Assert.fail("Should throw ParseException for empty query");
    } catch (ParseException e) {
      // Expected
    }

    // Case 7: Whitespace-only query
    try {
      parseQueryWithMinimumShouldMatch("   ", null);
      Assert.fail("Should throw ParseException for whitespace-only query");
    } catch (ParseException e) {
      // Expected
    }

    // Case 8: Single term query should work with explicit minimum_should_match
    try {
      Query result8 = parseQueryWithMinimumShouldMatch("java", "1");
      Assert.assertTrue(result8 instanceof BooleanQuery);
      BooleanQuery booleanQuery8 = (BooleanQuery) result8;
      Assert.assertEquals(booleanQuery8.clauses().size(), 1);
      Assert.assertEquals(booleanQuery8.getMinimumNumberShouldMatch(), 1);
    } catch (ParseException e) {
      Assert.fail("Single term query should work with minimum_should_match: " + e.getMessage());
    }

    // Case 8b: Single term query should work without minimum_should_match (defaults to 1)
    try {
      Query result8b = parseQueryWithMinimumShouldMatch("java", null);
      Assert.assertTrue(result8b instanceof BooleanQuery);
      BooleanQuery booleanQuery8b = (BooleanQuery) result8b;
      Assert.assertEquals(booleanQuery8b.clauses().size(), 1);
      Assert.assertEquals(booleanQuery8b.getMinimumNumberShouldMatch(), 1);
    } catch (ParseException e) {
      Assert.fail("Single term query should work without minimum_should_match: " + e.getMessage());
    }

    // Case 9: Non-Boolean query (phrase query)
    try {
      parseQueryWithMinimumShouldMatch("\"java programming\"", null);
      Assert.fail("Should throw ParseException for non-Boolean query (phrase)");
    } catch (ParseException e) {
      // Expected - should contain the error message about Boolean queries
      Assert.assertTrue(e.getMessage().contains("Boolean queries"),
          "Error message should mention Boolean queries, got: " + e.getMessage());
    }

    // Case 10: Non-Boolean query (wildcard query)
    try {
      parseQueryWithMinimumShouldMatch("java*", null);
      Assert.fail("Should throw ParseException for non-Boolean query (wildcard)");
    } catch (ParseException e) {
      // Expected - should contain the error message about Boolean queries
      Assert.assertTrue(e.getMessage().contains("Boolean queries"),
          "Error message should mention Boolean queries, got: " + e.getMessage());
    }
  }

  @Test
  public void testEdgeCases()
      throws ParseException {
    // Case 1: minimum_should_match value greater than number of should clauses (boundary)
    Query result1 = parseQueryWithMinimumShouldMatch("java OR python", "5");
    Assert.assertTrue(result1 instanceof BooleanQuery);
    BooleanQuery booleanQuery1 = (BooleanQuery) result1;
    Assert.assertEquals(booleanQuery1.clauses().size(), 2);
    // Should cap at the number of available clauses (2)
    Assert.assertEquals(booleanQuery1.getMinimumNumberShouldMatch(), 2);

    // Case 2: negative minimum_should_match that would result in negative value (boundary)
    Query result2 = parseQueryWithMinimumShouldMatch("java OR python", "-5");
    Assert.assertTrue(result2 instanceof BooleanQuery);
    BooleanQuery booleanQuery2 = (BooleanQuery) result2;
    Assert.assertEquals(booleanQuery2.clauses().size(), 2);
    // Should not go below 0 (2 - 5 = -3, but capped at 0)
    Assert.assertEquals(booleanQuery2.getMinimumNumberShouldMatch(), 0);

    // Case 3: negative percentage that would result in zero (boundary)
    Query result3 = parseQueryWithMinimumShouldMatch("java OR python", "-80%");
    Assert.assertTrue(result3 instanceof BooleanQuery);
    BooleanQuery booleanQuery3 = (BooleanQuery) result3;
    Assert.assertEquals(booleanQuery3.clauses().size(), 2);
    // -80% means 20% must match, but 20% of 2 = 0.4, rounded down to 0
    Assert.assertEquals(booleanQuery3.getMinimumNumberShouldMatch(), 0);

    // Case 4: minimum_should_match value equal to number of should clauses (boundary)
    Query result4 = parseQueryWithMinimumShouldMatch("java OR python OR scala", "3");
    Assert.assertTrue(result4 instanceof BooleanQuery);
    BooleanQuery booleanQuery4 = (BooleanQuery) result4;
    Assert.assertEquals(booleanQuery4.clauses().size(), 3);
    // Should require all 3 clauses to match
    Assert.assertEquals(booleanQuery4.getMinimumNumberShouldMatch(), 3);

    // Case 5: percentage that results in decimal value (boundary rounding)
    Query result5 = parseQueryWithMinimumShouldMatch("java OR python OR scala OR kotlin", "75%");
    Assert.assertTrue(result5 instanceof BooleanQuery);
    BooleanQuery booleanQuery5 = (BooleanQuery) result5;
    Assert.assertEquals(booleanQuery5.clauses().size(), 4);
    // 75% of 4 = 3.0, should round down to 3
    Assert.assertEquals(booleanQuery5.getMinimumNumberShouldMatch(), 3);
  }
}
