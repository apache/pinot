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
    // Test 1: MUST_SHOULD_80_percent - OpenSearch AND (one OR two OR three OR four) with minimumShouldMatch=80%
    Query result1 = parseQueryWithMinimumShouldMatch("OpenSearch AND (one OR two OR three OR four)", "80%");
    Assert.assertTrue(result1 instanceof BooleanQuery);
    BooleanQuery booleanQuery1 = (BooleanQuery) result1;
    // Should have 2 clauses: MUST(OpenSearch) and MUST(nested BooleanQuery)
    Assert.assertEquals(booleanQuery1.clauses().size(), 2);
    // The nested BooleanQuery should have minimumShouldMatch=3 (80% of 4 = 3.2, rounded down to 3)
    BooleanQuery nestedQuery1 = (BooleanQuery) booleanQuery1.clauses().get(1).getQuery();
    Assert.assertEquals(nestedQuery1.getMinimumNumberShouldMatch(), 3);

    // Test 2: MUST_SHOULD_negative_20_percent - OpenSearch AND (one OR two OR three OR four) with
    // minimumShouldMatch=-20%
    Query result2 = parseQueryWithMinimumShouldMatch("OpenSearch AND (one OR two OR three OR four)", "-20%");
    Assert.assertTrue(result2 instanceof BooleanQuery);
    BooleanQuery booleanQuery2 = (BooleanQuery) result2;
    Assert.assertEquals(booleanQuery2.clauses().size(), 2);
    // The nested BooleanQuery should have minimumShouldMatch=3 (100+(-20)=80% of 4 = 3.2, rounded down to 3)
    BooleanQuery nestedQuery2 = (BooleanQuery) booleanQuery2.clauses().get(1).getQuery();
    Assert.assertEquals(nestedQuery2.getMinimumNumberShouldMatch(), 3);

    // Test 3: SHOULD_only_default_one - one OR two OR three OR four without minimumShouldMatch
    Query result3 = parseQueryWithMinimumShouldMatch("one OR two OR three OR four", null);
    Assert.assertTrue(result3 instanceof BooleanQuery);
    BooleanQuery booleanQuery3 = (BooleanQuery) result3;
    Assert.assertEquals(booleanQuery3.clauses().size(), 4);
    // Default minimumShouldMatch should be 1 for SHOULD-only queries
    Assert.assertEquals(booleanQuery3.getMinimumNumberShouldMatch(), 1);

    // Test 4: SHOULD_minimum_2 - one OR two OR three OR four with minimumShouldMatch=2
    Query result4 = parseQueryWithMinimumShouldMatch("one OR two OR three OR four", "2");
    Assert.assertTrue(result4 instanceof BooleanQuery);
    BooleanQuery booleanQuery4 = (BooleanQuery) result4;
    Assert.assertEquals(booleanQuery4.clauses().size(), 4);
    Assert.assertEquals(booleanQuery4.getMinimumNumberShouldMatch(), 2);

    // Test 5: SHOULD_75_percent - one OR two OR three OR four with minimumShouldMatch=75%
    Query result5 = parseQueryWithMinimumShouldMatch("one OR two OR three OR four", "75%");
    Assert.assertTrue(result5 instanceof BooleanQuery);
    BooleanQuery booleanQuery5 = (BooleanQuery) result5;
    Assert.assertEquals(booleanQuery5.clauses().size(), 4);
    // 75% of 4 = 3 matches required
    Assert.assertEquals(booleanQuery5.getMinimumNumberShouldMatch(), 3);

    // Test 6: SHOULD_100_percent - one OR two OR three OR four with minimumShouldMatch=100%
    Query result6 = parseQueryWithMinimumShouldMatch("one OR two OR three OR four", "100%");
    Assert.assertTrue(result6 instanceof BooleanQuery);
    BooleanQuery booleanQuery6 = (BooleanQuery) result6;
    Assert.assertEquals(booleanQuery6.clauses().size(), 4);
    // 100% of 4 = 4 matches required
    Assert.assertEquals(booleanQuery6.getMinimumNumberShouldMatch(), 4);

    // Test 7: SHOULD_25_percent - one OR two OR three OR four with minimumShouldMatch=25%
    Query result7 = parseQueryWithMinimumShouldMatch("one OR two OR three OR four", "25%");
    Assert.assertTrue(result7 instanceof BooleanQuery);
    BooleanQuery booleanQuery7 = (BooleanQuery) result7;
    Assert.assertEquals(booleanQuery7.clauses().size(), 4);
    // 25% of 4 = 1 match required
    Assert.assertEquals(booleanQuery7.getMinimumNumberShouldMatch(), 1);

    // Test 8: single_term_query - OpenSearch with minimumShouldMatch=1
    Query result8 = parseQueryWithMinimumShouldMatch("OpenSearch", "1");
    Assert.assertTrue(result8 instanceof BooleanQuery);
    BooleanQuery booleanQuery8 = (BooleanQuery) result8;
    Assert.assertEquals(booleanQuery8.clauses().size(), 1);
    Assert.assertEquals(booleanQuery8.getMinimumNumberShouldMatch(), 1);

    // Test 9: SHOULD_negative_50_percent - one OR two OR three OR four with minimumShouldMatch=-50%
    Query result9 = parseQueryWithMinimumShouldMatch("one OR two OR three OR four", "-50%");
    Assert.assertTrue(result9 instanceof BooleanQuery);
    BooleanQuery booleanQuery9 = (BooleanQuery) result9;
    Assert.assertEquals(booleanQuery9.clauses().size(), 4);
    // -50% means 50% must match, so 50% of 4 = 2 matches required
    Assert.assertEquals(booleanQuery9.getMinimumNumberShouldMatch(), 2);

    // Test 10: Deep nested query - OpenSearch AND ((one OR two) AND (three OR four OR five)) with
    // minimumShouldMatch=60%
    Query result10 = parseQueryWithMinimumShouldMatch(
        "OpenSearch AND ((one OR two) AND (three OR four OR five))", "60%");
    Assert.assertTrue(result10 instanceof BooleanQuery);
    BooleanQuery booleanQuery10 = (BooleanQuery) result10;
    Assert.assertEquals(booleanQuery10.clauses().size(), 2);

    // Get the nested BooleanQuery: ((one OR two) AND (three OR four OR five))
    BooleanQuery nestedQuery10 = (BooleanQuery) booleanQuery10.clauses().get(1).getQuery();
    Assert.assertEquals(nestedQuery10.clauses().size(), 2);

    // Get the first sub-nested BooleanQuery: (one OR two)
    BooleanQuery subNested1 = (BooleanQuery) nestedQuery10.clauses().get(0).getQuery();
    Assert.assertEquals(subNested1.clauses().size(), 2);
    // 60% of 2 = 1.2, rounded down to 1
    Assert.assertEquals(subNested1.getMinimumNumberShouldMatch(), 1);

    // Get the second sub-nested BooleanQuery: (three OR four OR five)
    BooleanQuery subNested2 = (BooleanQuery) nestedQuery10.clauses().get(1).getQuery();
    Assert.assertEquals(subNested2.clauses().size(), 3);
    // 60% of 3 = 1.8, rounded down to 1
    Assert.assertEquals(subNested2.getMinimumNumberShouldMatch(), 1);
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
