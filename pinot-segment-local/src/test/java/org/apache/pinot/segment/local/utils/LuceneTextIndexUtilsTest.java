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

import java.util.Map;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

public class LuceneTextIndexUtilsTest {
  @Test
  public void testBooleanQueryRewrittenToSpanQuery() {
    // Test 1: The input is a boolean query with 2 clauses: "*pache pino*"
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    WildcardQuery wildcardQuery = new WildcardQuery(new Term("field", "*apche"));
    PrefixQuery prefixQuery = new PrefixQuery(new Term("field", "pino"));
    builder.add(new BooleanClause(wildcardQuery, BooleanClause.Occur.SHOULD))
        .add(new BooleanClause(prefixQuery, BooleanClause.Occur.SHOULD));

    SpanQuery[] spanQueries1 =
        {new SpanMultiTermQueryWrapper<>(wildcardQuery), new SpanMultiTermQueryWrapper<>(prefixQuery)};
    SpanQuery expectedQuery = new SpanNearQuery(spanQueries1, 0, true);
    Assert.assertEquals(expectedQuery, LuceneTextIndexUtils.convertToMultiTermSpanQuery(builder.build()));

    // Test 2: The input is a boolean query with 3 clauses: "*pache real pino*"
    builder = new BooleanQuery.Builder();
    Term term = new Term("field", "real");
    builder.add(new BooleanClause(wildcardQuery, BooleanClause.Occur.SHOULD))
        .add(new BooleanClause(new TermQuery(term), BooleanClause.Occur.SHOULD))
        .add(new BooleanClause(prefixQuery, BooleanClause.Occur.SHOULD));

    SpanQuery[] spanQueries2 =
        {new SpanMultiTermQueryWrapper<>(wildcardQuery), new SpanTermQuery(term), new SpanMultiTermQueryWrapper<>(
            prefixQuery)};
    expectedQuery = new SpanNearQuery(spanQueries2, 0, true);
    Assert.assertEquals(expectedQuery, LuceneTextIndexUtils.convertToMultiTermSpanQuery(builder.build()));

    // Test 3: The input is a boolean query with 3 clauses: "*pache real* pino*"
    builder = new BooleanQuery.Builder();
    builder.add(new BooleanClause(wildcardQuery, BooleanClause.Occur.SHOULD))
        .add(new BooleanClause(prefixQuery, BooleanClause.Occur.SHOULD))
        .add(new BooleanClause(prefixQuery, BooleanClause.Occur.SHOULD));

    SpanQuery[] spanQueries3 = {new SpanMultiTermQueryWrapper<>(wildcardQuery), new SpanMultiTermQueryWrapper<>(
        prefixQuery), new SpanMultiTermQueryWrapper<>(prefixQuery)};
    expectedQuery = new SpanNearQuery(spanQueries3, 0, true);
    Assert.assertEquals(expectedQuery, LuceneTextIndexUtils.convertToMultiTermSpanQuery(builder.build()));

    // Test 4: The input is a boolean query with 1 clause: "*pino*".
    WildcardQuery wildcardQuery1 = new WildcardQuery(new Term("field", "*pino*"));
    builder = new BooleanQuery.Builder();
    builder.add(new BooleanClause(wildcardQuery1, BooleanClause.Occur.SHOULD));
    SpanQuery[] spanQueries4 = {new SpanMultiTermQueryWrapper<>(wildcardQuery1)};
    expectedQuery = new SpanNearQuery(spanQueries4, 0, true);
    Assert.assertEquals(expectedQuery, LuceneTextIndexUtils.convertToMultiTermSpanQuery(builder.build()));

    // Test 5: Boolean queries without any wildcard/prefix subqueries are left unchanged.
    builder = new BooleanQuery.Builder();
    builder.add(new BooleanClause(new TermQuery(term), BooleanClause.Occur.SHOULD))
        .add(new BooleanClause(new TermQuery(term), BooleanClause.Occur.SHOULD));
    BooleanQuery q = builder.build();
    Assert.assertEquals(q, LuceneTextIndexUtils.convertToMultiTermSpanQuery(q));
  }

  @Test
  public void testQueryIsNotRewritten() {
    // Test 1: Term query is not re-written.
    TermQuery termQuery = new TermQuery(new Term("field", "real"));
    Assert.assertEquals(termQuery, LuceneTextIndexUtils.convertToMultiTermSpanQuery(termQuery));
    // Test 2: Regex query is not re-written.
    RegexpQuery regexpQuery = new RegexpQuery(new Term("field", "\\d+"));
    Assert.assertEquals(regexpQuery, LuceneTextIndexUtils.convertToMultiTermSpanQuery(regexpQuery));
  }

  @Test
  public void testParseOptionsFromSearchString() {
    // Test single term with options
    Map.Entry<String, Map<String, String>> result =
        LuceneTextIndexUtils.parseOptionsFromSearchString("term1 __OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test multiple terms with AND
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 AND term2 __OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test multiple terms with OR
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 OR term2 __OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 OR term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test multiple terms with NOT
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 NOT term2 __OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 NOT term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test complex combinations
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 NOT term2 OR term3 __OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 NOT term2 OR term3");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test multiple options in single __OPTIONS
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "term1 AND term2 __OPTIONS(parser=CLASSIC,allowLeadingWildcard=true)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");
    Assert.assertEquals(result.getValue().get("allowLeadingWildcard"), "true");

    // Test multiple __OPTIONS in query
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "term1 __OPTIONS(parser=CLASSIC) AND term2 __OPTIONS(phraseSlop=2)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");
    Assert.assertEquals(result.getValue().get("phraseSlop"), "2");

    // Test overriding options
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "term1 __OPTIONS(parser=CLASSIC) AND term2 __OPTIONS(parser=STANDARD)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "STANDARD");

    // Test multiple options with overrides
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "term1 __OPTIONS(parser=CLASSIC,allowLeadingWildcard=true) AND "
            + "term2 __OPTIONS(parser=STANDARD,fuzzyMinSim=0.5)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "STANDARD");
    Assert.assertEquals(result.getValue().get("allowLeadingWildcard"), "true");
    Assert.assertEquals(result.getValue().get("fuzzyMinSim"), "0.5");

    // Test redundant operators
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 AND AND term2 __OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test no options
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 AND term2");
    Assert.assertNull(result);

    // Test escaped options
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 AND term2 \\__OPTIONS(parser=CLASSIC)");
    Assert.assertNull(result);
  }

  private void validateQueryPreservation(String originalQuery, String expectedQueryWithoutOptions) {
    Map.Entry<String, Map<String, String>> result = LuceneTextIndexUtils.parseOptionsFromSearchString(originalQuery);
    Assert.assertNotNull(result);
    Assert.assertEquals("Query should be preserved exactly (minus OPTIONS) for: " + originalQuery,
        expectedQueryWithoutOptions, result.getKey());
  }

  @Test
  public void testParseOptionsWithOperators() {
    // Test with OR operator
    String query1 =
        "*ava realtime streaming system* OR *chine learner* __OPTIONS(parser=CLASSIC, allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result1 = LuceneTextIndexUtils.parseOptionsFromSearchString(query1);
    Assert.assertNotNull(result1);
    Assert.assertEquals("*ava realtime streaming system* OR *chine learner*", result1.getKey());
    Assert.assertEquals(2, result1.getValue().size());
    Assert.assertEquals("CLASSIC", result1.getValue().get("parser"));
    Assert.assertEquals("true", result1.getValue().get("allowLeadingWildcard"));

    // Test with AND operator
    String query2 =
        "*ava realtime streaming system* AND *chine learn* __OPTIONS(parser=CLASSIC, allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result2 = LuceneTextIndexUtils.parseOptionsFromSearchString(query2);
    Assert.assertNotNull(result2);
    Assert.assertEquals("*ava realtime streaming system* AND *chine learn*", result2.getKey());
    Assert.assertEquals(2, result2.getValue().size());
    Assert.assertEquals("CLASSIC", result2.getValue().get("parser"));
    Assert.assertEquals("true", result2.getValue().get("allowLeadingWildcard"));

    // Test with multiple operators
    String query3 = "*ava* AND *stream* OR *learn* __OPTIONS(parser=CLASSIC, allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result3 = LuceneTextIndexUtils.parseOptionsFromSearchString(query3);
    Assert.assertNotNull(result3);
    Assert.assertEquals("*ava* AND *stream* OR *learn*", result3.getKey());
    Assert.assertEquals(2, result3.getValue().size());
    Assert.assertEquals("CLASSIC", result3.getValue().get("parser"));
    Assert.assertEquals("true", result3.getValue().get("allowLeadingWildcard"));

    // Test with NOT operator
    String query4 = "NOT *ava realtime streaming system* __OPTIONS(parser=CLASSIC, allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result4 = LuceneTextIndexUtils.parseOptionsFromSearchString(query4);
    Assert.assertNotNull(result4);
    Assert.assertEquals("NOT *ava realtime streaming system*", result4.getKey());
    Assert.assertEquals(2, result4.getValue().size());
    Assert.assertEquals("CLASSIC", result4.getValue().get("parser"));
    Assert.assertEquals("true", result4.getValue().get("allowLeadingWildcard"));

    // Test with complex query and multiple OPTIONS
    String query5 = "*ava* AND *stream* OR *learn* __OPTIONS(parser=CLASSIC) __OPTIONS(allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result5 = LuceneTextIndexUtils.parseOptionsFromSearchString(query5);
    Assert.assertNotNull(result5);
    Assert.assertEquals("*ava* AND *stream* OR *learn*", result5.getKey());
    Assert.assertEquals(2, result5.getValue().size());
    Assert.assertEquals("CLASSIC", result5.getValue().get("parser"));
    Assert.assertEquals("true", result5.getValue().get("allowLeadingWildcard"));
  }

  @Test
  public void testParseOptionsEarlyReturnOptimization() {
    // Test null input - should return null early
    Map.Entry<String, Map<String, String>> result = LuceneTextIndexUtils.parseOptionsFromSearchString(null);
    Assert.assertNull(result);

    // Test empty string - should return null early
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("");
    Assert.assertNull(result);

    // Test string without __OPTIONS - should return null early
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("simple search term");
    Assert.assertNull(result);

    // Test string with partial __OPTIONS - should return null early
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTION");
    Assert.assertNull(result);

    // Test string with partial __OPTIONS - should return null early
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTION(parser=CLASSIC)");
    Assert.assertNull(result);

    // Test string with __OPTIONS but not in correct format - should return null early
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS");
    Assert.assertNull(result);

    // Test string with __OPTIONS in different case - should return null early
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __options(parser=CLASSIC)");
    Assert.assertNull(result);

    // Test string with __OPTIONS as part of another word - should return null early
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search my__OPTIONS(parser=CLASSIC)");
    Assert.assertNull(result);

    // Test string with __OPTIONS at the end without parentheses - should return null early
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __oPTIONS");
    Assert.assertNull(result);
  }

  @Test
  public void testParseOptionsExceptionHandling() {
    // Test malformed options that could cause parsing exceptions
    // These should not throw exceptions but return null instead

    // Test unclosed parentheses
    Map.Entry<String, Map<String, String>> result =
        LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC");
    Assert.assertNull(result);

    // Test empty parentheses
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS()");
    Assert.assertNull(result);

    // Test malformed option format
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser)");
    Assert.assertNull(result);

    // Test option with empty value
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=)");
    Assert.assertNull(result);

    // Test option with empty key
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(=CLASSIC)");
    Assert.assertNull(result);

    // Test multiple equals signs
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC=EXTRA)");
    Assert.assertNull(result);

    // Test nested parentheses
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=(CLASSIC))");
    Assert.assertNull(result);

    // Test nested parentheses in option value
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, test=(value))");
    Assert.assertNull(result);

    // Test nested parentheses in option key
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS((parser)=CLASSIC)");
    Assert.assertNull(result);

    // Test special characters that might cause regex issues
    result =
        LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, test=value with spaces)");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));
    Assert.assertEquals("value with spaces", result.getValue().get("test"));

    // Test unicode characters
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, test=测试)");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));

    // Test very long option values that might cause stack overflow
    StringBuilder longValue = new StringBuilder("search __OPTIONS(parser=CLASSIC, test=");
    for (int i = 0; i < 10000; i++) {
      longValue.append("a");
    }
    longValue.append(")");
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(longValue.toString());
    Assert.assertNull(result);
  }

  @Test
  public void testParseOptionsValidation() {
    // Test nested parentheses validation
    Map.Entry<String, Map<String, String>> result;

    // Test simple nested parentheses
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=(CLASSIC))");
    Assert.assertNull(result);

    // Test complex nested parentheses
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, test=(A(B)C))");
    Assert.assertNull(result);

    // Test parentheses in option value
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "search __OPTIONS(parser=CLASSIC, test=value(with)parentheses)");
    Assert.assertNull(result);

    // Test parentheses in option key
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, (test)=value)");
    Assert.assertNull(result);

    // Test empty key validation
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(=CLASSIC)");
    Assert.assertNull(result);

    // Test empty value validation
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=)");
    Assert.assertNull(result);

    // Test empty key and value
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(=)");
    Assert.assertNull(result);

    // Test whitespace-only key
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(  =CLASSIC)");
    Assert.assertNull(result);

    // Test whitespace-only value
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=  )");
    Assert.assertNull(result);

    // Test invalid option format (no equals sign)
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser)");
    Assert.assertNull(result);

    // Test invalid option format (multiple equals signs)
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC=EXTRA)");
    Assert.assertNull(result);

    // Test mixed valid and invalid options
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, invalid, test=true)");
    Assert.assertNull(result);

    // Test valid options (should work)
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, test=true)");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));
    Assert.assertEquals("true", result.getValue().get("test"));
  }

  @Test
  public void testParseOptionsEdgeCases() {
    // Test whitespace variations
    Map.Entry<String, Map<String, String>> result =
        LuceneTextIndexUtils.parseOptionsFromSearchString("  search  __OPTIONS(  parser=CLASSIC  )  ");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey().trim());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));

    // Test multiple spaces in option parsing
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "search __OPTIONS(parser=CLASSIC,   allowLeadingWildcard=true)");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));
    Assert.assertEquals("true", result.getValue().get("allowLeadingWildcard"));

    // Test options with special characters in values (but valid format)
    result =
        LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, test=value-with-dashes)");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));
    Assert.assertEquals("value-with-dashes", result.getValue().get("test"));

    // Test options with underscores in values
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "search __OPTIONS(parser=CLASSIC, test=value_with_underscores)");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));
    Assert.assertEquals("value_with_underscores", result.getValue().get("test"));

    // Test options with dots in values
    result =
        LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, test=value.with.dots)");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));
    Assert.assertEquals("value.with.dots", result.getValue().get("test"));

    // Test options with numbers in values
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, test=value123)");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));
    Assert.assertEquals("value123", result.getValue().get("test"));

    // Test options with mixed alphanumeric values
    result =
        LuceneTextIndexUtils.parseOptionsFromSearchString("search __OPTIONS(parser=CLASSIC, test=value-123_with.dots)");
    Assert.assertNotNull(result);
    Assert.assertEquals("search", result.getKey());
    Assert.assertEquals("CLASSIC", result.getValue().get("parser"));
    Assert.assertEquals("value-123_with.dots", result.getValue().get("test"));
  }
}
