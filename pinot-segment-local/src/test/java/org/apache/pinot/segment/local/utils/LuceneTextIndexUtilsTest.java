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
        LuceneTextIndexUtils.parseOptionsFromSearchString("term1 __PINOT_OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test multiple terms with AND
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 AND term2 __PINOT_OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test multiple terms with OR
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 OR term2 __PINOT_OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 OR term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test multiple terms with NOT
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 NOT term2 __PINOT_OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 NOT term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test complex combinations
    result =
        LuceneTextIndexUtils.parseOptionsFromSearchString("term1 NOT term2 OR term3 __PINOT_OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 NOT term2 OR term3");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test multiple options in single __PINOT_OPTIONS
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "term1 AND term2 __PINOT_OPTIONS(parser=CLASSIC,allowLeadingWildcard=true)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");
    Assert.assertEquals(result.getValue().get("allowLeadingWildcard"), "true");

    // Test multiple __PINOT_OPTIONS in query
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "term1 __PINOT_OPTIONS(parser=CLASSIC) AND term2 __PINOT_OPTIONS(phraseSlop=2)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");
    Assert.assertEquals(result.getValue().get("phraseSlop"), "2");

    // Test overriding options
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "term1 __PINOT_OPTIONS(parser=CLASSIC) AND term2 __PINOT_OPTIONS(parser=STANDARD)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "STANDARD");

    // Test multiple options with overrides
    result = LuceneTextIndexUtils.parseOptionsFromSearchString(
        "term1 __PINOT_OPTIONS(parser=CLASSIC,allowLeadingWildcard=true) AND "
            + "term2 __PINOT_OPTIONS(parser=STANDARD,fuzzyMinSim=0.5)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "STANDARD");
    Assert.assertEquals(result.getValue().get("allowLeadingWildcard"), "true");
    Assert.assertEquals(result.getValue().get("fuzzyMinSim"), "0.5");

    // Test redundant operators
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 AND AND term2 __PINOT_OPTIONS(parser=CLASSIC)");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getKey(), "term1 AND term2");
    Assert.assertEquals(result.getValue().get("parser"), "CLASSIC");

    // Test no options
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 AND term2");
    Assert.assertNull(result);

    // Test escaped options
    result = LuceneTextIndexUtils.parseOptionsFromSearchString("term1 AND term2 \\__PINOT_OPTIONS(parser=CLASSIC)");
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
        "*ava realtime streaming system* OR *chine learner* __PINOT_OPTIONS(parser=CLASSIC, allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result1 = LuceneTextIndexUtils.parseOptionsFromSearchString(query1);
    Assert.assertNotNull(result1);
    Assert.assertEquals("*ava realtime streaming system* OR *chine learner*", result1.getKey());
    Assert.assertEquals(2, result1.getValue().size());
    Assert.assertEquals("CLASSIC", result1.getValue().get("parser"));
    Assert.assertEquals("true", result1.getValue().get("allowLeadingWildcard"));

    // Test with AND operator
    String query2 =
        "*ava realtime streaming system* AND *chine learn* __PINOT_OPTIONS(parser=CLASSIC, allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result2 = LuceneTextIndexUtils.parseOptionsFromSearchString(query2);
    Assert.assertNotNull(result2);
    Assert.assertEquals("*ava realtime streaming system* AND *chine learn*", result2.getKey());
    Assert.assertEquals(2, result2.getValue().size());
    Assert.assertEquals("CLASSIC", result2.getValue().get("parser"));
    Assert.assertEquals("true", result2.getValue().get("allowLeadingWildcard"));

    // Test with multiple operators
    String query3 = "*ava* AND *stream* OR *learn* __PINOT_OPTIONS(parser=CLASSIC, allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result3 = LuceneTextIndexUtils.parseOptionsFromSearchString(query3);
    Assert.assertNotNull(result3);
    Assert.assertEquals("*ava* AND *stream* OR *learn*", result3.getKey());
    Assert.assertEquals(2, result3.getValue().size());
    Assert.assertEquals("CLASSIC", result3.getValue().get("parser"));
    Assert.assertEquals("true", result3.getValue().get("allowLeadingWildcard"));

    // Test with NOT operator
    String query4 = "NOT *ava realtime streaming system* __PINOT_OPTIONS(parser=CLASSIC, allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result4 = LuceneTextIndexUtils.parseOptionsFromSearchString(query4);
    Assert.assertNotNull(result4);
    Assert.assertEquals("NOT *ava realtime streaming system*", result4.getKey());
    Assert.assertEquals(2, result4.getValue().size());
    Assert.assertEquals("CLASSIC", result4.getValue().get("parser"));
    Assert.assertEquals("true", result4.getValue().get("allowLeadingWildcard"));

    // Test with complex query and multiple OPTIONS
    String query5 =
        "*ava* AND *stream* OR *learn* __PINOT_OPTIONS(parser=CLASSIC) __PINOT_OPTIONS(allowLeadingWildcard=true)";
    Map.Entry<String, Map<String, String>> result5 = LuceneTextIndexUtils.parseOptionsFromSearchString(query5);
    Assert.assertNotNull(result5);
    Assert.assertEquals("*ava* AND *stream* OR *learn*", result5.getKey());
    Assert.assertEquals(2, result5.getValue().size());
    Assert.assertEquals("CLASSIC", result5.getValue().get("parser"));
    Assert.assertEquals("true", result5.getValue().get("allowLeadingWildcard"));
  }
}
