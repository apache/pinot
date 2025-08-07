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

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.pinot.segment.local.segment.index.text.lucene.parsers.PrefixPhraseQueryParser;
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
  public void testParseOptionsString() {
    // Test null options
    LuceneTextIndexUtils.LuceneTextIndexOptions options = LuceneTextIndexUtils.createOptions(null);
    Map<String, String> result = options.getOptions();
    Assert.assertTrue(result.isEmpty());

    // Test empty options
    options = LuceneTextIndexUtils.createOptions("");
    result = options.getOptions();
    Assert.assertTrue(result.isEmpty());

    // Test whitespace-only options
    options = LuceneTextIndexUtils.createOptions("   ");
    result = options.getOptions();
    Assert.assertTrue(result.isEmpty());

    // Test single option
    options = LuceneTextIndexUtils.createOptions("parser=CLASSIC");
    result = options.getOptions();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("CLASSIC", result.get("parser"));

    // Test multiple options
    options = LuceneTextIndexUtils.createOptions("parser=CLASSIC,allowLeadingWildcard=true,defaultOperator=AND");
    result = options.getOptions();
    Assert.assertEquals(3, result.size());
    Assert.assertEquals("CLASSIC", result.get("parser"));
    Assert.assertEquals("true", result.get("allowLeadingWildcard"));
    Assert.assertEquals("AND", result.get("defaultOperator"));

    // Test options with spaces
    options = LuceneTextIndexUtils.createOptions("parser=CLASSIC, allowLeadingWildcard=true , defaultOperator=AND");
    result = options.getOptions();
    Assert.assertEquals(3, result.size());
    Assert.assertEquals("CLASSIC", result.get("parser"));
    Assert.assertEquals("true", result.get("allowLeadingWildcard"));
    Assert.assertEquals("AND", result.get("defaultOperator"));

    // Test invalid option format (should be ignored)
    options = LuceneTextIndexUtils.createOptions("parser=CLASSIC,invalidOption,defaultOperator=AND");
    result = options.getOptions();
    Assert.assertEquals(2, result.size());
    Assert.assertEquals("CLASSIC", result.get("parser"));
    Assert.assertEquals("AND", result.get("defaultOperator"));

    // Test empty key or value (should be ignored)
    options = LuceneTextIndexUtils.createOptions("=CLASSIC,parser=,defaultOperator=AND");
    result = options.getOptions();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("AND", result.get("defaultOperator"));
  }

  @Test
  public void testLuceneTextIndexOptionsWrapper() {
    // Test creating options from string
    String optionsString = "parser=CLASSIC,defaultOperator=AND,allowLeadingWildcard=true";
    LuceneTextIndexUtils.LuceneTextIndexOptions options = LuceneTextIndexUtils.createOptions(optionsString);

    // Test getter methods
    Assert.assertEquals(options.getParser(), "CLASSIC");
    Assert.assertEquals(options.getDefaultOperator(), "AND");
    Assert.assertTrue(options.isAllowLeadingWildcard());
    Assert.assertTrue(options.isEnablePositionIncrements()); // default value
    Assert.assertFalse(options.isAutoGeneratePhraseQueries()); // default value

    // Test with empty options
    LuceneTextIndexUtils.LuceneTextIndexOptions emptyOptions = LuceneTextIndexUtils.createOptions(null);
    Assert.assertEquals(emptyOptions.getParser(), "CLASSIC"); // default value
    Assert.assertEquals(emptyOptions.getDefaultOperator(), "OR"); // default value

    // Test with empty string
    LuceneTextIndexUtils.LuceneTextIndexOptions emptyStringOptions = LuceneTextIndexUtils.createOptions("");
    Assert.assertEquals(emptyStringOptions.getParser(), "CLASSIC"); // default value
    Assert.assertEquals(emptyStringOptions.getDefaultOperator(), "OR"); // default value
  }

  @Test
  public void testLuceneTextIndexOptionsAllGetters() {
    // Test all getter methods with various options
    String optionsString = "parser=STANDARD,defaultOperator=AND,allowLeadingWildcard=true,"
        + "enablePositionIncrements=false,autoGeneratePhraseQueries=true,splitOnWhitespace=false,"
        + "lowercaseExpandedTerms=false,analyzeWildcard=true,fuzzyPrefixLength=3,fuzzyMinSim=0.8,"
        + "locale=fr,timeZone=EST,phraseSlop=2,maxDeterminizedStates=5000";

    LuceneTextIndexUtils.LuceneTextIndexOptions options = LuceneTextIndexUtils.createOptions(optionsString);

    // Test all getter methods
    Assert.assertEquals(options.getParser(), "STANDARD");
    Assert.assertEquals(options.getDefaultOperator(), "AND");
    Assert.assertTrue(options.isAllowLeadingWildcard());
    Assert.assertFalse(options.isEnablePositionIncrements());
    Assert.assertTrue(options.isAutoGeneratePhraseQueries());
    Assert.assertFalse(options.isSplitOnWhitespace());
    Assert.assertFalse(options.isLowercaseExpandedTerms());
    Assert.assertTrue(options.isAnalyzeWildcard());
    Assert.assertEquals(options.getFuzzyPrefixLength(), 3);
    Assert.assertEquals(options.getFuzzyMinSim(), 0.8f, 0.001f);
    Assert.assertEquals(options.getLocale(), "fr");
    Assert.assertEquals(options.getTimeZone(), "EST");
    Assert.assertEquals(options.getPhraseSlop(), 2);
    Assert.assertEquals(options.getMaxDeterminizedStates(), 5000);
  }

  @Test
  public void testMatchPhraseQueryParser()
      throws Exception {
    // Test the new MATCHPHRASE parser functionality
    String optionsString = "parser=MATCHPHRASE,enablePrefixMatch=true";
    LuceneTextIndexUtils.LuceneTextIndexOptions options =
        new LuceneTextIndexUtils.LuceneTextIndexOptions(optionsString);

    // Create a simple analyzer for testing
    Analyzer analyzer = new WhitespaceAnalyzer();
    String column = "testColumn";

    // Test positive case: "java realtime streaming"
    String query = "java realtime streaming";

    Query result = LuceneTextIndexUtils.createQueryParserWithOptions(query, options, column, analyzer);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof SpanNearQuery);

    // Test positive case: "realtime stream*"
    query = "realtime stream*";
    result = LuceneTextIndexUtils.createQueryParserWithOptions(query, options, column, analyzer);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof SpanNearQuery);

    // Test positive case: "stream*" - single term should return SpanMultiTermQueryWrapper
    query = "stream*";
    result = LuceneTextIndexUtils.createQueryParserWithOptions(query, options, column, analyzer);
    Assert.assertNotNull(result);
    Assert.assertTrue(result instanceof SpanMultiTermQueryWrapper);

    // Test edge case: empty string ""
    query = "";
    try {
      LuceneTextIndexUtils.createQueryParserWithOptions(query, options, column, analyzer);
      Assert.fail("Expected exception for empty query");
    } catch (RuntimeException e) {
      // The method wraps ParseException in RuntimeException via reflection
      Assert.assertTrue(e.getCause() instanceof InvocationTargetException);
    }

    // Test edge case: null query
    try {
      LuceneTextIndexUtils.createQueryParserWithOptions(null, options, column, analyzer);
      Assert.fail("Expected exception for null query");
    } catch (RuntimeException e) {
      // The method wraps ParseException in RuntimeException via reflection
      Assert.assertTrue(e.getCause() instanceof InvocationTargetException);
    }

    // Test that TopLevelQuery throws UnsupportedOperationException
    try {
      PrefixPhraseQueryParser parser = new PrefixPhraseQueryParser(column, analyzer);
      parser.TopLevelQuery(column);
      Assert.fail("Expected UnsupportedOperationException for TopLevelQuery");
    } catch (UnsupportedOperationException e) {
      Assert.assertTrue(e.getMessage().contains("TopLevelQuery is not supported"));
    }

    // Test slop and inOrder settings
    PrefixPhraseQueryParser slopParser = new PrefixPhraseQueryParser(column, analyzer);

    // Test default slop and inOrder (0 slop, true inOrder)
    Query defaultSlopQuery = slopParser.parse("java realtime streaming");
    Assert.assertTrue(defaultSlopQuery instanceof SpanNearQuery);

    // Test custom slop and inOrder
    slopParser.setSlop(2);
    slopParser.setInOrder(false);
    Query customSlopQuery = slopParser.parse("java realtime streaming");
    Assert.assertTrue(customSlopQuery instanceof SpanNearQuery);

    // Test invalid slop (should throw exception)
    try {
      slopParser.setSlop(-1);
      Assert.fail("Expected IllegalArgumentException for negative slop");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("Slop cannot be negative"));
    }

    // Test slop and inOrder with createQueryParserWithOptions
    LuceneTextIndexUtils.LuceneTextIndexOptions slopOptions =
        LuceneTextIndexUtils.createOptions("parser=MATCHPHRASE,enablePrefixMatch=true");

    // Test default slop and inOrder behavior
    Query defaultSlopResult = LuceneTextIndexUtils.createQueryParserWithOptions(
        "java realtime streaming", slopOptions, column, analyzer);
    Assert.assertTrue(defaultSlopResult instanceof SpanNearQuery);

    // Test custom slop and inOrder settings
    LuceneTextIndexUtils.LuceneTextIndexOptions customSlopOptions =
        LuceneTextIndexUtils.createOptions("parser=MATCHPHRASE,enablePrefixMatch=true");

    // Create a parser instance to test slop and inOrder settings
    PrefixPhraseQueryParser customParser = new PrefixPhraseQueryParser(column, analyzer);
    customParser.setEnablePrefixMatch(true);
    customParser.setSlop(2);
    customParser.setInOrder(false);

    // Test that custom settings work correctly
    Query customSlopResult = customParser.parse("java realtime streaming");
    Assert.assertTrue(customSlopResult instanceof SpanNearQuery);

    // Test that the parser can be configured with different slop values
    customParser.setSlop(1);
    Query slop1Result = customParser.parse("java realtime streaming");
    Assert.assertTrue(slop1Result instanceof SpanNearQuery);

    // Test that the parser can be configured with different inOrder values
    customParser.setInOrder(true);
    Query inOrderTrueResult = customParser.parse("java realtime streaming");
    Assert.assertTrue(inOrderTrueResult instanceof SpanNearQuery);

    // Test default behavior using createOptions
    LuceneTextIndexUtils.LuceneTextIndexOptions defaultOptions =
        LuceneTextIndexUtils.createOptions("parser=MATCHPHRASE");

    // Test single term with default behavior (prefix match disabled)
    Query defaultSingleTermQuery =
        LuceneTextIndexUtils.createQueryParserWithOptions("stream", defaultOptions, column, analyzer);
    Assert.assertTrue(defaultSingleTermQuery instanceof SpanTermQuery);

    // Test multiple terms with default behavior (prefix match disabled)
    Query defaultMultiTermQuery =
        LuceneTextIndexUtils.createQueryParserWithOptions("java realtime streaming", defaultOptions, column, analyzer);
    Assert.assertTrue(defaultMultiTermQuery instanceof SpanNearQuery);
  }
}
