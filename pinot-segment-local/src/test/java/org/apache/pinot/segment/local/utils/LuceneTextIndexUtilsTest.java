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

    SpanQuery[] spanQueries3 =
        {new SpanMultiTermQueryWrapper<>(wildcardQuery), new SpanMultiTermQueryWrapper<>(prefixQuery),
            new SpanMultiTermQueryWrapper<>(prefixQuery),};
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
}
