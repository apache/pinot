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

import java.util.ArrayList;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanNotQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneTextIndexUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuceneTextIndexUtils.class);

  private LuceneTextIndexUtils() {
  }

  /**
   * Converts a BooleanQuery to appropriate SpanQuery based on its clauses.
   * For AND operations, uses SpanNearQuery with specified slop.
   * For OR operations, uses SpanOrQuery.
   * For NOT operations, uses SpanNotQuery.
   *
   * @param query a Lucene query
   * @return a span query if the input query is boolean query with one or more
   * prefix or wildcard subqueries; the same query otherwise.
   */
  public static Query convertToMultiTermSpanQuery(Query query) {
    if (!(query instanceof BooleanQuery)) {
      return query;
    }
    LOGGER.debug("Perform rewriting for the phrase query {}.", query);

    BooleanQuery booleanQuery = (BooleanQuery) query;
    ArrayList<SpanQuery> spanQueryLst = new ArrayList<>();
    boolean prefixOrSuffixQueryFound = false;
    boolean isOrQuery = false;
    boolean hasNotClause = false;
    SpanQuery notQuery = null;

    // First pass: Check query type and collect NOT clauses
    for (BooleanClause clause : booleanQuery.clauses()) {
      if (clause.getOccur() == BooleanClause.Occur.SHOULD) {
        isOrQuery = true;
      } else if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) {
        hasNotClause = true;
        Query q = clause.getQuery();
        if (q instanceof WildcardQuery || q instanceof PrefixQuery) {
          notQuery = new SpanMultiTermQueryWrapper<>((AutomatonQuery) q);
        } else if (q instanceof TermQuery) {
          notQuery = new SpanTermQuery(((TermQuery) q).getTerm());
        }
      }
    }

    // Second pass: Process non-NOT clauses
    for (BooleanClause clause : booleanQuery.clauses()) {
      if (clause.getOccur() == BooleanClause.Occur.MUST_NOT) {
        continue; // Skip NOT clauses as they're handled separately
      }
      Query q = clause.getQuery();
      if (q instanceof WildcardQuery || q instanceof PrefixQuery) {
        prefixOrSuffixQueryFound = true;
        spanQueryLst.add(new SpanMultiTermQueryWrapper<>((AutomatonQuery) q));
      } else if (q instanceof TermQuery) {
        spanQueryLst.add(new SpanTermQuery(((TermQuery) q).getTerm()));
      } else {
        LOGGER.info("query can not be handled currently {} ", q);
        return query;
      }
    }

    if (!prefixOrSuffixQueryFound) {
      return query;
    }

    // Create appropriate span query based on operation type
    SpanQuery baseQuery;
    if (isOrQuery) {
      baseQuery = new SpanOrQuery(spanQueryLst.toArray(new SpanQuery[0]));
      LOGGER.debug("The OR query {} is re-written as {}", query, baseQuery);
    } else {
      baseQuery = new SpanNearQuery(spanQueryLst.toArray(new SpanQuery[0]), 0, true);
      LOGGER.debug("The AND query {} is re-written as {}", query, baseQuery);
    }

    // Apply NOT operation if present
    if (hasNotClause && notQuery != null) {
      SpanNotQuery spanNotQuery = new SpanNotQuery(baseQuery, notQuery);
      LOGGER.debug("Applied NOT operation to query: {}", spanNotQuery);
      return spanNotQuery;
    }

    return baseQuery;
  }
}
