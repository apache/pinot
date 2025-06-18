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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
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
   *
   * @param query a Lucene query
   * @return a span query with 0 slop and the original clause order if the input query is boolean query with one or more
   * prefix or wildcard subqueries; the same query otherwise.
   */
  public static Query convertToMultiTermSpanQuery(Query query) {
    if (!(query instanceof BooleanQuery)) {
      return query;
    }
    LOGGER.debug("Perform rewriting for the phrase query {}.", query);
    ArrayList<SpanQuery> spanQueryLst = new ArrayList<>();
    boolean prefixOrSuffixQueryFound = false;
    for (BooleanClause clause : ((BooleanQuery) query).clauses()) {
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
    SpanNearQuery spanNearQuery = new SpanNearQuery(spanQueryLst.toArray(new SpanQuery[0]), 0, true);
    LOGGER.debug("The phrase query {} is re-written as {}", query, spanNearQuery);
    return spanNearQuery;
  }

  /**
   * Parses search options from a search query string.
   * The options can be specified anywhere in the query using __PINOT_OPTIONS(...) syntax.
   * For example: "term1 __PINOT_OPTIONS(parser=CLASSIC) AND term2 __PINOT_OPTIONS(analyzer=STANDARD)"
   * Options from later occurrences will override earlier ones.
   *
   * @param searchQuery The search query string
   * @return A Map.Entry containing the cleaned search term and options map, or null if no options found
   */
  public static Map.Entry<String, Map<String, String>> parseOptionsFromSearchString(String searchQuery) {
    // Pattern to match __PINOT_OPTIONS(...) anywhere in the string, but not escaped
    Pattern pattern = Pattern.compile("(?<!\\\\)__PINOT_OPTIONS\\(([^)]*)\\)");
    Matcher matcher = pattern.matcher(searchQuery);
    Map<String, String> mergedOptions = new HashMap<>();
    String actualSearchTerm;
    boolean foundOptions = false;
    // Find all __PINOT_OPTIONS and merge them
    while (matcher.find()) {
      foundOptions = true;
      String optionsStr = matcher.group(1).trim();
      // Parse options
      for (String option : optionsStr.split(",")) {
        String[] parts = option.trim().split("=");
        if (parts.length == 2) {
          mergedOptions.put(parts[0].trim(), parts[1].trim());
        }
      }
    }

    // If no options found, return null
    if (!foundOptions) {
      return null;
    }
    // Remove all __PINOT_OPTIONS from the query (but not escaped ones)
    actualSearchTerm = pattern.matcher(searchQuery).replaceAll("").trim();
    // Clean up extra whitespace and fix multiple consecutive operators
    actualSearchTerm = actualSearchTerm.replaceAll("\\s+", " ").trim();
    actualSearchTerm = actualSearchTerm.replaceAll("\\b(AND|OR|NOT)\\s+\\1\\b", "$1");
    actualSearchTerm = actualSearchTerm.replaceAll("\\s+", " ").trim();
    return new AbstractMap.SimpleEntry<>(actualSearchTerm, mergedOptions);
  }
}
