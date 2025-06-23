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
import java.util.HashMap;
import java.util.Map;
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


/**
 * Utility class for Lucene text index operations.
 * Contains common methods for parsing options and creating query parsers.
 */
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
   * Creates a configured Lucene query with the specified options.
   * This method can be used by both single-column and multi-column text index readers.
   *
   * @param actualQuery The cleaned query string without __OPTIONS
   * @param options The parsed options map
   * @param column The column name for the search
   * @param analyzer The Lucene analyzer to use
   * @return The parsed Lucene Query object
   * @throws RuntimeException if parser creation or query parsing fails
   */
  public static Query createQueryParserWithOptions(String actualQuery, Map<String, String> options, String column,
      org.apache.lucene.analysis.Analyzer analyzer) {
    // Get parser type from options
    String parserType = options.getOrDefault("parser", "CLASSIC");
    String parserClassName;

    switch (parserType.toUpperCase()) {
      case "STANDARD":
        parserClassName = "org.apache.lucene.queryparser.flexible.standard.StandardQueryParser";
        break;
      case "COMPLEX":
        parserClassName = "org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser";
        break;
      default:
        parserClassName = "org.apache.lucene.queryparser.classic.QueryParser";
        break;
    }

    // Create parser instance and apply options
    try {
      Class<?> parserClass = Class.forName(parserClassName);
      Object parser;

      if (parserClassName.equals("org.apache.lucene.queryparser.flexible.standard.StandardQueryParser")) {
        java.lang.reflect.Constructor<?> constructor = parserClass.getConstructor();
        parser = constructor.newInstance();
        try {
          java.lang.reflect.Method setAnalyzerMethod =
              parserClass.getMethod("setAnalyzer", org.apache.lucene.analysis.Analyzer.class);
          setAnalyzerMethod.invoke(parser, analyzer);
        } catch (Exception e) {
          LOGGER.warn("Failed to set analyzer on StandardQueryParser: {}", e.getMessage());
        }
      } else {
        // CLASSIC and COMPLEX parsers use the standard (String, Analyzer) constructor
        java.lang.reflect.Constructor<?> constructor =
            parserClass.getConstructor(String.class, org.apache.lucene.analysis.Analyzer.class);
        parser = constructor.newInstance(column, analyzer);
      }

      // Dynamically apply options using reflection
      Class<?> clazz = parser.getClass();
      for (Map.Entry<String, String> entry : options.entrySet()) {
        String key = entry.getKey();
        if (key.equals("parser")) {
          continue; // Skip parser option as it's only used for initialization
        }
        String value = entry.getValue();
        String setterName = "set" + Character.toUpperCase(key.charAt(0)) + key.substring(1);

        boolean found = false;
        for (java.lang.reflect.Method method : clazz.getMethods()) {
          if (method.getName().equalsIgnoreCase(setterName) && method.getParameterCount() == 1) {
            try {
              Class<?> paramType = method.getParameterTypes()[0];
              Object paramValue;

              if (paramType == boolean.class || paramType == Boolean.class) {
                paramValue = Boolean.valueOf(value);
              } else if (paramType == int.class || paramType == Integer.class) {
                paramValue = Integer.valueOf(value);
              } else if (paramType == float.class || paramType == Float.class) {
                paramValue = Float.valueOf(value);
              } else if (paramType == double.class || paramType == Double.class) {
                paramValue = Double.valueOf(value);
              } else if (paramType.isEnum()) {
                paramValue = java.lang.Enum.valueOf((Class<java.lang.Enum>) paramType, value);
              } else {
                paramValue = value;
              }

              method.invoke(parser, paramValue);
              found = true;
              break;
            } catch (Exception e) {
              LOGGER.warn("Failed to apply option {}={}: {}", key, value, e.getMessage());
            }
          }
        }

        if (!found) {
          LOGGER.warn("Failed to apply option: {}={} (setter not found)", key, value);
        }
      }

      // Parse the query using the configured parser
      Query query;
      if (parser.getClass().getName().equals("org.apache.lucene.queryparser.flexible.standard.StandardQueryParser")) {
        // StandardQueryParser uses parse(String, String) where second parameter is default field
        java.lang.reflect.Method parseMethod = parser.getClass().getMethod("parse", String.class, String.class);
        query = (Query) parseMethod.invoke(parser, actualQuery, column);
      } else {
        // Other parsers use parse(String)
        java.lang.reflect.Method parseMethod = parser.getClass().getMethod("parse", String.class);
        query = (Query) parseMethod.invoke(parser, actualQuery);
      }

      return query;
    } catch (Exception e) {
      String msg = "Failed to create or parse query: " + e.getMessage();
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * Parses options from a separate options string parameter.
   * The options should be in the format: "key1=value1,key2=value2,key3=value3"
   *
   * @param optionsString The options string in key=value format
   * @return A Map containing the parsed options, or empty map if optionsString is null/empty
   */
  public static Map<String, String> parseOptionsString(String optionsString) {
    if (optionsString == null || optionsString.trim().isEmpty()) {
      return new HashMap<>();
    }

    Map<String, String> options = new HashMap<>();
    String[] keyValuePairs = optionsString.split(",");
    for (String keyValuePair : keyValuePairs) {
      String[] parts = keyValuePair.trim().split("=");
      if (parts.length == 2) {
        String key = parts[0].trim();
        String value = parts[1].trim();
        if (!key.isEmpty() && !value.isEmpty()) {
          options.put(key, value);
        }
      } else {
        LOGGER.warn("Invalid option format (expected key=value): {}", keyValuePair);
      }
    }
    return options;
  }
}
