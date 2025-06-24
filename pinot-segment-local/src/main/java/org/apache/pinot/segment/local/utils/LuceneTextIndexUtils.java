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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
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

  // Parser option key
  public static final String PARSER_OPTION = "parser";

  // Parser values
  public static final String PARSER_CLASSIC = "CLASSIC";
  public static final String PARSER_STANDARD = "STANDARD";
  public static final String PARSER_COMPLEX = "COMPLEX";

  // Default operator values
  public static final String DEFAULT_OPERATOR_AND = "AND";
  public static final String DEFAULT_OPERATOR_OR = "OR";

  // Boolean values
  public static final String TRUE = "true";
  public static final String FALSE = "false";

  // Option keys - Lucene QueryParser configuration options
  // These options are applied to Lucene QueryParser instances via reflection
  // Documentation:
  // https://lucene.apache.org/core/9_8_0/queryparser/org/apache/lucene/queryparser/classic/QueryParser.html
  public static final String DEFAULT_OPERATOR_OPTION = "DefaultOperator";
  public static final String ALLOW_LEADING_WILDCARD_OPTION = "AllowLeadingWildcard";
  public static final String ENABLE_POSITION_INCREMENTS_OPTION = "EnablePositionIncrements";
  public static final String AUTO_GENERATE_PHRASE_QUERIES_OPTION = "AutoGeneratePhraseQueries";
  public static final String SPLIT_ON_WHITESPACE_OPTION = "SplitOnWhitespace";
  public static final String LOWERCASE_EXPANDED_TERMS_OPTION = "LowercaseExpandedTerms";
  public static final String ANALYZE_WILDCARD_OPTION = "AnalyzeWildcard";
  public static final String FUZZY_PREFIX_LENGTH_OPTION = "FuzzyPrefixLength";
  public static final String FUZZY_MIN_SIM_OPTION = "FuzzyMinSim";
  public static final String LOCALE_OPTION = "Locale";
  public static final String TIME_ZONE_OPTION = "TimeZone";
  public static final String PHRASE_SLOP_OPTION = "PhraseSlop";
  public static final String MAX_DETERMINIZED_STATES_OPTION = "MaxDeterminizedStates";

  // Parser class names
  public static final String STANDARD_QUERY_PARSER_CLASS =
      "org.apache.lucene.queryparser.flexible.standard.StandardQueryParser";
  public static final String COMPLEX_PHRASE_QUERY_PARSER_CLASS =
      "org.apache.lucene.queryparser.complexPhrase.ComplexPhraseQueryParser";
  public static final String CLASSIC_QUERY_PARSER = "org.apache.lucene.queryparser.classic.QueryParser";

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
      Analyzer analyzer) {
    // Get parser type from options
    String parserType = options.getOrDefault(PARSER_OPTION, PARSER_CLASSIC);
    String parserClassName;

    switch (parserType.toUpperCase()) {
      case PARSER_STANDARD:
        parserClassName = STANDARD_QUERY_PARSER_CLASS;
        break;
      case PARSER_COMPLEX:
        parserClassName = COMPLEX_PHRASE_QUERY_PARSER_CLASS;
        break;
      default:
        parserClassName = CLASSIC_QUERY_PARSER;
        break;
    }

    // Create parser instance and apply options
    try {
      Class<?> parserClass = Class.forName(parserClassName);
      Object parser;

      if (parserClassName.equals(STANDARD_QUERY_PARSER_CLASS)) {
        Constructor<?> constructor = parserClass.getConstructor();
        parser = constructor.newInstance();
        try {
          Method setAnalyzerMethod = parserClass.getMethod("setAnalyzer", Analyzer.class);
          setAnalyzerMethod.invoke(parser, analyzer);
        } catch (Exception e) {
          LOGGER.warn("Failed to set analyzer on StandardQueryParser: {}", e.getMessage());
        }
      } else {
        // CLASSIC and COMPLEX parsers use the standard (String, Analyzer) constructor
        Constructor<?> constructor = parserClass.getConstructor(String.class, Analyzer.class);
        parser = constructor.newInstance(column, analyzer);
      }

      // Dynamically apply options using reflection
      Class<?> clazz = parser.getClass();
      for (Map.Entry<String, String> entry : options.entrySet()) {
        String key = entry.getKey();
        if (key.equals(PARSER_OPTION)) {
          continue; // Skip parser option as it's only used for initialization
        }
        String value = entry.getValue();
        String setterName = "set" + Character.toUpperCase(key.charAt(0)) + key.substring(1);

        boolean found = false;
        for (Method method : clazz.getMethods()) {
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
                paramValue = Enum.valueOf((Class<Enum>) paramType, value);
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
      if (parser.getClass().getName().equals(STANDARD_QUERY_PARSER_CLASS)) {
        // StandardQueryParser uses parse(String, String) where second parameter is default field
        Method parseMethod = parser.getClass().getMethod("parse", String.class, String.class);
        query = (Query) parseMethod.invoke(parser, actualQuery, column);
      } else {
        // Other parsers use parse(String)
        Method parseMethod = parser.getClass().getMethod("parse", String.class);
        query = (Query) parseMethod.invoke(parser, actualQuery);
      }

      return query;
    } catch (Exception e) {
      String msg = "Failed to create or parse query: " + e.getMessage();
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * Parse the options string into a map of key-value pairs.
   * This method is now private and should be accessed through LuceneTextIndexOptions class.
   */
  private static Map<String, String> parseOptionsString(String optionsString) {
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

  /**
   * Helper class to wrap the parsed Lucene text index options.
   * Similar to DistinctCountOffHeapAggregationFunction.Parameters pattern.
   */
  public static class LuceneTextIndexOptions {
    private final Map<String, String> _options;

    public LuceneTextIndexOptions(String optionsString) {
      _options = parseOptionsString(optionsString);
    }

    public Map<String, String> getOptions() {
      return _options;
    }

    public String getParser() {
      return _options.getOrDefault(PARSER_OPTION, PARSER_CLASSIC);
    }

    public String getDefaultOperator() {
      return _options.getOrDefault(DEFAULT_OPERATOR_OPTION, DEFAULT_OPERATOR_OR);
    }

    public boolean isAllowLeadingWildcard() {
      return Boolean.parseBoolean(_options.getOrDefault(ALLOW_LEADING_WILDCARD_OPTION, FALSE));
    }

    public boolean isEnablePositionIncrements() {
      return Boolean.parseBoolean(_options.getOrDefault(ENABLE_POSITION_INCREMENTS_OPTION, TRUE));
    }

    public boolean isAutoGeneratePhraseQueries() {
      return Boolean.parseBoolean(_options.getOrDefault(AUTO_GENERATE_PHRASE_QUERIES_OPTION, FALSE));
    }

    public boolean isSplitOnWhitespace() {
      return Boolean.parseBoolean(_options.getOrDefault(SPLIT_ON_WHITESPACE_OPTION, TRUE));
    }

    public boolean isLowercaseExpandedTerms() {
      return Boolean.parseBoolean(_options.getOrDefault(LOWERCASE_EXPANDED_TERMS_OPTION, TRUE));
    }

    public boolean isAnalyzeWildcard() {
      return Boolean.parseBoolean(_options.getOrDefault(ANALYZE_WILDCARD_OPTION, FALSE));
    }

    public int getFuzzyPrefixLength() {
      return Integer.parseInt(_options.getOrDefault(FUZZY_PREFIX_LENGTH_OPTION, "0"));
    }

    public float getFuzzyMinSim() {
      return Float.parseFloat(_options.getOrDefault(FUZZY_MIN_SIM_OPTION, "2.0"));
    }

    public String getLocale() {
      return _options.getOrDefault(LOCALE_OPTION, "en");
    }

    public String getTimeZone() {
      return _options.getOrDefault(TIME_ZONE_OPTION, "UTC");
    }

    public int getPhraseSlop() {
      return Integer.parseInt(_options.getOrDefault(PHRASE_SLOP_OPTION, "0"));
    }

    public int getMaxDeterminizedStates() {
      return Integer.parseInt(_options.getOrDefault(MAX_DETERMINIZED_STATES_OPTION, "10000"));
    }
  }

  /**
   * Creates LuceneTextIndexOptions from a string.
   * This is a convenience method for creating the options wrapper.
   *
   * @param optionsString The options string in format "key1=value1,key2=value2"
   * @return The LuceneTextIndexOptions wrapper
   */
  public static LuceneTextIndexOptions createOptions(String optionsString) {
    return new LuceneTextIndexOptions(optionsString);
  }
}
