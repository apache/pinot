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
package org.apache.pinot.sql.parsers.rewriter;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class QueryRewriterFactory {

  private QueryRewriterFactory() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(QueryRewriterFactory.class);

  public static final List<String> DEFAULT_QUERY_REWRITERS_CLASS_NAMES =
      ImmutableList.of(CompileTimeFunctionsInvoker.class.getName(), SelectionsRewriter.class.getName(),
          PredicateComparisonRewriter.class.getName(), OrdinalsUpdater.class.getName(),
          AliasApplier.class.getName(), NonAggregationGroupByToDistinctQueryRewriter.class.getName());

  public static void init(String queryRewritersClassNamesStr) {
    List<String> queryRewritersClassNames =
        (queryRewritersClassNamesStr != null) ? Arrays.asList(queryRewritersClassNamesStr.split(","))
            : DEFAULT_QUERY_REWRITERS_CLASS_NAMES;
    final List<QueryRewriter> queryRewriters = getQueryRewriters(queryRewritersClassNames);
    synchronized (CalciteSqlParser.class) {
      CalciteSqlParser.QUERY_REWRITERS.clear();
      CalciteSqlParser.QUERY_REWRITERS.addAll(queryRewriters);
    }
  }

  public static List<QueryRewriter> getQueryRewriters() {
    return getQueryRewriters(DEFAULT_QUERY_REWRITERS_CLASS_NAMES);
  }

  public static List<QueryRewriter> getQueryRewriters(List<String> queryRewriterClasses) {
    final ImmutableList.Builder<QueryRewriter> builder = ImmutableList.builder();
    for (String queryRewriterClassName : queryRewriterClasses) {
      try {
        builder.add(getQueryRewriter(queryRewriterClassName));
      } catch (Exception e) {
        LOGGER.error("Failed to load QueryRewriter: {}", queryRewriterClassName, e);
      }
    }
    return builder.build();
  }

  private static QueryRewriter getQueryRewriter(String queryRewriterClassName)
      throws Exception {
    final Class<QueryRewriter> queryRewriterClass = (Class<QueryRewriter>) Class.forName(queryRewriterClassName);
    return (QueryRewriter) queryRewriterClass.getDeclaredConstructors()[0].newInstance();
  }
}
