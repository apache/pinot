/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.integration.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public abstract class BaseClusterIntegrationTestWithQueryGenerator extends BaseClusterIntegrationTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseClusterIntegrationTestWithQueryGenerator.class);

  /**
   * NOTE:
   * <p>
   * For queries with <code>LIMIT</code> or <code>TOP</code>, need to remove limit or add <code>LIMIT 10000</code> to
   * the H2 SQL query because the comparison only works on exhausted result with at most 10000 rows.
   * <ul>
   *   <li>Eg. <code>SELECT a FROM table LIMIT 15 -> [SELECT a FROM table LIMIT 10000]</code></li>
   * </ul>
   * <p>
   * For queries with multiple aggregation functions, need to split each of them into a separate H2 SQL query.
   * <ul>
   *   <li>Eg. <code>SELECT SUM(a), MAX(b) FROM table -> [SELECT SUM(a) FROM table, SELECT MAX(b) FROM table]</code></li>
   * </ul>
   * <p>
   * For group-by queries, need to add group-by columns to the select clause for H2 SQL query.
   * <ul>
   *   <li>Eg. <code>SELECT SUM(a) FROM table GROUP BY b -> [SELECT b, SUM(a) FROM table GROUP BY b]</code></li>
   * </ul>
   *
   * @throws Exception
   */
  @Test(enabled = false)
  public void testHardcodedQueries()
      throws Exception {
    // Here are some sample queries.
    String query;
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch >= 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch < 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    runQuery(query, Collections.singletonList(query));
    query = "SELECT MAX(ArrTime), MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 16312";
    runQuery(query, Arrays.asList("SELECT MAX(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 15312",
        "SELECT MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 15312"));
  }

  @Test
  public void testHardcodedQuerySet()
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTest.class.getClassLoader()
        .getResource("On_Time_On_Time_Performance_2014_100k_subset.test_queries_10K");
    Assert.assertNotNull(resourceUrl);
    File queriesFile = new File(resourceUrl.getFile());
    Random random = new Random();

    try (BufferedReader reader = new BufferedReader(new FileReader(queriesFile))) {
      while (true) {
        // Skip up to MAX_NUM_QUERIES_SKIPPED queries.
        int numQueriesSkipped = random.nextInt(MAX_NUM_QUERIES_SKIPPED);
        for (int i = 0; i < numQueriesSkipped; i++) {
          reader.readLine();
        }

        String queryString = reader.readLine();
        // Reach end of file.
        if (queryString == null) {
          return;
        }

        JSONObject query = new JSONObject(queryString);
        String pqlQuery = query.getString("pql");
        JSONArray hsqls = query.getJSONArray("hsqls");
        List<String> sqlQueries = new ArrayList<>();
        int length = hsqls.length();
        for (int i = 0; i < length; i++) {
          sqlQueries.add(hsqls.getString(i));
        }
        runQuery(pqlQuery, sqlQueries);
      }
    }
  }

  // This is disabled because testGeneratedQueriesWithMultiValues covers the same thing.
  @Test(enabled = false)
  public void testGeneratedQueriesWithoutMultiValues()
      throws Exception {
    testGeneratedQueries(false);
  }

  @Test
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    testGeneratedQueries(true);
  }

  protected void testGeneratedQueries(boolean withMultiValues)
      throws Exception {
    _queryGenerator.setSkipMultiValuePredicates(!withMultiValues);
    int generatedQueryCount = getGeneratedQueryCount();
    for (int i = 0; i < generatedQueryCount; i++) {
      QueryGenerator.Query query = _queryGenerator.generateQuery();
      String pqlQuery = query.generatePql();
      LOGGER.debug("Running query: {}", pqlQuery);
      runQuery(pqlQuery, query.generateH2Sql());
    }
  }

}
