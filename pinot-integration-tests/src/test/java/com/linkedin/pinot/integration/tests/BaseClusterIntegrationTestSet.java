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

import com.google.common.base.Function;
import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.util.TestUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;


/**
 * Shared set of common tests for cluster integration tests.
 * <p>To enable the test, override it and add @Test annotation.
 */
public abstract class BaseClusterIntegrationTestSet extends BaseClusterIntegrationTest {
  private static final Random RANDOM = new Random();

  // Default settings
  private static final String DEFAULT_QUERY_FILE_NAME = "On_Time_On_Time_Performance_2014_100k_subset.test_queries_10K";
  private static final int DEFAULT_NUM_QUERIES_TO_GENERATE = 100;
  private static final int DEFAULT_MAX_NUM_QUERIES_TO_SKIP_IN_QUERY_FILE = 200;

  /**
   * Can be overridden to change default setting
   */
  @Nonnull
  protected String getQueryFileName() {
    return DEFAULT_QUERY_FILE_NAME;
  }

  /**
   * Can be overridden to change default setting
   */
  protected int getNumQueriesToGenerate() {
    return DEFAULT_NUM_QUERIES_TO_GENERATE;
  }

  /**
   * Can be overridden to change default setting
   */
  protected int getMaxNumQueriesToSkipInQueryFile() {
    return DEFAULT_MAX_NUM_QUERIES_TO_SKIP_IN_QUERY_FILE;
  }

  /**
   * Test hardcoded queries.
   * <p>NOTE:
   * <p>For queries with <code>LIMIT</code> or <code>TOP</code>, need to remove limit or add <code>LIMIT 10000</code> to
   * the H2 SQL query because the comparison only works on exhausted result with at most 10000 rows.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT a FROM table LIMIT 15 -> [SELECT a FROM table LIMIT 10000]</code>
   *   </li>
   * </ul>
   * <p>For queries with multiple aggregation functions, need to split each of them into a separate H2 SQL query.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT SUM(a), MAX(b) FROM table -> [SELECT SUM(a) FROM table, SELECT MAX(b) FROM table]</code>
   *   </li>
   * </ul>
   * <p>For group-by queries, need to add group-by columns to the select clause for H2 SQL query.
   * <ul>
   *   <li>
   *     Eg. <code>SELECT SUM(a) FROM table GROUP BY b -> [SELECT b, SUM(a) FROM table GROUP BY b]</code>
   *   </li>
   * </ul>
   *
   * @throws Exception
   */
  public void testHardcodedQueries()
      throws Exception {
    // Here are some sample queries.
    String query;
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch = 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <> 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch > 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch >= 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch < 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT COUNT(*) FROM mytable WHERE DaysSinceEpoch <= 16312 AND Carrier = 'DL'";
    testQuery(query, Collections.singletonList(query));
    query = "SELECT MAX(ArrTime), MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 16312";
    testQuery(query, Arrays.asList("SELECT MAX(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 15312",
        "SELECT MIN(ArrTime) FROM mytable WHERE DaysSinceEpoch >= 15312"));
  }

  /**
   * Test random queries from the query file.
   *
   * @throws Exception
   */
  public void testQueriesFromQueryFile()
      throws Exception {
    URL resourceUrl = BaseClusterIntegrationTestSet.class.getClassLoader().getResource(getQueryFileName());
    Assert.assertNotNull(resourceUrl);
    File queriesFile = new File(resourceUrl.getFile());

    int maxNumQueriesToSkipInQueryFile = getMaxNumQueriesToSkipInQueryFile();
    try (BufferedReader reader = new BufferedReader(new FileReader(queriesFile))) {
      while (true) {
        int numQueriesSkipped = RANDOM.nextInt(maxNumQueriesToSkipInQueryFile);
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
        testQuery(pqlQuery, sqlQueries);
      }
    }
  }

  /**
   * Test queries without multi values generated by query generator.
   *
   * @throws Exception
   */
  public void testGeneratedQueriesWithoutMultiValues()
      throws Exception {
    testGeneratedQueries(false);
  }

  /**
   * Test queries with multi values generated by query generator.
   *
   * @throws Exception
   */
  public void testGeneratedQueriesWithMultiValues()
      throws Exception {
    testGeneratedQueries(true);
  }

  private void testGeneratedQueries(boolean withMultiValues)
      throws Exception {
    QueryGenerator queryGenerator = getQueryGenerator();
    queryGenerator.setSkipMultiValuePredicates(!withMultiValues);
    int numQueriesToGenerate = getNumQueriesToGenerate();
    for (int i = 0; i < numQueriesToGenerate; i++) {
      QueryGenerator.Query query = queryGenerator.generateQuery();
      testQuery(query.generatePql(), query.generateH2Sql());
    }
  }

  /**
   * Test if routing table get updated when instance is shutting down.
   *
   * @throws Exception
   */
  public void testInstanceShutdown()
      throws Exception {
    List<String> instances = _helixAdmin.getInstancesInCluster(_clusterName);
    Assert.assertFalse(instances.isEmpty(), "List of instances should not be empty");

    // Mark all instances in the cluster as shutting down
    for (String instance : instances) {
      InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(_clusterName, instance);
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
      _helixAdmin.setInstanceConfig(_clusterName, instance, instanceConfig);
    }

    // Check that the routing table is empty
    checkForEmptyRoutingTable(true);

    // Mark all instances as not shutting down
    for (String instance : instances) {
      InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(_clusterName, instance);
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false);
      _helixAdmin.setInstanceConfig(_clusterName, instance, instanceConfig);
    }

    // Check that the routing table is not empty
    checkForEmptyRoutingTable(false);

    // Check on each server instance
    for (String instance : instances) {
      if (!instance.startsWith(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)) {
        continue;
      }
      // E.g. instance: Server_1.2.3.4_1234; instanceAddress: 1.2.3.4_1234
      String instanceAddress = instance.substring(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE.length());

      // Ensure that the random instance is in the routing table
      checkForInstanceInRoutingTable(true, instanceAddress);

      // Mark the server instance as shutting down
      InstanceConfig instanceConfig = _helixAdmin.getInstanceConfig(_clusterName, instance);
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
      _helixAdmin.setInstanceConfig(_clusterName, instance, instanceConfig);

      // Check that it is not in the routing table
      checkForInstanceInRoutingTable(false, instanceAddress);

      // Re-enable the server instance
      instanceConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false);
      _helixAdmin.setInstanceConfig(_clusterName, instance, instanceConfig);

      // Check that it is in the routing table
      checkForInstanceInRoutingTable(true, instanceAddress);
    }
  }

  private void checkForInstanceInRoutingTable(final boolean shouldExist, @Nonnull final String instanceAddress)
      throws Exception {
    String errorMessage;
    if (shouldExist) {
      errorMessage = "Routing table does not contain expected instance: " + instanceAddress;
    } else {
      errorMessage = "Routing table contains unexpected instance: " + instanceAddress;
    }
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          JSONArray routingTableSnapshot =
              getDebugInfo("debug/routingTable/" + getTableName()).getJSONArray("routingTableSnapshot");
          int numTables = routingTableSnapshot.length();
          for (int i = 0; i < numTables; i++) {
            JSONObject tableRouting = routingTableSnapshot.getJSONObject(i);
            String tableNameWithType = tableRouting.getString("tableName");
            if (TableNameBuilder.extractRawTableName(tableNameWithType).equals(getTableName())) {
              JSONArray routingTableEntries = tableRouting.getJSONArray("routingTableEntries");
              int numRoutingTableEntries = routingTableEntries.length();
              for (int j = 0; j < numRoutingTableEntries; j++) {
                JSONObject routingTableEntry = routingTableEntries.getJSONObject(j);
                if (routingTableEntry.has(instanceAddress)) {
                  return shouldExist;
                }
              }
            }
          }
          return !shouldExist;
        } catch (Exception e) {
          return null;
        }
      }
    }, 60_000L, errorMessage);
  }

  private void checkForEmptyRoutingTable(final boolean shouldBeEmpty)
      throws Exception {
    String errorMessage;
    if (shouldBeEmpty) {
      errorMessage = "Routing table is not empty";
    } else {
      errorMessage = "Routing table is empty";
    }
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          JSONArray routingTableSnapshot =
              getDebugInfo("debug/routingTable/" + getTableName()).getJSONArray("routingTableSnapshot");
          int numTables = routingTableSnapshot.length();
          for (int i = 0; i < numTables; i++) {
            JSONObject tableRouting = routingTableSnapshot.getJSONObject(i);
            String tableNameWithType = tableRouting.getString("tableName");
            if (TableNameBuilder.extractRawTableName(tableNameWithType).equals(getTableName())) {
              JSONArray routingTableEntries = tableRouting.getJSONArray("routingTableEntries");
              int numRoutingTableEntries = routingTableEntries.length();
              for (int j = 0; j < numRoutingTableEntries; j++) {
                JSONObject routingTableEntry = routingTableEntries.getJSONObject(j);
                if (routingTableEntry.length() == 0) {
                  if (!shouldBeEmpty) {
                    return false;
                  }
                } else {
                  if (shouldBeEmpty) {
                    return false;
                  }
                }
              }
            }
          }
          return true;
        } catch (Exception e) {
          return null;
        }
      }
    }, 60_000L, errorMessage);
  }
}
