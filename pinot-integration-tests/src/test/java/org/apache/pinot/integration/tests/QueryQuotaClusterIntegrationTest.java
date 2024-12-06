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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Properties;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManagerTest;
import org.apache.pinot.client.BrokerResponse;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.PinotClientTransport;
import org.apache.pinot.client.ResultSetGroup;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.client.Connection.FAIL_ON_EXCEPTIONS;
import static org.testng.Assert.assertTrue;


/**
 * This test suite is focused only on validating that the config changes are propagated properly as expected.
 * Validations around different cases arising from cluster config, database config and table config are extensively
 * tested as part of {@link HelixExternalViewBasedQueryQuotaManagerTest}
 */
public class QueryQuotaClusterIntegrationTest extends BaseClusterIntegrationTest {
  private PinotClientTransport _pinotClientTransport;
  private String _brokerHostPort;

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(1);
    startServers(1);
    _brokerHostPort = LOCAL_HOST + ":" + _brokerPorts.get(0);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    Properties properties = new Properties();
    properties.put(FAIL_ON_EXCEPTIONS, "FALSE");
    _pinotClientTransport = new JsonAsyncHttpPinotClientTransportFactory()
        .withConnectionProperties(getPinotConnectionProperties())
        .buildTransport();
    _pinotConnection = ConnectionFactory.fromZookeeper(properties, getZkUrl() + "/" + getHelixClusterName(),
        _pinotClientTransport);

    // create default application rate limiter manually, otherwise verifyQuotaUpdate will fail
    setQueryQuotaForApplication(null);
  }

  @AfterMethod
  void resetQuotas()
      throws Exception {
    addQueryQuotaToClusterConfig(null);
    addAppQueryQuotaToClusterConfig(null);
    setQueryQuotaForApplication(null);
    addQueryQuotaToDatabaseConfig(null);
    addQueryQuotaToTableConfig(null);

    _brokerHostPort = LOCAL_HOST + ":" + _brokerPorts.get(0);
    verifyQuotaUpdate(0);
  }

  @Test
  public void testDefaultDatabaseQueryQuota()
      throws Exception {
    addQueryQuotaToClusterConfig(40);
    testQueryRate(40);
  }

  @Test
  public void testDefaultApplicationQueryQuota()
      throws Exception {
    addAppQueryQuotaToClusterConfig(50);
    testQueryRate(50);
  }

  @Test
  public void testDatabaseConfigQueryQuota()
      throws Exception {
    addQueryQuotaToDatabaseConfig(10);
    testQueryRate(10);
  }

  @Test
  public void testApplicationQueryQuota()
      throws Exception {
    setQueryQuotaForApplication(15);
    testQueryRate(15);
  }

  @Test
  public void testDefaultDatabaseQueryQuotaOverride()
      throws Exception {
    addQueryQuotaToClusterConfig(25);
    // override lower than default quota
    addQueryQuotaToDatabaseConfig(10);
    testQueryRate(10);
    // override higher than default quota
    addQueryQuotaToDatabaseConfig(40);
    testQueryRate(40);
  }

  @Test
  public void testDefaultApplicationQueryQuotaOverride()
      throws Exception {
    addAppQueryQuotaToClusterConfig(25);
    // override lower than default quota
    setQueryQuotaForApplication(10);
    testQueryRate(10);
    // override higher than default quota
    setQueryQuotaForApplication(40);
    testQueryRate(40);
  }

  @Test
  public void testDatabaseQueryQuotaWithTableQueryQuota()
      throws Exception {
    addQueryQuotaToDatabaseConfig(25);
    // table quota within database quota. Queries should fail upon table quota (10 qps) breach
    addQueryQuotaToTableConfig(10);
    testQueryRate(10);
    // table quota more than database quota. Queries should fail upon database quota (25 qps) breach
    addQueryQuotaToTableConfig(50);
    testQueryRate(25);
  }

  @Test
  public void testApplicationQueryQuotaWithTableQueryQuota()
      throws Exception {
    setQueryQuotaForApplication(25);
    // table quota within database quota. Queries should fail upon table quota (10 qps) breach
    addQueryQuotaToTableConfig(10);
    testQueryRate(10);
    // table quota more than database quota. Queries should fail upon database quota (25 qps) breach
    addQueryQuotaToTableConfig(50);
    testQueryRate(25);
  }

  @Test
  public void testDatabaseQueryQuotaWithTableQueryQuotaWithExtraBroker()
      throws Exception {
    BaseBrokerStarter brokerStarter = null;
    try {
      addQueryQuotaToDatabaseConfig(25);
      addQueryQuotaToTableConfig(10);
      // Add one more broker such that quota gets distributed equally among them
      brokerStarter = startOneBroker(2);
      _brokerHostPort = LOCAL_HOST + ":" + brokerStarter.getPort();
      // query only one broker across the divided quota
      testQueryRateOnBroker(5);
      // drop table level quota so that database quota comes into effect
      addQueryQuotaToTableConfig(null);
      // query only one broker across the divided quota
      testQueryRateOnBroker(12.5f);
    } finally {
      if (brokerStarter != null) {
        brokerStarter.stop();
      }
    }
  }

  @Test
  public void testApplicationAndDatabaseQueryQuotaWithTableQueryQuotaWithExtraBroker()
      throws Exception {
    BaseBrokerStarter brokerStarter = null;
    try {
      addAppQueryQuotaToClusterConfig(null);
      addQueryQuotaToClusterConfig(null);
      setQueryQuotaForApplication(50);
      addQueryQuotaToDatabaseConfig(25);
      addQueryQuotaToTableConfig(10);
      //
      // Add one more broker such that quota gets distributed equally among them
      brokerStarter = startOneBroker(2);
      _brokerHostPort = LOCAL_HOST + ":" + brokerStarter.getPort();
      // query only one broker across the divided quota
      testQueryRateOnBroker(5);

      // drop table level quota so that database quota comes into effect
      addQueryQuotaToTableConfig(null);
      // query only one broker across the divided quota
      testQueryRateOnBroker(12.5f);

      // drop database level quota so that app quota comes into effect
      addQueryQuotaToDatabaseConfig(null);
      // query only one broker across the divided quota
      testQueryRateOnBroker(25f);
    } finally {
      if (brokerStarter != null) {
        brokerStarter.stop();
      }
    }
  }

  /**
   * Runs the query load with the max rate that the quota can allow and ensures queries are not failing.
   * Then runs the query load with double the max rate and expects queries to fail due to quota breach.
   * @param maxRate max rate allowed by the quota
   */
  void testQueryRate(int maxRate) {
    verifyQuotaUpdate(maxRate);
    runQueries(maxRate, false);
    //increase the qps and some of the queries should be throttled.
    runQueries(maxRate * 2, true);
  }

  void testQueryRateOnBroker(float maxRate) {
    verifyQuotaUpdate(maxRate);
    runQueriesOnBroker(maxRate, false);
    //increase the qps and some of the queries should be throttled.
    runQueriesOnBroker(maxRate * 2, true);
  }

  // adjust sleep time depending on deadline and number of iterations left
  private static void sleep(long deadline, double iterationsLeft) {
    long time = System.currentTimeMillis();
    long sleepDeadline = time + (long) Math.max(Math.ceil((deadline - time) / iterationsLeft), 0);

    while (time < sleepDeadline) {
      try {
        Thread.sleep(sleepDeadline - time);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
      time = System.currentTimeMillis();
    }
  }

  // try to keep the qps below 50 to ensure that the time lost between 2 query runs on top of the sleepMillis
  // is not comparable to sleepMillis, else the actual qps would end up being much lower than required qps
  private void runQueries(int qps, boolean shouldFail) {
    int failCount = 0;
    long deadline = System.currentTimeMillis() + 1000;

    for (int i = 0; i < qps; i++) {
      sleep(deadline, qps - i);
      ResultSetGroup resultSetGroup =
          _pinotConnection.execute("SET applicationName='default'; SELECT COUNT(*) FROM " + getTableName());
      for (PinotClientException exception : resultSetGroup.getExceptions()) {
        if (exception.getMessage().contains("QuotaExceededError")) {
          failCount++;
          break;
        }
      }
    }

    if (shouldFail) {
      assertTrue(failCount != 0, "Expected nonzero failures for qps: " + qps);
    } else {
      Assert.assertEquals(failCount, 0, "Expected zero failures for qps: " + qps);
    }
  }

  private static volatile float _quota;
  private static volatile String _quotaSource;

  private void verifyQuotaUpdate(float quotaQps) {
    try {
      TestUtils.waitForCondition(aVoid -> {
        try {
          float tableQuota = Float.parseFloat(sendGetRequest(
              String.format("http://%s/debug/tables/queryQuota/%s_OFFLINE", _brokerHostPort, getTableName())));
          tableQuota = tableQuota == 0 ? Long.MAX_VALUE : tableQuota;
          float dbQuota = Float.parseFloat(
              sendGetRequest(String.format("http://%s/debug/databases/queryQuota/default", _brokerHostPort)));
          float appQuota = Float.parseFloat(
              sendGetRequest(String.format("http://%s/debug/applicationQuotas/default", _brokerHostPort)));
          dbQuota = dbQuota == 0 ? Long.MAX_VALUE : dbQuota;
          appQuota = appQuota == 0 ? Long.MAX_VALUE : appQuota;
          float actualQuota = Math.min(Math.min(tableQuota, dbQuota), appQuota);
          _quota = actualQuota;
          if (_quota == dbQuota) {
            _quotaSource = "database";
          } else if (_quota == tableQuota) {
            _quotaSource = "table";
          } else {
            _quotaSource = "application";
          }
          return quotaQps == actualQuota || (quotaQps == 0 && tableQuota == Long.MAX_VALUE && dbQuota == Long.MAX_VALUE
              && appQuota == Long.MAX_VALUE);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }, 5000, "Failed to reflect query quota on rate limiter in 5s.");
    } catch (AssertionError ae) {
      throw new AssertionError(
          ae.getMessage() + " Expected quota:" + quotaQps + " but is: " + _quota + " set on: " + _quotaSource, ae);
    }
  }

  private BrokerResponse executeQueryOnBroker(String query) {
    return _pinotClientTransport.executeQuery(_brokerHostPort, query);
  }

  private void runQueriesOnBroker(float qps, boolean shouldFail) {
    int failCount = 0;
    long deadline = System.currentTimeMillis() + 1000;

    for (int i = 0; i < qps; i++) {
      sleep(deadline, qps - i);
      BrokerResponse resultSetGroup =
          executeQueryOnBroker("SET applicationName='default'; SELECT COUNT(*) FROM " + getTableName());
      for (Iterator<JsonNode> it = resultSetGroup.getExceptions().elements(); it.hasNext(); ) {
        JsonNode exception = it.next();
        if (exception.toPrettyString().contains("QuotaExceededError")) {
          failCount++;
          break;
        }
      }
    }

    if (shouldFail) {
      assertTrue(failCount != 0, "Expected nonzero failures for qps: " + qps);
    } else {
      Assert.assertEquals(failCount, 0, "Expected 0 failures for qps: " + qps);
    }
  }

  public void addQueryQuotaToTableConfig(Integer maxQps)
      throws Exception {
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setQuotaConfig(new QuotaConfig(null, maxQps == null ? null : maxQps.toString()));
    updateTableConfig(tableConfig);
    // to allow change propagation to QueryQuotaManager
  }

  public void addQueryQuotaToDatabaseConfig(Integer maxQps)
      throws Exception {
    String url = _controllerRequestURLBuilder.getBaseUrl() + "/databases/default/quotas";
    if (maxQps != null) {
        url += "?maxQueriesPerSecond=" + maxQps;
    }
    HttpClient.wrapAndThrowHttpException(_httpClient.sendPostRequest(new URI(url), null, null));
    // to allow change propagation to QueryQuotaManager
  }

  public void setQueryQuotaForApplication(Integer maxQps)
      throws Exception {
    String url = _controllerRequestURLBuilder.getBaseUrl() + "/applicationQuotas/default";
    if (maxQps != null) {
      url += "?maxQueriesPerSecond=" + maxQps;
    }
    HttpClient.wrapAndThrowHttpException(_httpClient.sendPostRequest(new URI(url), null, null));
    // to allow change propagation to QueryQuotaManager
  }

  public void addQueryQuotaToClusterConfig(Integer maxQps)
      throws Exception {
    if (maxQps == null) {
      HttpClient.wrapAndThrowHttpException(_httpClient.sendDeleteRequest(new URI(
        _controllerRequestURLBuilder.forClusterConfigs() + "/"
            + CommonConstants.Helix.DATABASE_MAX_QUERIES_PER_SECOND)));
    } else {
      String payload = "{\"" + CommonConstants.Helix.DATABASE_MAX_QUERIES_PER_SECOND + "\":\"" + maxQps + "\"}";
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPostRequest(new URI(_controllerRequestURLBuilder.forClusterConfigs()), payload));
    }
    // to allow change propagation to QueryQuotaManager
  }

  public void addAppQueryQuotaToClusterConfig(Integer maxQps)
      throws Exception {
    if (maxQps == null) {
      HttpClient.wrapAndThrowHttpException(_httpClient.sendDeleteRequest(new URI(
          _controllerRequestURLBuilder.forClusterConfigs() + "/"
              + CommonConstants.Helix.APPLICATION_MAX_QUERIES_PER_SECOND)));
    } else {
      String payload = "{\"" + CommonConstants.Helix.APPLICATION_MAX_QUERIES_PER_SECOND + "\":\"" + maxQps + "\"}";
      HttpClient.wrapAndThrowHttpException(
          _httpClient.sendJsonPostRequest(new URI(_controllerRequestURLBuilder.forClusterConfigs()), payload));
    }
    // to allow change propagation to QueryQuotaManager
  }
}
