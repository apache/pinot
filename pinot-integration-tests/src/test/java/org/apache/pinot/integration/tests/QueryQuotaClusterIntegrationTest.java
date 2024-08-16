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
  }

  @AfterMethod
  void resetQuotas()
      throws Exception {
    addQueryQuotaToClusterConfig(null);
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
  public void testDatabaseConfigQueryQuota()
      throws Exception {
    addQueryQuotaToDatabaseConfig(10);
    testQueryRate(10);
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
  public void testDatabaseQueryQuotaWithTableQueryQuotaWithExtraBroker()
      throws Exception {
    BaseBrokerStarter brokerStarter = null;
    try {
      addQueryQuotaToDatabaseConfig(25);
      addQueryQuotaToTableConfig(10);
      // Add one more broker such that quota gets distributed equally among them
      brokerStarter = startOneBroker(2);
      _brokerHostPort = LOCAL_HOST + ":" + brokerStarter.getPort();
      // to allow change propagation to QueryQuotaManager
      // query only one broker across the divided quota
      testQueryRateOnBroker(5);
      // drop table level quota so that database quota comes into effect
      addQueryQuotaToTableConfig(null);
      // query only one broker across the divided quota
      testQueryRateOnBroker(12);
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
  void testQueryRate(int maxRate)
      throws Exception {
    verifyQuotaUpdate(maxRate);
    runQueries(maxRate, false);
    //increase the qps and some of the queries should be throttled.
    runQueries(maxRate * 2, true);
  }

  void testQueryRateOnBroker(int maxRate)
      throws Exception {
    verifyQuotaUpdate(maxRate);
    runQueriesOnBroker(maxRate, false);
    //increase the qps and some of the queries should be throttled.
    runQueriesOnBroker(maxRate * 2, true);
  }

  // try to keep the qps below 50 to ensure that the time lost between 2 query runs on top of the sleepMillis
  // is not comparable to sleepMillis, else the actual qps would end up being much lower than required qps
  private void runQueries(double qps, boolean shouldFail)
      throws Exception {
    int failCount = 0;
    long sleepMillis = (long) (1000 / qps);
    Thread.sleep(sleepMillis);
    for (int i = 0; i < qps * 2; i++) {
      ResultSetGroup resultSetGroup = _pinotConnection.execute("SELECT COUNT(*) FROM " + getTableName());
      for (PinotClientException exception : resultSetGroup.getExceptions()) {
        if (exception.getMessage().contains("QuotaExceededError")) {
          failCount++;
          break;
        }
      }
      Thread.sleep(sleepMillis);
    }
    assertTrue((failCount == 0 && !shouldFail) || (failCount != 0 && shouldFail));
  }

  private void verifyQuotaUpdate(long quotaQps) {
    String query = "SELECT COUNT(*) FROM " + getTableName();
    if (quotaQps == 0) {
      TestUtils.waitForCondition(aVoid -> !executeQueryOnBroker(query).getExceptions().elements().hasNext()
          && !executeQueryOnBroker(query).getExceptions().elements().hasNext(), 1000, "Failed to unset quota");
      return;
    }
    long lockMillis = (1000 / quotaQps);
    TestUtils.waitForCondition(aVoid -> {
      long beforeLock = System.currentTimeMillis();
      while (executeQueryOnBroker(query).getExceptions().elements().hasNext()) {
        beforeLock = System.currentTimeMillis();
      }
      long afterRequest = System.currentTimeMillis();
      long beforeNextRequest = System.currentTimeMillis();
      while (executeQueryOnBroker(query).getExceptions().elements().hasNext()) {
        if (beforeNextRequest > afterRequest + (lockMillis * 1.2)) {
          return false;
        }
        beforeNextRequest = System.currentTimeMillis();
      }
      return System.currentTimeMillis() > beforeLock + (lockMillis * 0.8);
    }, lockMillis, 5000, "Failed to get lock in 5s");
  }

  private BrokerResponse executeQueryOnBroker(String query) {
    return _pinotClientTransport.executeQuery(_brokerHostPort, query);
  }

  private void runQueriesOnBroker(double qps, boolean shouldFail)
      throws Exception {
    int failCount = 0;
    long sleepMillis = (long) (1000 / qps);
    Thread.sleep(sleepMillis);
    for (int i = 0; i < qps * 2; i++) {
      BrokerResponse resultSetGroup = executeQueryOnBroker("SELECT COUNT(*) FROM " + getTableName());
      for (Iterator<JsonNode> it = resultSetGroup.getExceptions().elements(); it.hasNext(); ) {
        JsonNode exception = it.next();
        if (exception.toPrettyString().contains("QuotaExceededError")) {
          failCount++;
          break;
        }
      }
      Thread.sleep(sleepMillis);
    }
    assertTrue((failCount == 0 && !shouldFail) || (failCount != 0 && shouldFail));
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
}
