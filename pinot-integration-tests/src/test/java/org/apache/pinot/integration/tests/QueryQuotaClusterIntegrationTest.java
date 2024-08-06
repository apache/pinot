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

import java.net.URI;
import java.util.Properties;
import org.apache.pinot.broker.broker.helix.BaseBrokerStarter;
import org.apache.pinot.broker.queryquota.HelixExternalViewBasedQueryQuotaManagerTest;
import org.apache.pinot.client.ConnectionFactory;
import org.apache.pinot.client.JsonAsyncHttpPinotClientTransportFactory;
import org.apache.pinot.client.PinotClientException;
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
  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBrokers(1);
    startServers(1);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    Properties properties = new Properties();
    properties.put(FAIL_ON_EXCEPTIONS, "FALSE");
    _pinotConnection = ConnectionFactory.fromZookeeper(properties, getZkUrl() + "/" + getHelixClusterName(),
        new JsonAsyncHttpPinotClientTransportFactory().withConnectionProperties(getPinotConnectionProperties())
            .buildTransport());
  }

  @AfterMethod
  void resetQuotas()
      throws Exception {
    addQueryQuotaToClusterConfig(null);
    addQueryQuotaToDatabaseConfig(null);
    addQueryQuotaToTableConfig(null);
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
      // to allow change propagation to QueryQuotaManager
      Thread.sleep(1000);
      testQueryRate(10);
      // drop table level quota so that database quota comes into effect
      addQueryQuotaToTableConfig(null);
      testQueryRate(25);
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
    runQueries(maxRate, false);
    //increase the qps and some of the queries should be throttled.
    runQueries(maxRate * 2, true);
  }

  // try to keep the qps below 50 to ensure that the time lost between 2 query runs on top of the sleepMillis
  // is not comparable to sleepMillis, else the actual qps would end up being much lower than required qps
  private void runQueries(double qps, boolean shouldFail)
      throws Exception {
    int failCount = 0;
    long sleepMillis = (long) (1000 / qps);
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


  public void addQueryQuotaToTableConfig(Integer maxQps)
      throws Exception {
    TableConfig tableConfig = getOfflineTableConfig();
    tableConfig.setQuotaConfig(new QuotaConfig(null, maxQps == null ? null : maxQps.toString()));
    updateTableConfig(tableConfig);
    // to allow change propagation to QueryQuotaManager
    Thread.sleep(1000);
  }

  public void addQueryQuotaToDatabaseConfig(Integer maxQps)
      throws Exception {
    String url = _controllerRequestURLBuilder.getBaseUrl() + "/databases/default/quotas";
    if (maxQps != null) {
        url += "?maxQueriesPerSecond=" + maxQps;
    }
    HttpClient.wrapAndThrowHttpException(_httpClient.sendPostRequest(new URI(url), null, null));
    // to allow change propagation to QueryQuotaManager
    Thread.sleep(1000);
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
    Thread.sleep(1000);
  }
}
