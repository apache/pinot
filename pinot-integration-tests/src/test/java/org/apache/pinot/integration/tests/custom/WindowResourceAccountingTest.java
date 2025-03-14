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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.pinot.core.accounting.AggregateByQueryIdAccountantFactoryForTest;
import org.apache.pinot.spi.accounting.QueryResourceTracker;
import org.apache.pinot.spi.accounting.ThreadResourceUsageAccountant;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class WindowResourceAccountingTest extends WindowFunnelTestBase {
  @Test
  public void testFunnel()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String query = String.format(
        "SELECT " + "funnelMaxStep(timestampCol, '1000', 4, " + "url = '/product/search', " + "url = '/cart/add', "
            + "url = '/checkout/start', " + "url = '/checkout/confirmation' " + ") " + "FROM %s LIMIT %d",
        getTableName(), getCountStarResult());

    JsonNode response = postQuery(query);
    ThreadResourceUsageAccountant accountant = Tracing.getThreadAccountant();
    assertEquals(getBrokerConf(0).getProperty(
            CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME),
        AggregateByQueryIdAccountantFactoryForTest.class.getCanonicalName());
    assertEquals(getServerConf(0).getProperty(
            CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + "." + CommonConstants.Accounting.CONFIG_OF_FACTORY_NAME),
        AggregateByQueryIdAccountantFactoryForTest.class.getCanonicalName());
    assertEquals(accountant.getClass().getCanonicalName(),
        AggregateByQueryIdAccountantFactoryForTest.AggregateByQueryIdAccountant.class.getCanonicalName());
    Map<String, ? extends QueryResourceTracker> queryMemUsage = accountant.getQueryResources();
    assertFalse(queryMemUsage.isEmpty());
    boolean foundRequestId = false;
    String queryIdKey = null;
    for (String key : queryMemUsage.keySet()) {
      if (key.contains(response.get("requestId").asText())) {
        foundRequestId = true;
        queryIdKey = key;
        break;
      }
    }
    assertTrue(foundRequestId);
    assertTrue(queryMemUsage.get(queryIdKey).getAllocatedBytes() > 0);
  }
}
