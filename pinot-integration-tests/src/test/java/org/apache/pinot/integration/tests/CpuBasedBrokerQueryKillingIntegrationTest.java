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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.core.accounting.ResourceUsageAccountantFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class CpuBasedBrokerQueryKillingIntegrationTest extends BaseQueryKillingIntegrationTest {

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    super.overrideBrokerConf(brokerConf);

    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_FACTORY_NAME, ResourceUsageAccountantFactory.class.getName());
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, true);
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED, true);
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_THRESHOLD_MS, 500);
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 1.1f);
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_PANIC_LEVEL_HEAP_USAGE_RATIO, 1.1f);
  }

  @Test
  public void testCpuTimeKill()
      throws Exception {
    // Trigger broker side CPU kill with a large streaming query
    // Do not run other queries because they can cause OOM on server first
    setUseMultiStageQueryEngine(true);
    verifyCpuTimeKill(LARGE_SELECT_STAR_QUERY, postQuery(LARGE_SELECT_STAR_QUERY));
  }

  @Test
  public void testCpuTimeKillMultipleQueries()
      throws Exception {
    // Trigger broker side CPU kill with a large streaming query
    // Do not run other queries because they can cause OOM on server first
    setUseMultiStageQueryEngine(true);
    AtomicReference<JsonNode> queryResponse1 = new AtomicReference<>();
    AtomicReference<JsonNode> queryResponse2 = new AtomicReference<>();
    AtomicReference<JsonNode> queryResponse3 = new AtomicReference<>();
    CountDownLatch countDownLatch = new CountDownLatch(3);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    executor.submit(() -> {
      queryResponse1.set(postQuery(LARGE_SELECT_STAR_QUERY));
      countDownLatch.countDown();
      return null;
    });
    executor.submit(() -> {
      queryResponse2.set(postQuery(AGGREGATE_QUERY));
      countDownLatch.countDown();
      return null;
    });
    executor.submit(() -> {
      queryResponse3.set(postQuery(SELECT_STAR_QUERY));
      countDownLatch.countDown();
      return null;
    });
    executor.shutdown();
    countDownLatch.await();

    JsonNode response = queryResponse1.get();
    verifyCpuTimeKill(LARGE_SELECT_STAR_QUERY, response);

    response = queryResponse2.get();
    JsonNode exceptionsNode = response.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + AGGREGATE_QUERY);
    assertTrue(exceptionsNode.isEmpty(),
        "Expected no exceptions for query: " + AGGREGATE_QUERY + ", but got: " + exceptionsNode);

    response = queryResponse3.get();
    exceptionsNode = response.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + SELECT_STAR_QUERY);
    assertTrue(exceptionsNode.isEmpty(),
        "Expected no exceptions for query: " + SELECT_STAR_QUERY + ", but got: " + exceptionsNode);
  }

  private void verifyCpuTimeKill(String query, JsonNode response) {
    JsonNode exceptionsNode = response.get("exceptions");
    assertNotNull(exceptionsNode, "Missing exceptions for query: " + query);
    assertEquals(exceptionsNode.size(), 1, "Expected 1 exception for query: " + query + ", but got: " + exceptionsNode);
    JsonNode exceptionNode = exceptionsNode.get(0);
    JsonNode errorCodeNode = exceptionNode.get("errorCode");
    assertNotNull(errorCodeNode, "Missing errorCode from exception: " + exceptionNode);
    int errorCode = errorCodeNode.asInt();
    assertEquals(errorCode, QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.getId(),
        "Unexpected error code: " + errorCode + " from exception: " + exceptionNode);
    JsonNode messageNode = exceptionNode.get("message");
    assertNotNull(messageNode, "Missing message from exception: " + exceptionNode);
    String message = messageNode.asText();
    assertTrue(message.contains("CPU time based killed on BROKER"),
        "Unexpected exception message: " + message + " from exception: " + exceptionNode);
  }
}
