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


public class MemoryBasedServerQueryKillingIntegrationTest extends BaseQueryKillingIntegrationTest {

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    super.overrideServerConf(serverConf);

    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_FACTORY_NAME, ResourceUsageAccountantFactory.class.getName());
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ALARMING_LEVEL_HEAP_USAGE_RATIO, 0f);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_CRITICAL_LEVEL_HEAP_USAGE_RATIO, 0.15f);
  }

  @Test(dataProvider = "expensiveQueries")
  public void testOomKill(String query)
      throws Exception {
    verifyOomKill(query, postQuery(query));
    setUseMultiStageQueryEngine(true);
    verifyOomKill(query, postQuery(query));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testOomKillMultipleQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    AtomicReference<JsonNode> queryResponse1 = new AtomicReference<>();
    AtomicReference<JsonNode> queryResponse2 = new AtomicReference<>();
    AtomicReference<JsonNode> queryResponse3 = new AtomicReference<>();
    CountDownLatch countDownLatch = new CountDownLatch(3);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    executor.submit(() -> {
      queryResponse1.set(postQuery(LARGE_DISTINCT_QUERY));
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
    verifyOomKill(LARGE_DISTINCT_QUERY, response);

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

  private void verifyOomKill(String query, JsonNode response) {
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
    assertTrue(message.contains("OOM killed on SERVER"),
        "Unexpected exception message: " + message + " from exception: " + exceptionNode);
  }
}
