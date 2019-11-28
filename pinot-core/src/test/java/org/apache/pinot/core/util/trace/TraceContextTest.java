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
package org.apache.pinot.core.util.trace;

import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TraceContextTest {
  private static final int NUM_CHILDREN_PER_REQUEST = 10;
  private static final int NUM_REQUESTS = 10;
  private static final Random RANDOM = new Random();

  @Test
  public void testSingleRequest()
      throws Exception {
    ExecutorService executorService = Executors.newCachedThreadPool();
    testSingleRequest(executorService, 0);
    executorService.shutdown();
    Assert.assertTrue(TraceContext.REQUEST_TO_TRACES_MAP.isEmpty());
  }

  @Test
  public void testMultipleRequests()
      throws Exception {
    final ExecutorService executorService = Executors.newCachedThreadPool();
    Future[] futures = new Future[NUM_REQUESTS];
    for (int i = 0; i < NUM_REQUESTS; i++) {
      final int requestId = i;
      futures[i] = executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            testSingleRequest(executorService, requestId);
          } catch (Exception e) {
            Assert.fail();
          }
        }
      });
    }
    for (Future future : futures) {
      future.get();
    }
    executorService.shutdown();
    Assert.assertTrue(TraceContext.REQUEST_TO_TRACES_MAP.isEmpty());
  }

  private void testSingleRequest(ExecutorService executorService, final long requestId)
      throws Exception {
    Set<String> expectedTraces = new HashSet<>(NUM_CHILDREN_PER_REQUEST + 1);
    TraceContext.register(requestId);
    String key = Integer.toString(RANDOM.nextInt());
    int value = RANDOM.nextInt();
    expectedTraces.add(getTraceString(key, value));

    // Add (requestId + 1) logs
    for (int i = 0; i <= requestId; i++) {
      TraceContext.logInfo(key, value);
    }

    Future[] futures = new Future[NUM_CHILDREN_PER_REQUEST];
    for (int i = 0; i < NUM_CHILDREN_PER_REQUEST; i++) {
      final String chileKey = Integer.toString(RANDOM.nextInt());
      final int childValue = RANDOM.nextInt();
      expectedTraces.add(getTraceString(chileKey, childValue));

      futures[i] = executorService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          // Add (requestId + 1) logs
          for (int j = 0; j <= requestId; j++) {
            TraceContext.logInfo(chileKey, childValue);
          }
        }
      });
    }
    for (Future future : futures) {
      future.get();
    }

    Queue<TraceContext.Trace> traces = TraceContext.REQUEST_TO_TRACES_MAP.get(requestId);
    Assert.assertNotNull(traces);
    Assert.assertEquals(traces.size(), NUM_CHILDREN_PER_REQUEST + 1);
    for (TraceContext.Trace trace : traces) {
      // Trace Id is not deterministic because it relies on the order of runJob() getting called
      List<TraceContext.Trace.LogEntry> logs = trace._logs;
      Assert.assertEquals(logs.size(), requestId + 1);
      for (TraceContext.Trace.LogEntry log : logs) {
        Assert.assertTrue(expectedTraces.contains(log.toJson().toString()));
      }
    }

    TraceContext.unregister();
  }

  private static String getTraceString(String key, Object value) {
    return JsonUtils.newObjectNode().set(key, JsonUtils.objectToJsonNode(value)).toString();
  }
}
