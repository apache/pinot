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
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TraceContextTest {
  private static final int NUM_CHILDREN_PER_REQUEST = 5000;
  private static final int NUM_REQUESTS = 10;
  private static final Random RANDOM = new Random();

  @Test
  public void testSingleRequest()
      throws Exception {
    ExecutorService executorService = Executors.newCachedThreadPool();
    testSingleRequest(executorService, 0);
    executorService.shutdown();
    assertTrue(TraceContext.REQUEST_TO_TRACES_MAP.isEmpty());
  }

  @Test
  public void testMultipleRequests()
      throws Exception {
    ExecutorService executorService = Executors.newCachedThreadPool();
    Future[] futures = new Future[NUM_REQUESTS];
    for (int i = 0; i < NUM_REQUESTS; i++) {
      int numLogs = i + 1;
      futures[i] = executorService.submit(() -> {
        try {
          testSingleRequest(executorService, numLogs);
        } catch (Exception e) {
          fail();
        }
      });
    }
    for (Future future : futures) {
      future.get();
    }
    executorService.shutdown();
    assertTrue(TraceContext.REQUEST_TO_TRACES_MAP.isEmpty());
  }

  private void testSingleRequest(ExecutorService executorService, int numLogs)
      throws Exception {
    assertNull(TraceContext.getTraceEntry());
    TraceContext.register();
    String key = Integer.toString(RANDOM.nextInt());
    int value = RANDOM.nextInt();
    Set<String> expectedTraces = new HashSet<>();
    expectedTraces.add(getTraceString(key, value));

    for (int i = 0; i < numLogs; i++) {
      TraceContext.logInfo(key, value);
    }

    Future[] futures = new Future[NUM_CHILDREN_PER_REQUEST];
    for (int i = 0; i < NUM_CHILDREN_PER_REQUEST; i++) {
      String chileKey = Integer.toString(RANDOM.nextInt());
      int childValue = RANDOM.nextInt();
      expectedTraces.add(getTraceString(chileKey, childValue));

      futures[i] = executorService.submit(new TraceRunnable() {
        @Override
        public void runJob() {
          for (int j = 0; j < numLogs; j++) {
            TraceContext.logInfo(chileKey, childValue);
          }
        }
      });
    }
    for (Future future : futures) {
      future.get();
    }
    // to check uniqueness of traceIds
    Set<String> traceIds = new HashSet<>();
    TraceContext.TraceEntry traceEntry = TraceContext.getTraceEntry();
    assertNotNull(traceEntry);
    Queue<TraceContext.Trace> traces = TraceContext.REQUEST_TO_TRACES_MAP.get(traceEntry._id);
    assertNotNull(traces);
    assertEquals(traces.size(), NUM_CHILDREN_PER_REQUEST + 1);
    for (TraceContext.Trace trace : traces) {
      // Trace Id is not deterministic because it relies on the order of runJob() getting called
      List<TraceContext.Trace.LogEntry> logs = trace._logs;
      traceIds.add(trace._traceId);
      assertEquals(logs.size(), numLogs);
      for (TraceContext.Trace.LogEntry log : logs) {
        assertTrue(expectedTraces.contains(log.toJson().toString()));
      }
    }
    assertEquals(traceIds.size(), NUM_CHILDREN_PER_REQUEST + 1);
    TraceContext.unregister();
    assertNull(TraceContext.getTraceEntry());
  }

  private static String getTraceString(String key, Object value) {
    return JsonUtils.newObjectNode().set(key, JsonUtils.objectToJsonNode(value)).toString();
  }
}
