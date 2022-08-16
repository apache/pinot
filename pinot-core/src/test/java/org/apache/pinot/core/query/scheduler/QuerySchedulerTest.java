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
package org.apache.pinot.core.query.scheduler;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.resources.ResourceManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class QuerySchedulerTest {
  @Test
  public void testCancelQuery() {
    PinotConfiguration config = new PinotConfiguration();
    config.setProperty("enable.query.cancellation", "true");
    QueryScheduler qs = createQueryScheduler(config);
    Set<String> queryIds = new HashSet<>();
    queryIds.add("foo");
    queryIds.add("bar");
    queryIds.add("baz");
    for (String id : queryIds) {
      ServerQueryRequest query = mock(ServerQueryRequest.class);
      when(query.getQueryId()).thenReturn(id);
      qs.submitQuery(query);
    }
    Assert.assertEquals(qs.getRunningQueryIds(), queryIds);
    for (String id : queryIds) {
      qs.cancelQuery(id);
    }
    Assert.assertTrue(qs.getRunningQueryIds().isEmpty());
    Assert.assertFalse(qs.cancelQuery("unknown"));
  }

  private QueryScheduler createQueryScheduler(PinotConfiguration config) {
    return new QueryScheduler(config, mock(QueryExecutor.class), mock(ResourceManager.class), mock(ServerMetrics.class),
        new LongAccumulator(Long::max, 0)) {
      @Override
      public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
        // Create a FutureTask does nothing but waits to be cancelled and trigger callbacks.
        return ListenableFutureTask.create(() -> null);
      }

      @Override
      public String name() {
        return "noop";
      }
    };
  }
}
