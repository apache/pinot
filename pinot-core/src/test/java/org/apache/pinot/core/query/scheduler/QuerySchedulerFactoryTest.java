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
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.query.executor.QueryExecutor;
import org.apache.pinot.core.query.request.ServerQueryRequest;
import org.apache.pinot.core.query.scheduler.fcfs.BoundedFCFSScheduler;
import org.apache.pinot.core.query.scheduler.fcfs.FCFSQueryScheduler;
import org.apache.pinot.core.query.scheduler.resources.UnboundedResourceManager;
import org.apache.pinot.core.query.scheduler.tokenbucket.TokenPriorityScheduler;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertTrue;


public class QuerySchedulerFactoryTest {

  @Test
  public void testQuerySchedulerFactory() {
    QueryExecutor queryExecutor = mock(QueryExecutor.class);
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    LongAccumulator latestQueryTime = mock(LongAccumulator.class);

    PinotConfiguration config = new PinotConfiguration();
    config.setProperty(QuerySchedulerFactory.ALGORITHM_NAME_CONFIG_KEY, QuerySchedulerFactory.FCFS_ALGORITHM);
    QueryScheduler queryScheduler = QuerySchedulerFactory.create(config, queryExecutor, serverMetrics, latestQueryTime);
    assertTrue(queryScheduler instanceof FCFSQueryScheduler);

    config.setProperty(QuerySchedulerFactory.ALGORITHM_NAME_CONFIG_KEY, QuerySchedulerFactory.TOKEN_BUCKET_ALGORITHM);
    queryScheduler = QuerySchedulerFactory.create(config, queryExecutor, serverMetrics, latestQueryTime);
    assertTrue(queryScheduler instanceof TokenPriorityScheduler);

    config.setProperty(QuerySchedulerFactory.ALGORITHM_NAME_CONFIG_KEY, QuerySchedulerFactory.BOUNDED_FCFS_ALGORITHM);
    queryScheduler = QuerySchedulerFactory.create(config, queryExecutor, serverMetrics, latestQueryTime);
    assertTrue(queryScheduler instanceof BoundedFCFSScheduler);

    config.setProperty(QuerySchedulerFactory.ALGORITHM_NAME_CONFIG_KEY, TestQueryScheduler.class.getName());
    queryScheduler = QuerySchedulerFactory.create(config, queryExecutor, serverMetrics, latestQueryTime);
    assertTrue(queryScheduler instanceof TestQueryScheduler);
  }

  public static final class TestQueryScheduler extends QueryScheduler {

    public TestQueryScheduler(PinotConfiguration config, QueryExecutor queryExecutor, ServerMetrics serverMetrics,
        LongAccumulator latestQueryTime) {
      super(config, queryExecutor, new UnboundedResourceManager(config), serverMetrics, latestQueryTime);
    }

    @Override
    public ListenableFuture<byte[]> submit(ServerQueryRequest queryRequest) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
      return "Test";
    }
  }
}
