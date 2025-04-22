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
package org.apache.pinot.query;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.helix.HelixManager;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.accounting.ThreadExecutionContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * Query server enclosure for testing Pinot query planner & runtime.
 *
 * This enclosure simulates a deployable component of Pinot that serves
 *   - regular Pinot query server (that serves segment-based queries)
 *   - intermediate stage queries (such as JOIN operator that awaits data scanned from left/right tables)
 *
 * Inside this construct it runs a regular pinot QueryExecutor as well as the new runtime - WorkerExecutor
 * Depending on the query request type it gets routed to either one of the two for execution.
 *
 * It also runs a GRPC Mailbox service that runs the new transport layer protocol as the backbone for all
 * multi-stage query communication.
 */
public class QueryServerEnclosure {
  private final int _queryRunnerPort;
  private final QueryRunner _queryRunner;

  public QueryServerEnclosure(MockInstanceDataManagerFactory factory) {
    this(factory, Map.of());
  }

  public QueryServerEnclosure(MockInstanceDataManagerFactory factory, Map<String, Object> config) {
    _queryRunnerPort = QueryTestUtils.getAvailablePort();
    Map<String, Object> runnerConfig = new HashMap<>(config);
    runnerConfig.put(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_HOSTNAME, "Server_localhost");
    runnerConfig.put(CommonConstants.MultiStageQueryRunner.KEY_OF_QUERY_RUNNER_PORT, _queryRunnerPort);
    InstanceDataManager instanceDataManager = factory.buildInstanceDataManager();
    HelixManager helixManager = factory.buildHelixManager();
    _queryRunner = new QueryRunner();
    _queryRunner.init(new PinotConfiguration(runnerConfig), helixManager, instanceDataManager, null, () -> true);
  }

  public int getPort() {
    return _queryRunnerPort;
  }

  public void start() {
    _queryRunner.start();
  }

  public void shutDown() {
    _queryRunner.shutDown();
  }

  public CompletableFuture<Void> processQuery(WorkerMetadata workerMetadata, StagePlan stagePlan,
      Map<String, String> requestMetadataMap, ThreadExecutionContext parentContext) {
    try (QueryThreadContext.CloseableContext closeMe1 = QueryThreadContext.openFromRequestMetadata(requestMetadataMap);
        QueryThreadContext.CloseableContext closeMe2 = MseWorkerThreadContext.open()) {
      return CompletableFuture.runAsync(
          () -> _queryRunner.processQuery(workerMetadata, stagePlan, requestMetadataMap, parentContext),
          _queryRunner.getExecutorService());
    }
  }
}
