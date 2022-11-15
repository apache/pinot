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
import java.util.concurrent.Executors;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.NamedThreadFactory;
import org.apache.pinot.common.utils.SchemaUtils;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.query.runtime.QueryRunner;
import org.apache.pinot.query.runtime.executor.OpChainSchedulerService;
import org.apache.pinot.query.runtime.executor.RoundRobinScheduler;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.query.testutils.MockInstanceDataManagerFactory;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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
  private static final int DEFAULT_EXECUTOR_THREAD_NUM = 5;
  private static final String TABLE_CONFIGS_PREFIX = "/CONFIGS/TABLE/";
  private static final String SCHEMAS_PREFIX = "/SCHEMAS/";

  private final OpChainSchedulerService _scheduler;
  private final int _queryRunnerPort;
  private final Map<String, Object> _runnerConfig = new HashMap<>();
  private final InstanceDataManager _instanceDataManager;
  private final HelixManager _helixManager;

  private QueryRunner _queryRunner;

  public QueryServerEnclosure(MockInstanceDataManagerFactory factory) {
    try {
      _instanceDataManager = factory.buildInstanceDataManager();
      _helixManager = mockHelixManager(factory.buildSchemaMap());
      _queryRunnerPort = QueryTestUtils.getAvailablePort();
      _runnerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_PORT, _queryRunnerPort);
      _runnerConfig.put(QueryConfig.KEY_OF_QUERY_RUNNER_HOSTNAME,
          String.format("Server_%s", QueryConfig.DEFAULT_QUERY_RUNNER_HOSTNAME));
      _queryRunner = new QueryRunner();
      _scheduler = new OpChainSchedulerService(new RoundRobinScheduler(),
          Executors.newFixedThreadPool(
              DEFAULT_EXECUTOR_THREAD_NUM,
              new NamedThreadFactory("test_query_server_enclosure_on_" + _queryRunnerPort + "_port")));
    } catch (Exception e) {
      throw new RuntimeException("Test Failed!", e);
    }
  }

  private HelixManager mockHelixManager(Map<String, Schema> schemaMap) {
    ZkHelixPropertyStore<ZNRecord> zkHelixPropertyStore = mock(ZkHelixPropertyStore.class);
    when(zkHelixPropertyStore.get(anyString(), any(), anyInt())).thenAnswer(invocationOnMock -> {
      String path = invocationOnMock.getArgument(0);
      if (path.startsWith(TABLE_CONFIGS_PREFIX)) {
        // TODO: add table config mock.
        return null;
      } else if (path.startsWith(SCHEMAS_PREFIX)) {
        String tableName = TableNameBuilder.extractRawTableName(path.substring(SCHEMAS_PREFIX.length()));
        return SchemaUtils.toZNRecord(schemaMap.get(tableName));
      } else {
        return null;
      }
    });
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixPropertyStore()).thenReturn(zkHelixPropertyStore);
    return helixManager;
  }

  private ServerMetrics mockServiceMetrics() {
    return mock(ServerMetrics.class);
  }

  public int getPort() {
    return _queryRunnerPort;
  }

  public void start()
      throws Exception {
    PinotConfiguration configuration = new PinotConfiguration(_runnerConfig);
    _queryRunner = new QueryRunner();
    _queryRunner.init(configuration, _instanceDataManager, _helixManager, mockServiceMetrics());
    _queryRunner.start();
    _scheduler.startAsync().awaitRunning();
  }

  public void shutDown() {
    _queryRunner.shutDown();
    _scheduler.stopAsync().awaitTerminated();
  }

  public void processQuery(DistributedStagePlan distributedStagePlan, Map<String, String> requestMetadataMap) {
    _queryRunner.processQuery(distributedStagePlan, _scheduler, requestMetadataMap);
  }
}
