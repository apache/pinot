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
package org.apache.pinot.query.testutils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * This is a builder pattern for generating a Mock Routing Manager.
 */
public class MockRoutingManagerFactory {
  private static final String TIME_BOUNDARY_COLUMN = "ts";
  private static final String HOST_NAME = "localhost";

  private final Map<String, String> _tableNameMap;
  private final Map<String, Schema> _schemaMap;
  private final Set<String> _hybridTables;
  private final Map<String, ServerInstance> _serverInstances;
  private final Map<String, Map<ServerInstance, List<String>>> _tableServerSegmentsMap;

  public MockRoutingManagerFactory(int... ports) {
    _tableNameMap = new HashMap<>();
    _schemaMap = new HashMap<>();
    _hybridTables = new HashSet<>();
    _serverInstances = new HashMap<>();
    _tableServerSegmentsMap = new HashMap<>();
    for (int port : ports) {
      _serverInstances.put(toHostname(port), getServerInstance(HOST_NAME, port, port, port, port));
    }
  }

  public void registerTable(Schema schema, String tableName) {
    if (TableNameBuilder.isTableResource(tableName)) {
      registerTableNameWithType(schema, tableName);
    } else {
      registerTableNameWithType(schema, TableNameBuilder.OFFLINE.tableNameWithType(tableName));
      registerTableNameWithType(schema, TableNameBuilder.REALTIME.tableNameWithType(tableName));
      _hybridTables.add(tableName);
    }
  }

  private void registerTableNameWithType(Schema schema, String tableNameWithType) {
    _tableNameMap.put(tableNameWithType.toLowerCase(Locale.US), tableNameWithType);
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    if (!rawTableName.equals(tableNameWithType)) {
      _tableNameMap.put(rawTableName.toLowerCase(Locale.US), tableNameWithType);
    }
    _schemaMap.put(rawTableName, schema);
  }

  public void registerSegment(int insertToServerPort, String tableNameWithType, String segmentName) {
    ServerInstance serverInstance = _serverInstances.get(toHostname(insertToServerPort));
    _tableServerSegmentsMap.computeIfAbsent(tableNameWithType, k -> new HashMap<>())
        .computeIfAbsent(serverInstance, k -> new ArrayList<>()).add(segmentName);
  }

  public RoutingManager buildRoutingManager(@Nullable Map<String, TablePartitionInfo> partitionInfoMap) {
    Map<String, RoutingTable> routingTableMap = new HashMap<>();
    for (Map.Entry<String, Map<ServerInstance, List<String>>> tableEntry : _tableServerSegmentsMap.entrySet()) {
      String tableNameWithType = tableEntry.getKey();
      RoutingTable fakeRoutingTable = new RoutingTable(tableEntry.getValue(), Collections.emptyList(), 0);
      routingTableMap.put(tableNameWithType, fakeRoutingTable);
    }
    return new FakeRoutingManager(routingTableMap, _hybridTables, partitionInfoMap, _serverInstances);
  }

  public TableCache buildTableCache() {
    TableCache mock = mock(TableCache.class);
    when(mock.getTableNameMap()).thenReturn(_tableNameMap);
    when(mock.getSchema(anyString())).thenAnswer(invocationOnMock -> {
      String schemaName = invocationOnMock.getArgument(0);
      return _schemaMap.get(schemaName);
    });
    return mock;
  }

  public static String toHostname(int port) {
    return String.format("%s_%d", HOST_NAME, port);
  }

  private static ServerInstance getServerInstance(String hostname, int nettyPort, int grpcPort, int servicePort,
      int mailboxPort) {
    String server = String.format("%s%s_%d", CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE, hostname, nettyPort);
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(server);
    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    simpleFields.put(CommonConstants.Helix.Instance.GRPC_PORT_KEY, String.valueOf(grpcPort));
    simpleFields.put(CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_SERVICE_PORT_KEY,
        String.valueOf(servicePort));
    simpleFields.put(CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY,
        String.valueOf(mailboxPort));
    return new ServerInstance(instanceConfig);
  }

  private static class FakeRoutingManager implements RoutingManager {
    private final Map<String, RoutingTable> _routingTableMap;
    private final Set<String> _hybridTables;
    private final Map<String, TablePartitionInfo> _partitionInfoMap;
    private final Map<String, ServerInstance> _serverInstances;

    public FakeRoutingManager(Map<String, RoutingTable> routingTableMap, Set<String> hybridTables,
        @Nullable Map<String, TablePartitionInfo> partitionInfoMap, Map<String, ServerInstance> serverInstances) {
      _routingTableMap = routingTableMap;
      _hybridTables = hybridTables;
      _partitionInfoMap = partitionInfoMap;
      _serverInstances = serverInstances;
    }

    @Override
    public Map<String, ServerInstance> getEnabledServerInstanceMap() {
      return _serverInstances;
    }

    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
      String tableNameWithType = brokerRequest.getPinotQuery().getDataSource().getTableName();
      return _routingTableMap.get(tableNameWithType);
    }

    @Override
    public boolean routingExists(String tableNameWithType) {
      return _routingTableMap.containsKey(tableNameWithType);
    }

    @Nullable
    @Override
    public TimeBoundaryInfo getTimeBoundaryInfo(String offlineTableName) {
      String rawTableName = TableNameBuilder.extractRawTableName(offlineTableName);
      return _hybridTables.contains(rawTableName) ? new TimeBoundaryInfo(TIME_BOUNDARY_COLUMN,
          String.valueOf(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))) : null;
    }

    @Nullable
    @Override
    public TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
      return _partitionInfoMap != null ? _partitionInfoMap.get(tableNameWithType) : null;
    }

    @Override
    public Set<String> getServingInstances(String tableNameWithType) {
      return _serverInstances.keySet();
    }
  }
}
