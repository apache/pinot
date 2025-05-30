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

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.ServerRouteInfo;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.auth.request.BrokerRequest;
import org.apache.pinot.spi.config.table.TableConfig;
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
  private final Map<String, Map<String, List<ServerInstance>>> _tableSegmentServersMap;
  private final Set<String> _disabledTables;

  public MockRoutingManagerFactory(int... ports) {
    _tableNameMap = new HashMap<>();
    _schemaMap = new HashMap<>();
    _hybridTables = new HashSet<>();
    _serverInstances = new HashMap<>();
    _tableSegmentServersMap = new HashMap<>();
    _disabledTables = new HashSet<>();
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
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    _tableNameMap.put(tableNameWithType, tableNameWithType);
    _tableNameMap.put(rawTableName, rawTableName);
    _schemaMap.put(rawTableName, schema);
  }

  public void registerSegment(int insertToServerPort, String tableNameWithType, String segmentName) {
    ServerInstance serverInstance = _serverInstances.get(toHostname(insertToServerPort));
    _tableSegmentServersMap.computeIfAbsent(tableNameWithType, k -> new HashMap<>())
        .computeIfAbsent(segmentName, k -> new ArrayList<>())
        .add(serverInstance);
  }

  public void disableTable(String tableNameWithType) {
    _disabledTables.add(tableNameWithType);
  }

  public RoutingManager buildRoutingManager(
      @Nullable Map<String, TablePartitionReplicatedServersInfo> partitionInfoMap) {
    int numTables = _tableSegmentServersMap.size();
    Map<String, RoutingTable> routingTableMap = Maps.newHashMapWithExpectedSize(numTables);
    Map<String, List<String>> tableSegmentsMap = Maps.newHashMapWithExpectedSize(numTables);
    int serverId = 0;
    for (Map.Entry<String, Map<String, List<ServerInstance>>> entry : _tableSegmentServersMap.entrySet()) {
      String tableNameWithType = entry.getKey();
      Map<String, List<ServerInstance>> segmentServersMap = entry.getValue();
      Map<ServerInstance, ServerRouteInfo> serverRouteInfoMap = new HashMap<>();
      for (Map.Entry<String, List<ServerInstance>> segmentServersEntry : segmentServersMap.entrySet()) {
        String segment = segmentServersEntry.getKey();
        List<ServerInstance> servers = segmentServersEntry.getValue();
        ServerInstance server;
        int numServers = servers.size();
        if (numServers == 1) {
          server = servers.get(0);
        } else {
          server = servers.get(serverId % numServers);
          serverId++;
        }
        serverRouteInfoMap.computeIfAbsent(server, k -> new ServerRouteInfo(new ArrayList<>(), null))
            .getSegments()
            .add(segment);
      }
      routingTableMap.put(tableNameWithType, new RoutingTable(serverRouteInfoMap, List.of(), 0));
      tableSegmentsMap.put(tableNameWithType, new ArrayList<>(segmentServersMap.keySet()));
    }
    Map<String, TablePartitionInfo> tablePartitionInfoMap = null;
    if (partitionInfoMap != null) {
      tablePartitionInfoMap = new HashMap<>();
      for (Map.Entry<String, TablePartitionReplicatedServersInfo> entry : partitionInfoMap.entrySet()) {
        String tableNameWithType = entry.getKey();
        TablePartitionReplicatedServersInfo partitionInfo = entry.getValue();
        // Create TablePartitionInfo from TablePartitionReplicatedServersInfo to mimic the simpler case when:
        // 1. There are no excluded new segments.
        // 2. There are no segments with invalid partition id.
        TablePartitionReplicatedServersInfo.PartitionInfo[] partitionInfos = partitionInfo.getPartitionInfoMap();
        List<List<String>> segmentsByPartition = new ArrayList<>();
        for (TablePartitionReplicatedServersInfo.PartitionInfo partition : partitionInfos) {
          segmentsByPartition.add(partition == null ? List.of() : partition._segments);
        }
        TablePartitionInfo tablePartitionInfo =
            new TablePartitionInfo(tableNameWithType, partitionInfo.getPartitionColumn(),
                partitionInfo.getPartitionFunctionName(), partitionInfo.getNumPartitions(), segmentsByPartition,
                List.of());
        tablePartitionInfoMap.put(tableNameWithType, tablePartitionInfo);
      }
    }
    return new FakeRoutingManager(routingTableMap, tableSegmentsMap, _hybridTables, _disabledTables, partitionInfoMap,
        _serverInstances, tablePartitionInfoMap);
  }

  public TableCache buildTableCache() {
    TableCache mock = mock(TableCache.class);
    when(mock.getTableNameMap()).thenReturn(_tableNameMap);
    when(mock.getActualTableName(anyString())).thenAnswer(invocationOnMock -> {
      String tableName = invocationOnMock.getArgument(0);
      return _tableNameMap.get(tableName);
    });
    when(mock.getSchema(anyString())).thenAnswer(invocationOnMock -> {
      String schemaName = invocationOnMock.getArgument(0);
      return _schemaMap.get(schemaName);
    });
    when(mock.getTableConfig(anyString())).thenAnswer(invocationOnMock -> {
      String tableName = invocationOnMock.getArgument(0);
      if (TableNameBuilder.getTableTypeFromTableName(tableName) != null && _tableNameMap.containsKey(tableName)) {
        return mock(TableConfig.class);
      }
      return null;
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
    private final Map<String, List<String>> _segmentsMap;
    private final Set<String> _hybridTables;
    private final Set<String> _disabledTables;
    @Nullable
    private final Map<String, TablePartitionReplicatedServersInfo> _partitionReplicatedServersInfoMap;
    @Nullable
    private final Map<String, TablePartitionInfo> _partitionInfoMap;
    private final Map<String, ServerInstance> _serverInstances;

    public FakeRoutingManager(Map<String, RoutingTable> routingTableMap, Map<String, List<String>> segmentsMap,
        Set<String> hybridTables, Set<String> disabledTables,
        @Nullable Map<String, TablePartitionReplicatedServersInfo> partitionReplicatedServersInfoMap,
        Map<String, ServerInstance> serverInstances, @Nullable Map<String, TablePartitionInfo> partitionInfoMap) {
      _segmentsMap = segmentsMap;
      _routingTableMap = routingTableMap;
      _hybridTables = hybridTables;
      _partitionReplicatedServersInfoMap = partitionReplicatedServersInfoMap;
      _serverInstances = serverInstances;
      _disabledTables = disabledTables;
      _partitionInfoMap = partitionInfoMap;
    }

    @Override
    public Map<String, ServerInstance> getEnabledServerInstanceMap() {
      return _serverInstances;
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
      String tableNameWithType = brokerRequest.getPinotQuery().getDataSource().getTableName();
      return _routingTableMap.get(tableNameWithType);
    }

    @Nullable
    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, String tableNameWithType, long requestId) {
      return _routingTableMap.get(tableNameWithType);
    }

    @Nullable
    @Override
    public List<String> getSegments(BrokerRequest brokerRequest) {
      String tableNameWithType = brokerRequest.getPinotQuery().getDataSource().getTableName();
      return _segmentsMap.get(tableNameWithType);
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

    @Nullable
    @Override
    public TablePartitionReplicatedServersInfo getTablePartitionReplicatedServersInfo(String tableNameWithType) {
      return _partitionReplicatedServersInfoMap != null ? _partitionReplicatedServersInfoMap.get(tableNameWithType)
          : null;
    }

    @Override
    public Set<String> getServingInstances(String tableNameWithType) {
      return _serverInstances.keySet();
    }

    @Override
    public boolean isTableDisabled(String tableNameWithType) {
      return _disabledTables.contains(tableNameWithType);
    }
  }
}
