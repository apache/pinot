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
import java.util.List;
import java.util.Map;
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
import org.apache.pinot.spi.config.table.TableType;
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

  private final HashMap<String, String> _tableNameMap;
  private final Map<String, Schema> _schemaMap;

  private final Map<String, ServerInstance> _serverInstances;
  private final Map<String, RoutingTable> _routingTableMap;
  private final List<String> _hybridTables;

  private final Map<String, Map<ServerInstance, List<String>>> _tableServerSegmentMap;

  public MockRoutingManagerFactory(int... ports) {
    _hybridTables = new ArrayList<>();
    _serverInstances = new HashMap<>();
    _schemaMap = new HashMap<>();
    _tableNameMap = new HashMap<>();
    _routingTableMap = new HashMap<>();

    _tableServerSegmentMap = new HashMap<>();
    for (int port : ports) {
      _serverInstances.put(toHostname(port), getServerInstance(HOST_NAME, port, port, port, port));
    }
  }

  public MockRoutingManagerFactory registerTable(Schema schema, String tableName) {
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
    if (tableType == null) {
      registerTableNameWithType(schema, TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(tableName));
      registerTableNameWithType(schema, TableNameBuilder.forType(TableType.REALTIME).tableNameWithType(tableName));
      _hybridTables.add(tableName);
    } else {
      registerTableNameWithType(schema, TableNameBuilder.forType(tableType).tableNameWithType(tableName));
    }
    return this;
  }

  public MockRoutingManagerFactory registerSegment(int insertToServerPort, String tableNameWithType,
      String segmentName) {
    Map<ServerInstance, List<String>> serverSegmentMap =
        _tableServerSegmentMap.getOrDefault(tableNameWithType, new HashMap<>());
    ServerInstance serverInstance = _serverInstances.get(toHostname(insertToServerPort));

    List<String> sSegments = serverSegmentMap.getOrDefault(serverInstance, new ArrayList<>());
    sSegments.add(segmentName);
    serverSegmentMap.put(serverInstance, sSegments);
    _tableServerSegmentMap.put(tableNameWithType, serverSegmentMap);
    return this;
  }

  public RoutingManager buildRoutingManager() {
    // create all the fake routing tables
    _routingTableMap.clear();
    for (Map.Entry<String, Map<ServerInstance, List<String>>> tableEntry : _tableServerSegmentMap.entrySet()) {
      String tableNameWithType = tableEntry.getKey();
      RoutingTable fakeRoutingTable = new RoutingTable(tableEntry.getValue(), Collections.emptyList(), 0);
      _routingTableMap.put(tableNameWithType, fakeRoutingTable);
    }
    return new FakeRoutingManager(_routingTableMap, _serverInstances, _hybridTables);
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

  private static String toHostname(int port) {
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

  private void registerTableNameWithType(Schema schema, String tableNameWithType) {
    String rawTableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    _tableNameMap.put(tableNameWithType, rawTableName);
    _schemaMap.put(rawTableName, schema);
    _schemaMap.put(tableNameWithType, schema);
  }

  private static class FakeRoutingManager implements RoutingManager {
    private final Map<String, RoutingTable> _routingTableMap;
    private final Map<String, ServerInstance> _serverInstances;
    private final List<String> _hybridTables;

    public FakeRoutingManager(Map<String, RoutingTable> routingTableMap, Map<String, ServerInstance> serverInstances,
        List<String> hybridTables) {
      _routingTableMap = routingTableMap;
      _serverInstances = serverInstances;
      _hybridTables = hybridTables;
    }

    @Override
    public Map<String, ServerInstance> getEnabledServerInstanceMap() {
      return _serverInstances;
    }

    @Override
    public RoutingTable getRoutingTable(BrokerRequest brokerRequest, long requestId) {
      String tableName = brokerRequest.getPinotQuery().getDataSource().getTableName();
      return _routingTableMap.getOrDefault(tableName,
          _routingTableMap.get(TableNameBuilder.extractRawTableName(tableName)));
    }

    @Override
    public boolean routingExists(String tableNameWithType) {
      return _routingTableMap.containsKey(tableNameWithType);
    }

    @Override
    public TimeBoundaryInfo getTimeBoundaryInfo(String tableName) {
      String rawTableName = TableNameBuilder.extractRawTableName(tableName);
      return _hybridTables.contains(rawTableName) ? new TimeBoundaryInfo(TIME_BOUNDARY_COLUMN,
          String.valueOf(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1))) : null;
    }

    @Nullable
    @Override
    public TablePartitionInfo getTablePartitionInfo(String tableNameWithType) {
      return null;
    }

    @Override
    public Map<String, ServerInstance> getEnabledServersForTableTenant(String tableNameWithType) {
      return _serverInstances;
    }
  }
}
