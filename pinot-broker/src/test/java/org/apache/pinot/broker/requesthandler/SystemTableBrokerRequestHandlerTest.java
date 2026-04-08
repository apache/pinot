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
package org.apache.pinot.broker.requesthandler;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.helix.HelixManager;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.systemtable.SystemTableProvider;
import org.apache.pinot.common.systemtable.SystemTableRegistry;
import org.apache.pinot.common.systemtable.datasource.InMemorySystemTableSegment;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.routing.MultiClusterRoutingContext;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.accounting.ThreadAccountant;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class SystemTableBrokerRequestHandlerTest {
  private static final AccessControlFactory ACCESS_CONTROL_FACTORY = new AllowAllAccessControlFactory();

  @BeforeClass
  public void setUp() {
    BrokerMetrics.register(Mockito.mock(BrokerMetrics.class));
    BrokerQueryEventListenerFactory.init(new PinotConfiguration());
  }

  @Test
  public void testSystemTablesQuery()
      throws Exception {
    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableRegistry registry = new SystemTableRegistry(tableCache, null, null, null);
    registry.register(new FakeTablesProvider());

    SystemTableBrokerRequestHandler handler =
        new SystemTableBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), null, ACCESS_CONTROL_FACTORY, null, tableCache, registry,
            ThreadAccountantUtils.getNoOpAccountant(), null, null);

    BrokerResponse response = handler.handleRequest("SELECT tableName,status FROM system.tables ORDER BY tableName");
    if (response.getExceptionsSize() > 0) {
      Assert.fail("Unexpected exceptions: " + response.getExceptions());
    }
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable, response.toString());
    DataSchema dataSchema = resultTable.getDataSchema();
    assertEquals(dataSchema.getColumnNames(), new String[]{"tableName", "status"});
    assertEquals(dataSchema.getColumnDataTypes(), new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0], "tblA");
    assertEquals(rows.get(0)[1], "ONLINE");
    assertEquals(rows.get(1)[0], "tblB");
    assertEquals(rows.get(1)[1], "OFFLINE");
  }

  @Test
  public void testSystemTablesQueryOnAnotherSystemTable()
      throws Exception {
    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableRegistry registry = new SystemTableRegistry(tableCache, null, null, null);
    registry.register(new NativeResponseProvider());

    SystemTableBrokerRequestHandler handler =
        new SystemTableBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), null, ACCESS_CONTROL_FACTORY, null, tableCache, registry,
            ThreadAccountantUtils.getNoOpAccountant(), null, null);

    BrokerResponse response = handler.handleRequest("SELECT tableName,latencyMs FROM system.native_latency");
    if (response.getExceptionsSize() > 0) {
      Assert.fail("Unexpected exceptions: " + response.getExceptions());
    }
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable, response.toString());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows().get(0)[0], "tblC");
    assertEquals(resultTable.getRows().get(0)[1], 123L);
    assertEquals(((BrokerResponseNative) response).getTablesQueried(), Set.of("system.native_latency"));
    assertEquals(response.getNumDocsScanned(), 1);
    assertEquals(response.getTotalDocs(), 1);
  }

  @Test
  public void testSystemTablesOffsetLimit()
      throws Exception {
    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableRegistry registry = new SystemTableRegistry(tableCache, null, null, null);
    registry.register(new FakeTablesProvider());

    SystemTableBrokerRequestHandler handler =
        new SystemTableBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), null, ACCESS_CONTROL_FACTORY, null, tableCache, registry,
            ThreadAccountantUtils.getNoOpAccountant(), null, null);

    BrokerResponse response = handler.handleRequest(
        "SELECT tableName,status FROM system.tables ORDER BY tableName LIMIT 1 OFFSET 1");
    if (response.getExceptionsSize() > 0) {
      Assert.fail("Unexpected exceptions: " + response.getExceptions());
    }
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable, response.toString());
    assertEquals(resultTable.getRows().size(), 1);
    assertEquals(resultTable.getRows().get(0)[0], "tblB");
    assertEquals(resultTable.getRows().get(0)[1], "OFFLINE");
    assertEquals(response.getTotalDocs(), 2);
  }

  @Test
  public void testSystemTablesSupportGroupBy()
      throws Exception {
    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableRegistry registry = new SystemTableRegistry(tableCache, null, null, null);
    registry.register(new FakeTablesProvider());

    SystemTableBrokerRequestHandler handler =
        new SystemTableBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), null, ACCESS_CONTROL_FACTORY, null, tableCache, registry,
            ThreadAccountantUtils.getNoOpAccountant(), null, null);

    BrokerResponse response = handler.handleRequest(
        "SELECT status, COUNT(*) AS cnt FROM system.tables GROUP BY status ORDER BY status");
    if (response.getExceptionsSize() > 0) {
      Assert.fail("Unexpected exceptions: " + response.getExceptions());
    }
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable, response.toString());
    assertEquals(resultTable.getDataSchema().getColumnNames(), new String[]{"status", "cnt"});
    assertEquals(resultTable.getDataSchema().getColumnDataTypes(), new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0], "OFFLINE");
    assertEquals(rows.get(0)[1], 1L);
    assertEquals(rows.get(1)[0], "ONLINE");
    assertEquals(rows.get(1)[1], 1L);
  }

  @Test
  public void testScatterGatherSystemTableQuery()
      throws Exception {
    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableRegistry registry = new SystemTableRegistry(tableCache, null, null, null);
    registry.register(new ScatterGatherTablesProvider());

    SystemTableBrokerRequestHandler handler =
        new ScatterGatherTestHandler(new PinotConfiguration(), "testBrokerId", new BrokerRequestIdGenerator(), null,
            ACCESS_CONTROL_FACTORY, null, tableCache, registry, ThreadAccountantUtils.getNoOpAccountant(), null, null);

    BrokerResponse response =
        handler.handleRequest("SELECT tableName,status FROM system.sg_tables ORDER BY tableName");
    if (response.getExceptionsSize() > 0) {
      Assert.fail("Unexpected exceptions: " + response.getExceptions());
    }
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable, response.toString());
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0)[0], "tblA");
    assertEquals(rows.get(0)[1], "ONLINE");
    assertEquals(rows.get(1)[0], "tblB");
    assertEquals(rows.get(1)[1], "OFFLINE");
  }

  private static class FakeTablesProvider implements SystemTableProvider {
    private final Schema _schema = new Schema.SchemaBuilder().setSchemaName("system.tables")
        .addSingleValueDimension("tableName", FieldSpec.DataType.STRING)
        .addSingleValueDimension("status", FieldSpec.DataType.STRING).build();

    @Override
    public String getTableName() {
      return "system.tables";
    }

    @Override
    public Schema getSchema() {
      return _schema;
    }

    @Override
    public TableConfig getTableConfig() {
      return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
    }

    @Override
    public IndexSegment getDataSource() {
      Map<String, IntFunction<Object>> valueProviders = new HashMap<>();
      valueProviders.put("tableName", docId -> docId == 0 ? "tblA" : "tblB");
      valueProviders.put("status", docId -> docId == 0 ? "ONLINE" : "OFFLINE");
      return new InMemorySystemTableSegment(getTableName(), _schema, 2, valueProviders);
    }
  }

  private static class NativeResponseProvider implements SystemTableProvider {
    private final Schema _schema = new Schema.SchemaBuilder().setSchemaName("system.native_latency")
        .addSingleValueDimension("tableName", FieldSpec.DataType.STRING)
        .addSingleValueDimension("latencyMs", FieldSpec.DataType.LONG).build();

    @Override
    public String getTableName() {
      return "system.native_latency";
    }

    @Override
    public Schema getSchema() {
      return _schema;
    }

    @Override
    public TableConfig getTableConfig() {
      return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
    }

    @Override
    public IndexSegment getDataSource() {
      Map<String, IntFunction<Object>> valueProviders = new HashMap<>();
      valueProviders.put("tableName", docId -> "tblC");
      valueProviders.put("latencyMs", docId -> 123L);
      return new InMemorySystemTableSegment(getTableName(), _schema, 1, valueProviders);
    }
  }

  private static class ScatterGatherTablesProvider implements SystemTableProvider {
    private final Schema _schema = new Schema.SchemaBuilder().setSchemaName("system.sg_tables")
        .addSingleValueDimension("tableName", FieldSpec.DataType.STRING)
        .addSingleValueDimension("status", FieldSpec.DataType.STRING).build();

    @Override
    public ExecutionMode getExecutionMode() {
      return ExecutionMode.BROKER_SCATTER_GATHER;
    }

    @Override
    public String getTableName() {
      return "system.sg_tables";
    }

    @Override
    public Schema getSchema() {
      return _schema;
    }

    @Override
    public TableConfig getTableConfig() {
      return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
    }

    @Override
    public IndexSegment getDataSource() {
      return new InMemorySystemTableSegment(getTableName(), _schema, 0, Map.of());
    }
  }

  private static class ScatterGatherTestHandler extends SystemTableBrokerRequestHandler {
    ScatterGatherTestHandler(PinotConfiguration config, String brokerId, BrokerRequestIdGenerator requestIdGenerator,
        RoutingManager routingManager, AccessControlFactory accessControlFactory, QueryQuotaManager queryQuotaManager,
        TableCache tableCache, SystemTableRegistry systemTableRegistry, ThreadAccountant threadAccountant,
        @Nullable MultiClusterRoutingContext multiClusterRoutingContext, @Nullable HelixManager helixManager) {
      super(config, brokerId, requestIdGenerator, routingManager, accessControlFactory, queryQuotaManager, tableCache,
          systemTableRegistry, threadAccountant, multiClusterRoutingContext, helixManager);
    }

    @Override
    protected Map<ServerRoutingInstance, DataTable> scatterGatherSystemTableDataTables(SystemTableProvider provider,
        PinotQuery pinotQuery, String tableName, JsonNode request, @Nullable HttpHeaders httpHeaders,
        long deadlineMs) {
      DataSchema dataSchema =
          new DataSchema(new String[]{"tableName", "status"}, new DataSchema.ColumnDataType[]{
              DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.STRING});
      DataTableBuilder builder1 = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
      builder1.startRow();
      builder1.setColumn(0, "tblA");
      builder1.setColumn(1, "ONLINE");
      try {
        builder1.finishRow();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      DataTable dataTable1 = builder1.build();

      DataTableBuilder builder2 = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
      builder2.startRow();
      builder2.setColumn(0, "tblB");
      builder2.setColumn(1, "OFFLINE");
      try {
        builder2.finishRow();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      DataTable dataTable2 = builder2.build();

      Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>(2);
      dataTableMap.put(new ServerRoutingInstance("brokerA", 8000, TableType.OFFLINE), dataTable1);
      dataTableMap.put(new ServerRoutingInstance("brokerB", 8000, TableType.OFFLINE), dataTable2);
      return dataTableMap;
    }
  }
}
