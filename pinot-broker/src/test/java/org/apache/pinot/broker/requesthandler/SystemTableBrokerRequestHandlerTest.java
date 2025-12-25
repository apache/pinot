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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.systemtable.SystemTableDataProvider;
import org.apache.pinot.common.systemtable.SystemTableRegistry;
import org.apache.pinot.common.systemtable.datasource.InMemorySystemTableSegment;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.IndexSegment;
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
import org.testng.annotations.BeforeMethod;
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

  @BeforeMethod
  public void clearSystemTableRegistry()
      throws Exception {
    SystemTableRegistry.close();
  }

  @Test
  public void testSystemTablesQuery()
      throws Exception {
    SystemTableRegistry.register(new FakeTablesProvider());

    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableBrokerRequestHandler handler =
        new SystemTableBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), null, ACCESS_CONTROL_FACTORY, null, tableCache,
            ThreadAccountantUtils.getNoOpAccountant());

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
    SystemTableRegistry.register(new NativeResponseProvider());

    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableBrokerRequestHandler handler =
        new SystemTableBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), null, ACCESS_CONTROL_FACTORY, null, tableCache,
            ThreadAccountantUtils.getNoOpAccountant());

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
    SystemTableRegistry.register(new FakeTablesProvider());

    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableBrokerRequestHandler handler =
        new SystemTableBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), null, ACCESS_CONTROL_FACTORY, null, tableCache,
            ThreadAccountantUtils.getNoOpAccountant());

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
    SystemTableRegistry.register(new FakeTablesProvider());

    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableBrokerRequestHandler handler =
        new SystemTableBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), null, ACCESS_CONTROL_FACTORY, null, tableCache,
            ThreadAccountantUtils.getNoOpAccountant());

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

  private static class FakeTablesProvider implements SystemTableDataProvider {
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

  private static class NativeResponseProvider implements SystemTableDataProvider {
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
}
