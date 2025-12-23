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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.systemtable.SystemTableDataProvider;
import org.apache.pinot.common.systemtable.SystemTableRegistry;
import org.apache.pinot.common.systemtable.SystemTableResponseUtils;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.BadQueryRequestException;
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

    BrokerResponse response = handler.handleRequest("SELECT tableName,status FROM system.tables");
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
  public void testSystemTablesNativeResponsePassThrough()
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

    BrokerResponse response = handler.handleRequest("SELECT tableName,status FROM system.tables LIMIT 1 OFFSET 1");
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
  public void testUnsupportedFeaturesIncludeDetails()
      throws Exception {
    SystemTableRegistry.register(new FakeTablesProvider());

    TableCache tableCache = Mockito.mock(TableCache.class);
    Mockito.when(tableCache.getColumnNameMap(anyString())).thenReturn(null);
    Mockito.when(tableCache.getSchema(anyString())).thenReturn(null);

    SystemTableBrokerRequestHandler handler =
        new SystemTableBrokerRequestHandler(new PinotConfiguration(), "testBrokerId",
            new BrokerRequestIdGenerator(), null, ACCESS_CONTROL_FACTORY, null, tableCache,
            ThreadAccountantUtils.getNoOpAccountant());

    BrokerResponse response = handler.handleRequest("SELECT tableName FROM system.tables ORDER BY tableName");
    assertEquals(response.getExceptionsSize(), 1);
    String message = ((BrokerResponseNative) response).getExceptions().get(0).getMessage();
    Assert.assertTrue(message.contains("ORDER BY"), message);
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
    public BrokerResponseNative getBrokerResponse(PinotQuery pinotQuery)
        throws BadQueryRequestException {
      List<GenericRow> rows = new ArrayList<>();
      GenericRow row1 = new GenericRow();
      row1.putValue("tableName", "tblA");
      row1.putValue("status", "ONLINE");
      rows.add(row1);

      GenericRow row2 = new GenericRow();
      row2.putValue("tableName", "tblB");
      row2.putValue("status", "OFFLINE");
      rows.add(row2);

      int totalRows = rows.size();
      List<String> projectionColumns = pinotQuery.getSelectList().stream()
          .map(expr -> expr.getIdentifier().getName()).collect(java.util.stream.Collectors.toList());
      int offset = Math.max(0, pinotQuery.getOffset());
      int limit = pinotQuery.getLimit();
      if (offset > 0) {
        if (offset >= rows.size()) {
          return SystemTableResponseUtils.buildBrokerResponse(getTableName(), _schema, projectionColumns, List.of(),
              totalRows);
        }
        rows = new ArrayList<>(rows.subList(offset, rows.size()));
      }
      if (limit == 0) {
        rows = List.of();
      } else if (limit > 0 && rows.size() > limit) {
        rows = new ArrayList<>(rows.subList(0, limit));
      }
      return SystemTableResponseUtils.buildBrokerResponse(getTableName(), _schema, projectionColumns, rows, totalRows);
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
    public BrokerResponseNative getBrokerResponse(PinotQuery pinotQuery) {
      List<String> projectionColumns = pinotQuery.getSelectList().stream()
          .map(expr -> expr.getIdentifier().getName()).collect(java.util.stream.Collectors.toList());
      DataSchema dataSchema = new DataSchema(new String[]{"tableName", "latencyMs"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG});
      List<Object[]> rows = List.<Object[]>of(new Object[]{"tblC", 123L});
      BrokerResponseNative brokerResponseNative = new BrokerResponseNative();
      brokerResponseNative.setResultTable(new ResultTable(dataSchema, rows));
      brokerResponseNative.setNumDocsScanned(rows.size());
      brokerResponseNative.setNumEntriesScannedPostFilter(rows.size());
      brokerResponseNative.setTotalDocs(rows.size());
      brokerResponseNative.setTablesQueried(Set.of(getTableName()));
      return brokerResponseNative;
    }
  }
}
