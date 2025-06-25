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
package org.apache.pinot.core.query.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;


public class BrokerReduceServiceTest {

  @Test
  public void testReduceTimeout()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable GROUP BY col1");
    DataSchema dataSchema =
        new DataSchema(new String[]{"col1", "count(*)"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    int numGroups = 5000;
    for (int i = 0; i < numGroups; i++) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, i);
      dataTableBuilder.setColumn(1, 1L);
      dataTableBuilder.finishRow();
    }
    DataTable dataTable = dataTableBuilder.build();
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    int numInstances = 1000;
    for (int i = 0; i < numInstances; i++) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", i, TableType.OFFLINE);
      dataTableMap.put(instance, dataTable);
    }
    long reduceTimeoutMs = 1;
    BrokerResponseNative brokerResponse =
        brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap, reduceTimeoutMs,
            mock(BrokerMetrics.class));
    brokerReduceService.shutDown();

    List<QueryProcessingException> exceptions = brokerResponse.getExceptions();
    assertEquals(exceptions.size(), 1);
    assertEquals(exceptions.get(0).getErrorCode(), QueryErrorCode.BROKER_TIMEOUT.getId());
  }

  @Test
  public void testSortReduceSortedAndUnsortedTables()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT col1 FROM testTable ORDER BY col1");
    DataSchema dataSchema =
        new DataSchema(new String[]{"col1"}, new ColumnDataType[]{ColumnDataType.INT});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    // sorted block
    int numGroups = 5;
    for (int i = 0; i < numGroups; i++) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, i * 2);
      dataTableBuilder.finishRow();
    }
    DataTable dataTable = dataTableBuilder.build();
    dataTable.getMetadata().put(DataTable.MetadataKey.ORDER_BY_EXPRESSIONS.getName(), "[col1 ASC]");
    // unsorted block
    DataTableBuilder unSortedDataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    int numUnsortedGroups = 5;
    for (int i = 0; i < numUnsortedGroups; i++) {
      unSortedDataTableBuilder.startRow();
      unSortedDataTableBuilder.setColumn(0, numUnsortedGroups + numGroups - i - 1);
      unSortedDataTableBuilder.finishRow();
    }
    DataTable unSortedDataTable = unSortedDataTableBuilder.build();
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    int numSortedInstances = 1;
    for (int i = 0; i < numSortedInstances; i++) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", i, TableType.OFFLINE);
      dataTableMap.put(instance, dataTable);
    }
    int numUnSortedInstances = 1;
    for (int i = 0; i < numUnSortedInstances; i++) {
      ServerRoutingInstance instance =
          new ServerRoutingInstance("localhost", i + numSortedInstances, TableType.OFFLINE);
      dataTableMap.put(instance, unSortedDataTable);
    }
    long reduceTimeoutMs = 100000;
    BrokerResponseNative brokerResponse =
        brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap, reduceTimeoutMs,
            mock(BrokerMetrics.class));
    brokerReduceService.shutDown();

    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 10);
    assertEquals(rows.get(0), new Object[]{0});
    assertEquals(rows.get(1), new Object[]{2});
    assertEquals(rows.get(2), new Object[]{4});
    assertEquals(rows.get(3), new Object[]{5});
    assertEquals(rows.get(4), new Object[]{6});
    assertEquals(rows.get(7), new Object[]{8});
    assertEquals(rows.get(8), new Object[]{8});
    assertEquals(rows.get(9), new Object[]{9});
  }

  @Test
  public void testSortReduceSortedAndUnsortedTablesDesc()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT col1 FROM testTable ORDER BY col1 DESC");
    DataSchema dataSchema =
        new DataSchema(new String[]{"col1"}, new ColumnDataType[]{ColumnDataType.INT});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    // sorted block
    int numGroups = 5;
    for (int i = 0; i < numGroups; i++) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, i * 2);
      dataTableBuilder.finishRow();
    }
    DataTable dataTable = dataTableBuilder.build();
    dataTable.getMetadata().put(DataTable.MetadataKey.ORDER_BY_EXPRESSIONS.getName(), "[col1 ASC]");
    // unsorted block
    DataTableBuilder unSortedDataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    int numUnsortedGroups = 5;
    for (int i = 0; i < numUnsortedGroups; i++) {
      unSortedDataTableBuilder.startRow();
      unSortedDataTableBuilder.setColumn(0, numUnsortedGroups + numGroups - i - 1);
      unSortedDataTableBuilder.finishRow();
    }
    DataTable unSortedDataTable = unSortedDataTableBuilder.build();
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    int numSortedInstances = 1;
    for (int i = 0; i < numSortedInstances; i++) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", i, TableType.OFFLINE);
      dataTableMap.put(instance, dataTable);
    }
    int numUnSortedInstances = 1;
    for (int i = 0; i < numUnSortedInstances; i++) {
      ServerRoutingInstance instance =
          new ServerRoutingInstance("localhost", i + numSortedInstances, TableType.OFFLINE);
      dataTableMap.put(instance, unSortedDataTable);
    }
    long reduceTimeoutMs = 100000;
    BrokerResponseNative brokerResponse =
        brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap, reduceTimeoutMs,
            mock(BrokerMetrics.class));
    brokerReduceService.shutDown();

    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 10);
    assertEquals(rows.get(0), new Object[]{9});
    assertEquals(rows.get(1), new Object[]{8});
    assertEquals(rows.get(2), new Object[]{8});
    assertEquals(rows.get(3), new Object[]{7});
    assertEquals(rows.get(4), new Object[]{6});
    assertEquals(rows.get(7), new Object[]{4});
    assertEquals(rows.get(8), new Object[]{2});
    assertEquals(rows.get(9), new Object[]{0});
  }

  @Test
  public void testSortReduceSortedAndUnsortedTablesDescNullHandlingEnabled()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest(
            "SET enableNullHandling=true; SELECT col1 FROM testTable ORDER BY col1 DESC");
    DataSchema dataSchema =
        new DataSchema(new String[]{"col1"}, new ColumnDataType[]{ColumnDataType.INT});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    // sorted block
    int numGroups = 5;
    for (int i = 0; i < numGroups; i++) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, i * 2);
      dataTableBuilder.finishRow();
    }
    DataTable dataTable = dataTableBuilder.build();
    dataTable.getMetadata().put(DataTable.MetadataKey.ORDER_BY_EXPRESSIONS.getName(), "[col1 ASC]");
    // unsorted block
    List<Object[]> unsortedRows = new ArrayList<>();
    int numUnsortedGroups = 4;
    for (int i = 0; i < numUnsortedGroups; i++) {
      Object[] row = new Object[1];
      row[0] = numUnsortedGroups + numGroups - i - 1;
      unsortedRows.add(row);
    }
    unsortedRows.add(new Object[]{null});

    DataTable unSortedDataTable = SelectionOperatorUtils.getDataTableFromRows(unsortedRows, dataSchema, true);
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    int numSortedInstances = 1;
    for (int i = 0; i < numSortedInstances; i++) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", i, TableType.OFFLINE);
      dataTableMap.put(instance, dataTable);
    }
    int numUnSortedInstances = 1;
    for (int i = 0; i < numUnSortedInstances; i++) {
      ServerRoutingInstance instance =
          new ServerRoutingInstance("localhost", i + numSortedInstances, TableType.OFFLINE);
      dataTableMap.put(instance, unSortedDataTable);
    }
    long reduceTimeoutMs = 100000;
    BrokerResponseNative brokerResponse =
        brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap, reduceTimeoutMs,
            mock(BrokerMetrics.class));
    brokerReduceService.shutDown();

    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 10);
    assertEquals(rows.get(0), new Object[]{null});
    assertEquals(rows.get(1), new Object[]{8});
    assertEquals(rows.get(2), new Object[]{8});
    assertEquals(rows.get(3), new Object[]{7});
    assertEquals(rows.get(4), new Object[]{6});
    assertEquals(rows.get(7), new Object[]{4});
    assertEquals(rows.get(8), new Object[]{2});
    assertEquals(rows.get(9), new Object[]{0});
  }
}
