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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseErrorMessage;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
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

    List<BrokerResponseErrorMessage> exceptions = brokerResponse.getExceptions();
    assertEquals(exceptions.size(), 1);
    assertEquals(exceptions.get(0).getErrorCode(), QueryErrorCode.BROKER_TIMEOUT.getId());
  }
}
