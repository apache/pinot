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
package org.apache.pinot.common.response;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class LazyToEagerBrokerResponseAdaptorTest {

  @Test
  public void testOfConsumesAllRows() {
    DataSchema dataSchema = new DataSchema(
        new String[]{"id", "name"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1, "one"});
    rows.add(new Object[]{2, "two"});

    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, new StreamingBrokerResponse.Metainfo.Error(List.of()), rows);

    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(streaming);
    ResultTable resultTable = eager.getResultTable();
    assertNotNull(resultTable);
    assertEquals(resultTable.getRows().size(), 2);
    assertEquals(resultTable.getRows().get(0)[0], 1);
    assertEquals(resultTable.getRows().get(0)[1], "one");
    assertEquals(resultTable.getRows().get(1)[0], 2);
    assertEquals(resultTable.getRows().get(1)[1], "two");
  }

  @Test
  public void testSetRequestId() {
    DataSchema dataSchema = new DataSchema(
        new String[]{"id"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}
    );
    List<Object[]> rows = new ArrayList<>();
    rows.add(new Object[]{1});
    StreamingBrokerResponse streaming = new StreamingBrokerResponse.ListStreamingBrokerResponse(
        dataSchema, new StreamingBrokerResponse.Metainfo.Error(List.of()), rows);
    BrokerResponse eager = LazyToEagerBrokerResponseAdaptor.of(streaming);
    eager.setRequestId("cursor-request-id");
    assertEquals(eager.getRequestId(), "cursor-request-id");
  }
}
