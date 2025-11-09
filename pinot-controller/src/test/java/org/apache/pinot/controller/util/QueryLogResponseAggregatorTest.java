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
package org.apache.pinot.controller.util;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class QueryLogResponseAggregatorTest {

  @Test
  public void testAggregationMergesRowsAndStats() {
    DataSchema schema = new DataSchema(new String[] {"requestId", "timeMs"},
        new ColumnDataType[] {ColumnDataType.LONG, ColumnDataType.LONG});

    BrokerResponseNative broker1 = new BrokerResponseNative();
    broker1.setResultTable(new ResultTable(schema, Collections.singletonList(new Object[] {1L, 10L})));
    broker1.setNumDocsScanned(5);
    broker1.setNumEntriesScannedInFilter(1);
    broker1.setNumEntriesScannedPostFilter(2);
    broker1.setTimeUsedMs(15);

    BrokerResponseNative broker2 = new BrokerResponseNative();
    broker2.setResultTable(new ResultTable(schema, Collections.singletonList(new Object[] {2L, 20L})));
    broker2.setNumDocsScanned(7);
    broker2.setNumEntriesScannedInFilter(3);
    broker2.setNumEntriesScannedPostFilter(4);
    broker2.setTimeUsedMs(25);
    broker2.setExceptions(List.of(new QueryProcessingException(500, "broker error")));

    BrokerResponseNative aggregated = QueryLogResponseAggregator.aggregate(List.of(broker1, broker2));

    assertNotNull(aggregated.getResultTable());
    assertEquals(aggregated.getResultTable().getRows().size(), 2);
    assertEquals(aggregated.getNumDocsScanned(), 12L);
    assertEquals(aggregated.getNumEntriesScannedInFilter(), 4L);
    assertEquals(aggregated.getNumEntriesScannedPostFilter(), 6L);
    assertEquals(aggregated.getTimeUsedMs(), 25L);
    assertEquals(aggregated.getNumServersResponded(), 2);
    assertEquals(aggregated.getExceptions().size(), 1);
    assertTrue(aggregated.getTablesQueried().contains("system.query_log"));
  }

  @Test
  public void testAggregationHandlesSchemaMismatch() {
    DataSchema schema1 = new DataSchema(new String[] {"requestId"}, new ColumnDataType[] {ColumnDataType.LONG});
    DataSchema schema2 = new DataSchema(new String[] {"requestId"}, new ColumnDataType[] {ColumnDataType.STRING});

    BrokerResponseNative broker1 = new BrokerResponseNative();
    broker1.setResultTable(new ResultTable(schema1, Collections.singletonList(new Object[] {1L})));

    BrokerResponseNative broker2 = new BrokerResponseNative();
    broker2.setResultTable(new ResultTable(schema2, Collections.singletonList(new Object[] {"incompatible"})));

    BrokerResponseNative aggregated = QueryLogResponseAggregator.aggregate(List.of(broker1, broker2));

    assertNotNull(aggregated.getResultTable());
    assertEquals(aggregated.getResultTable().getRows().size(), 1);
    assertEquals(aggregated.getExceptions().size(), 1);
  }
}
