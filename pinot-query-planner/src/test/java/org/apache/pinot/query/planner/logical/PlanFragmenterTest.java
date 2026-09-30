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
package org.apache.pinot.query.planner.logical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;


/// Tests the physical-table and collation proof used to enable leaf ORDER BY merge receive.
public class PlanFragmenterTest {
  @DataProvider
  public Object[][] leafSortCases() {
    return new Object[][]{
        {"table", false, List.of("OFFLINE"), false, true, true, false},
        {"table", true, List.of("OFFLINE"), false, true, true, true},
        {"table", true, List.of("OFFLINE", "REALTIME"), false, true, true, false},
        {"table", true, List.of("OFFLINE"), true, true, true, false},
        {"table", true, List.of(), false, true, true, false},
        {"table", true, List.of("OFFLINE"), false, false, true, false},
        {"table", true, List.of("OFFLINE"), false, true, false, false},
        {"table_OFFLINE", true, List.of("OFFLINE", "REALTIME"), false, true, true, true}
    };
  }

  @Test(dataProvider = "leafSortCases")
  public void shouldMergeOnlyProvenPhysicalLeafSort(String tableName, boolean enabled, List<String> tableTypes,
      boolean logical, boolean cacheAvailable, boolean matchingCollation, boolean expected) {
    TableCache cache = mock(TableCache.class);
    when(cache.getActualTableName(tableName)).thenReturn(tableName);
    when(cache.isLogicalTable(tableName)).thenReturn(logical);
    for (String tableType : tableTypes) {
      when(cache.getTableConfig("table_" + tableType)).thenReturn(mock(TableConfig.class));
    }
    DataSchema schema = new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0));
    TableScanNode scan = new TableScanNode(0, schema, PlanNode.NodeHint.EMPTY, new ArrayList<>(), tableName,
        List.of("key"));
    SortNode sort = new SortNode(0, schema, PlanNode.NodeHint.EMPTY, new ArrayList<>(List.of(scan)), collations, 10,
        -1);
    ExchangeNode exchange = new ExchangeNode(0, schema, List.of(sort), PinotRelExchangeType.STREAMING,
        RelDistribution.Type.HASH_DISTRIBUTED, List.of(), false,
        matchingCollation ? collations : List.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
        false, false, null, null, null);
    PlanFragmenter fragmenter = new PlanFragmenter(enabled, cacheAvailable ? cache : null);
    MailboxReceiveNode receive = (MailboxReceiveNode) exchange.visit(fragmenter, fragmenter.createContext());
    assertEquals(receive.isSort(), expected);
    assertEquals(receive.isSortedOnSender(), expected);
    assertSame(fragmenter.getPlanFragmentMap().get(2).getFragmentRoot().getInputs().get(0), sort,
        "An existing leaf sort must be reused without an extra materializing sender sort");
  }
}
