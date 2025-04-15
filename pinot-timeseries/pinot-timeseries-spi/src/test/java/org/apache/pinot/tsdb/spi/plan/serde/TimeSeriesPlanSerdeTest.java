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
package org.apache.pinot.tsdb.spi.plan.serde;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class TimeSeriesPlanSerdeTest {
  private static final int SERIES_LIMIT = 1000;
  private static final Map<String, String> QUERY_OPTIONS = ImmutableMap.of("numGroupsLimit", "1000");

  @Test
  public void testSerdeForScanFilterProjectNode() {
    Map<String, String> aggParams = new HashMap<>();
    aggParams.put("window", "5m");
    // create leaf node
    LeafTimeSeriesPlanNode leafTimeSeriesPlanNode =
        new LeafTimeSeriesPlanNode("sfp#0", new ArrayList<>(), "myTable", "myTimeColumn", TimeUnit.MILLISECONDS, 0L,
            "myFilterExpression", "myValueExpression", new AggInfo("SUM", false, aggParams), new ArrayList<>(),
            SERIES_LIMIT, QUERY_OPTIONS);
    // serialize and deserialize to re-create another node
    BaseTimeSeriesPlanNode planNode =
        TimeSeriesPlanSerde.deserialize(TimeSeriesPlanSerde.serialize(leafTimeSeriesPlanNode));
    assertTrue(planNode instanceof LeafTimeSeriesPlanNode);
    LeafTimeSeriesPlanNode deserializedNode = (LeafTimeSeriesPlanNode) planNode;
    // assert that deserialized node is same as serialized node
    assertEquals(deserializedNode.getTableName(), "myTable");
    assertEquals(deserializedNode.getTimeColumn(), "myTimeColumn");
    assertEquals(deserializedNode.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(deserializedNode.getOffsetSeconds(), 0L);
    assertEquals(deserializedNode.getFilterExpression(), "myFilterExpression");
    assertEquals(deserializedNode.getValueExpression(), "myValueExpression");
    assertNotNull(deserializedNode.getAggInfo());
    assertFalse(deserializedNode.getAggInfo().getIsPartial());
    assertNotNull(deserializedNode.getAggInfo().getParams());
    assertEquals(deserializedNode.getAggInfo().getParams().get("window"), "5m");
    assertEquals(deserializedNode.getGroupByExpressions().size(), 0);
    assertEquals(deserializedNode.getLimit(), SERIES_LIMIT);
    assertEquals(deserializedNode.getQueryOptions(), QUERY_OPTIONS);
  }
}
