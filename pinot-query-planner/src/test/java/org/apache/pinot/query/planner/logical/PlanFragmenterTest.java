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
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.KWayMergeExchangeNode;
import org.apache.pinot.query.planner.plannode.MailboxMergeReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Verifies fragmentation preserves the explicit ordered exchange contract without discovering leaf ordering.
public class PlanFragmenterTest {
  @Test
  public void shouldLowerOnlyExplicitMergeExchange() {
    DataSchema schema = new DataSchema(new String[]{"key"}, new ColumnDataType[]{ColumnDataType.INT});
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0));
    TableScanNode scan = new TableScanNode(0, schema, PlanNode.NodeHint.EMPTY, new ArrayList<>(), "table_OFFLINE",
        List.of("key"));
    SortNode sort =
        new SortNode(0, schema, PlanNode.NodeHint.EMPTY, new ArrayList<>(List.of(scan)), collations, 15, -1);
    for (boolean merge : new boolean[]{false, true}) {
      ExchangeNode exchange = merge
          ? new KWayMergeExchangeNode(0, schema, List.of(sort), RelDistribution.Type.HASH_DISTRIBUTED,
              List.of(), false, collations, 10, 5, "hashCode")
          : new ExchangeNode(0, schema, List.of(sort), PinotRelExchangeType.STREAMING,
              RelDistribution.Type.HASH_DISTRIBUTED, List.of(), false, collations, false, false, null, null,
              "hashCode");
      PlanFragmenter fragmenter = new PlanFragmenter();
      MailboxReceiveNode receive = (MailboxReceiveNode) exchange.visit(fragmenter, fragmenter.createContext());
      assertEquals(receive instanceof MailboxMergeReceiveNode, merge);
      assertFalse(receive.isSort());
      assertFalse(receive.isSortedOnSender());
      MailboxSendNode send = (MailboxSendNode) fragmenter.getPlanFragmentMap().get(2).getFragmentRoot();
      assertFalse(send.isSort());
      assertSame(send.getInputs().get(0), sort, "The logical sender sort must be reused");
      if (merge) {
        assertEquals(((MailboxMergeReceiveNode) receive).getFetch(), 10);
        assertEquals(((MailboxMergeReceiveNode) receive).getOffset(), 5);
        KWayMergeExchangeNode copy = (KWayMergeExchangeNode) exchange.withInputs(List.of(sort));
        assertEquals(copy.getFetch(), 10);
        assertEquals(copy.getOffset(), 5);
        assertTrue(copy.equals(exchange));
      }
    }
  }
}
