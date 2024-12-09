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
package org.apache.pinot.tsdb.planner;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TimeSeriesPlanFragmenterTest {
  @Test
  public void testGetFragmentsWithMultipleLeafNodes() {
    /*
     * Create Input:
     *         Node-1
     *        /     \
     *      Node-2  Leaf-2
     *       /
     *     Leaf-1
     * Expected Outputs:
     *   Fragment-1:
     *         Node-1
     *        /     \
     *      Node-2  Exchange (named Leaf-2)
     *       /
     *     Exchange (named Leaf-1)
     *   Fragment-2:
     *         Leaf-1
     *   Fragment-3:
     *         Leaf-2
     */
    LeafTimeSeriesPlanNode leafOne = createMockLeafNode("Leaf-1");
    LeafTimeSeriesPlanNode leafTwo = createMockLeafNode("Leaf-2");
    BaseTimeSeriesPlanNode nodeTwo = new MockTimeSeriesPlanNode("Node-2", Collections.singletonList(leafOne));
    BaseTimeSeriesPlanNode nodeOne = new MockTimeSeriesPlanNode("Node-1", ImmutableList.of(nodeTwo, leafTwo));
    List<BaseTimeSeriesPlanNode> fragments = TimeSeriesPlanFragmenter.getFragments(nodeOne, false);
    // Test whether correct number of fragments generated
    assertEquals(fragments.size(), 3);
    // Test whether fragment roots are correct
    assertEquals(fragments.get(0).getId(), "Node-1");
    assertEquals(fragments.get(1).getId(), "Leaf-1");
    assertEquals(fragments.get(2).getId(), "Leaf-2");
    // Test whether broker fragment has the right inputs
    {
      BaseTimeSeriesPlanNode brokerFragment = fragments.get(0);
      assertEquals(brokerFragment.getInputs().size(), 2);
      // Left and right inputs should have IDs Node-2 and Leaf-2.
      BaseTimeSeriesPlanNode leftInput = brokerFragment.getInputs().get(0);
      BaseTimeSeriesPlanNode rightInput = brokerFragment.getInputs().get(1);
      assertEquals(leftInput.getId(), "Node-2");
      assertEquals(rightInput.getId(), "Leaf-2");
      // Right input should be exchange
      assertTrue(rightInput instanceof TimeSeriesExchangeNode, "Node should have been replaced by Exchange");
      // Input for Left input should be exchange
      assertEquals(leftInput.getInputs().size(), 1);
      assertEquals(leftInput.getInputs().get(0).getId(), "Leaf-1");
      assertTrue(leftInput.getInputs().get(0) instanceof TimeSeriesExchangeNode);
    }
    // Test the other two fragments
    assertTrue(fragments.get(1) instanceof LeafTimeSeriesPlanNode, "Expected leaf node in fragment");
    assertTrue(fragments.get(2) instanceof LeafTimeSeriesPlanNode, "Expected leaf node in fragment");
  }

  @Test
  public void testGetFragmentsForSingleServerQuery() {
    /*
     * Create Input:
     *         Node-1
     *        /     \
     *      Node-2  Leaf-2
     *       /
     *     Leaf-1
     * Expected Outputs:
     *   Fragment-1:
     *         Node-1
     *        /     \
     *      Node-2  Exchange (named Leaf-2)
     *       /
     *     Exchange (named Leaf-1)
     *   Fragment-2:
     *         Leaf-1
     *   Fragment-3:
     *         Leaf-2
     */
    LeafTimeSeriesPlanNode leafOne = createMockLeafNode("Leaf-1");
    LeafTimeSeriesPlanNode leafTwo = createMockLeafNode("Leaf-2");
    BaseTimeSeriesPlanNode nodeTwo = new MockTimeSeriesPlanNode("Node-2", Collections.singletonList(leafOne));
    BaseTimeSeriesPlanNode nodeOne = new MockTimeSeriesPlanNode("Node-1", ImmutableList.of(nodeTwo, leafTwo));
    List<BaseTimeSeriesPlanNode> fragments = TimeSeriesPlanFragmenter.getFragments(nodeOne, true);
    assertEquals(fragments.size(), 2, "Expect only 2 fragments for single-server query");
    assertEquals(fragments.get(0).getId(), "Node-1");
    assertEquals(fragments.get(1), nodeOne);
  }

  @Test
  public void testGetFragmentsWithSinglePlanNode() {
    /*
     * Create Input:
     *         Leaf-1
     * Expected Outputs:
     *   Fragment-1:
     *       Exchange (named Leaf-1)
     *   Fragment-2:
     *         Leaf-1
     */
    LeafTimeSeriesPlanNode leafOne = createMockLeafNode("Leaf-1");
    List<BaseTimeSeriesPlanNode> fragments = TimeSeriesPlanFragmenter.getFragments(leafOne, false);
    assertEquals(fragments.size(), 2);
    assertTrue(fragments.get(0) instanceof TimeSeriesExchangeNode);
    assertTrue(fragments.get(1) instanceof LeafTimeSeriesPlanNode);
    assertEquals(fragments.get(0).getId(), fragments.get(1).getId());
  }

  private LeafTimeSeriesPlanNode createMockLeafNode(String id) {
    return new LeafTimeSeriesPlanNode(id, Collections.emptyList(), "someTableName", "someTimeColumn",
        TimeUnit.SECONDS, 0L, "", "", null, Collections.emptyList());
  }

  static class MockTimeSeriesPlanNode extends BaseTimeSeriesPlanNode {
    public MockTimeSeriesPlanNode(String id, List<BaseTimeSeriesPlanNode> inputs) {
      super(id, inputs);
    }

    @Override
    public BaseTimeSeriesPlanNode withInputs(List<BaseTimeSeriesPlanNode> newInputs) {
      return new MockTimeSeriesPlanNode(_id, newInputs);
    }

    @Override
    public String getKlass() {
      return "";
    }

    @Override
    public String getExplainName() {
      return "";
    }

    @Override
    public BaseTimeSeriesOperator run() {
      return null;
    }
  }
}
