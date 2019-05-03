/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pinot.thirdeye.cube.data.node;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.additive.AdditiveCubeNode;
import org.apache.pinot.thirdeye.cube.additive.AdditiveRow;
import org.apache.pinot.thirdeye.cube.data.dbrow.Row;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CubeNodeTest {

  // Since CubeNode has cyclic reference between current node and parent node, the toString() will encounter
  // overflowStack exception if it doesn't take care of the cyclic reference carefully.
  @Test
  public void testToString() {
    AdditiveRow root = new AdditiveRow(new Dimensions(), new DimensionValues());
    AdditiveCubeNode rootNode = new AdditiveCubeNode(root);

    AdditiveRow child = new AdditiveRow(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")), 20, 30);
    AdditiveCubeNode childNode = new AdditiveCubeNode(1, 0, child, rootNode);

    childNode.toString();
  }

  @Test
  public void testSimpleEquals() {
    AdditiveRow root1 = new AdditiveRow(new Dimensions(), new DimensionValues());
    AdditiveCubeNode rootNode1 = new AdditiveCubeNode(root1);

    AdditiveRow root2 = new AdditiveRow(new Dimensions(), new DimensionValues());
    AdditiveCubeNode rootNode2 = new AdditiveCubeNode(root2);

    Assert.assertEquals(rootNode1, rootNode2);
    Assert.assertTrue(CubeNodeUtils.equalHierarchy(rootNode1, rootNode2));

    AdditiveRow root3 = new AdditiveRow(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")));
    CubeNode rootNode3 = new AdditiveCubeNode(root3);
    Assert.assertNotEquals(rootNode1, rootNode3);
  }

  @Test
  public void testHierarchicalEquals() {
    AdditiveCubeNode rootNode1 = buildHierarchicalNodes();
    AdditiveCubeNode rootNode2 = buildHierarchicalNodes();

    Assert.assertTrue(CubeNodeUtils.equalHierarchy(rootNode1, rootNode2));
  }

  /**
   * Hierarchy 1:
   *      A
   *     / \
   *    B  C
   *
   * Hierarchy 2:
   *      A
   *
   * Failed because structure difference.
   */
  @Test
  public void testHierarchicalEqualsFail1() {
    AdditiveCubeNode rootNode1 = buildHierarchicalNodes();

    AdditiveRow rootRow = new AdditiveRow(new Dimensions(), new DimensionValues(), 30, 45);
    AdditiveCubeNode rootNode2 = new AdditiveCubeNode(rootRow);

    Assert.assertEquals(rootNode1, rootNode2);
    Assert.assertFalse(CubeNodeUtils.equalHierarchy(rootNode1, rootNode2));
  }

  /**
   * Hierarchy 1:
   *      A
   *     / \
   *    B  C
   *
   * Hierarchy 2:
   *      A'
   *
   * Failed because data difference.
   */
  @Test
  public void testHierarchicalEqualsFail2() throws Exception {
    AdditiveCubeNode rootNode1 = buildHierarchicalNodes();

    AdditiveRow rootRow = new AdditiveRow(new Dimensions(), new DimensionValues(), 20, 15);
    AdditiveCubeNode rootNode2 = new AdditiveCubeNode(rootRow);

    Assert.assertNotEquals(rootNode1, rootNode2);
    Assert.assertFalse(CubeNodeUtils.equalHierarchy(rootNode1, rootNode2));
  }

  /**
   * Hierarchy 1:
   *      A
   *     ^ ^
   *     / \
   *    v  v
   *    B  C
   *
   * Hierarchy 2:
   *      A
   *      ^
   *       \
   *       v
   *       C
   *
   * Failed because Hierarchy 2's A doesn't have a reference to B.
   */
  @Test
  public void testHierarchicalEqualsFail3() throws Exception {
    AdditiveCubeNode rootNode1 = buildHierarchicalNodes();

    List<List<Row>> rows = buildHierarchicalRows();
    // Root level
    AdditiveRow rootRow = (AdditiveRow) rows.get(0).get(0);
    AdditiveCubeNode rootNode2 = new AdditiveCubeNode(rootRow);

    // Level 1
    AdditiveRow INRow = (AdditiveRow) rows.get(1).get(1);
    CubeNode INNode = new AdditiveCubeNode(1, 1, INRow, rootNode2);

    Assert.assertEquals(rootNode1, rootNode2);
    Assert.assertFalse(CubeNodeUtils.equalHierarchy(rootNode1, rootNode2));
  }

  private List<List<Row>> buildHierarchicalRows() {
    List<List<Row>> hierarchicalRows = new ArrayList<>();
    // Root level
    List<Row> rootLevel = new ArrayList<>();
    rootLevel.add(new AdditiveRow(new Dimensions(), new DimensionValues(), 30, 45));
    hierarchicalRows.add(rootLevel);

    // Level 1
    List<Row> level1 = new ArrayList<>();
    Row row1 = new AdditiveRow(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")), 20, 30);
    level1.add(row1);

    Row row2 = new AdditiveRow(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("IN")), 10, 15);
    level1.add(row2);

    hierarchicalRows.add(level1);

    return hierarchicalRows;
  }

  private AdditiveCubeNode buildHierarchicalNodes() {
    List<List<Row>> rows = buildHierarchicalRows();
    // Root level
    AdditiveRow rootRow = (AdditiveRow) rows.get(0).get(0);
    AdditiveCubeNode rootNode = new AdditiveCubeNode(rootRow);

    // Level 1
    AdditiveRow USRow = (AdditiveRow) rows.get(1).get(0);
    CubeNode USNode = new AdditiveCubeNode(1, 0, USRow, rootNode);

    AdditiveRow INRow = (AdditiveRow) rows.get(1).get(1);
    CubeNode INNode = new AdditiveCubeNode(1, 1, INRow, rootNode);

    return rootNode;
  }
}
