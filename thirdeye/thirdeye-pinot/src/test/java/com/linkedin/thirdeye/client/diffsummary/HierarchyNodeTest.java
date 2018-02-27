package com.linkedin.thirdeye.client.diffsummary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class HierarchyNodeTest {

  // Since HierarchyNode has cyclic reference between current node and parent node, the toString() will encounter
  // overflowStack exception if it doesn't take care of the cyclic reference carefully.
  @Test
  public void testToString() {
    Row root = new Row(new Dimensions(), new DimensionValues());
    HierarchyNode rootNode = new HierarchyNode(root);

    Row child = new Row(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")), 20, 30);
    HierarchyNode childNode = new HierarchyNode(1, 0, child, rootNode);

    childNode.toString();
  }

  @Test
  public void testSimpleEquals() throws Exception {
    Row root1 = new Row(new Dimensions(), new DimensionValues());
    HierarchyNode rootNode1 = new HierarchyNode(root1);

    Row root2 = new Row(new Dimensions(), new DimensionValues());
    HierarchyNode rootNode2 = new HierarchyNode(root2);

    Assert.assertEquals(rootNode1, rootNode2);
    Assert.assertTrue(HierarchyNode.equalHierarchy(rootNode1, rootNode2));

    Row root3 = new Row(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")));
    HierarchyNode rootNode3 = new HierarchyNode(root3);
    Assert.assertNotEquals(rootNode1, rootNode3);
  }

  @Test
  public void testHierarchicalEquals() throws Exception {
    HierarchyNode rootNode1 = buildHierarchicalNodes();
    HierarchyNode rootNode2 = buildHierarchicalNodes();

    Assert.assertTrue(HierarchyNode.equalHierarchy(rootNode1, rootNode2));
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
    HierarchyNode rootNode1 = buildHierarchicalNodes();

    Row rootRow = new Row(new Dimensions(), new DimensionValues(), 30, 45);
    HierarchyNode rootNode2 = new HierarchyNode(rootRow);

    Assert.assertEquals(rootNode1, rootNode2);
    Assert.assertFalse(HierarchyNode.equalHierarchy(rootNode1, rootNode2));
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
    HierarchyNode rootNode1 = buildHierarchicalNodes();

    Row rootRow = new Row(new Dimensions(), new DimensionValues(), 20, 15);
    HierarchyNode rootNode2 = new HierarchyNode(rootRow);

    Assert.assertNotEquals(rootNode1, rootNode2);
    Assert.assertFalse(HierarchyNode.equalHierarchy(rootNode1, rootNode2));
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
    HierarchyNode rootNode1 = buildHierarchicalNodes();

    List<List<Row>> rows = buildHierarchicalRows();
    // Root level
    Row rootRow = rows.get(0).get(0);
    HierarchyNode rootNode2 = new HierarchyNode(rootRow);

    // Level 1
    Row INRow = rows.get(1).get(1);
    HierarchyNode INNode = new HierarchyNode(1, 1, INRow, rootNode2);

    Assert.assertEquals(rootNode1, rootNode2);
    Assert.assertFalse(HierarchyNode.equalHierarchy(rootNode1, rootNode2));
  }

  private List<List<Row>> buildHierarchicalRows() {
    List<List<Row>> hierarchicalRows = new ArrayList<>();
    // Root level
    List<Row> rootLevel = new ArrayList<>();
    rootLevel.add(new Row(new Dimensions(), new DimensionValues(), 30, 45));
    hierarchicalRows.add(rootLevel);

    // Level 1
    List<Row> level1 = new ArrayList<>();
    Row row1 = new Row(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("US")), 20, 30);
    level1.add(row1);

    Row row2 = new Row(new Dimensions(Collections.singletonList("country")),
        new DimensionValues(Collections.singletonList("IN")), 10, 15);
    level1.add(row2);

    hierarchicalRows.add(level1);

    return hierarchicalRows;
  }

  private HierarchyNode buildHierarchicalNodes() {
    List<List<Row>> rows = buildHierarchicalRows();
    // Root level
    Row rootRow = rows.get(0).get(0);
    HierarchyNode rootNode = new HierarchyNode(rootRow);

    // Level 1
    Row USRow = rows.get(1).get(0);
    HierarchyNode USNode = new HierarchyNode(1, 0, USRow, rootNode);

    Row INRow = rows.get(1).get(1);
    HierarchyNode INNode = new HierarchyNode(1, 1, INRow, rootNode);

    return rootNode;
  }
}
