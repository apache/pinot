package com.linkedin.thirdeye.client.diffsummary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CubeTest {

  @Test
  public void testHierarchyRowToHierarchyNode() {
    List<List<Row>> rows = buildHierarchicalRows();
    List<List<HierarchyNode>> actualNodes =
        Cube.hierarchyRowToHierarchyNode(rows, new Dimensions(Arrays.asList("country", "page")));

    List<List<HierarchyNode>> expectedNodes = expectedHierarchicalNodes();

    // Test if the data is current; the reference (i.e., tree structure is not tested)
    Assert.assertEquals(actualNodes, expectedNodes);

    // Test the structure of the hierarchy
    Assert.assertTrue(HierarchyNode.equalHierarchy(actualNodes.get(0).get(0), expectedNodes.get(0).get(0)));
  }

  private List<List<Row>> buildHierarchicalRows() {
    List<List<Row>> hierarchicalRows = new ArrayList<>();
    // Root level
    {
      List<Row> rootLevel = new ArrayList<>();
      rootLevel.add(new Row(new Dimensions(), new DimensionValues(), 30, 45));
      hierarchicalRows.add(rootLevel);
    }
    // Level 1
    {
      List<Row> level1 = new ArrayList<>();
      Row row1 = new Row(new Dimensions(Collections.singletonList("country")),
          new DimensionValues(Collections.singletonList("US")), 20, 30);
      level1.add(row1);

      Row row2 = new Row(new Dimensions(Collections.singletonList("country")),
          new DimensionValues(Collections.singletonList("IN")), 10, 15);
      level1.add(row2);

      hierarchicalRows.add(level1);
    }
    // Level 2
    {
      List<Row> level2 = new ArrayList<>();
      Row row1 =
          new Row(new Dimensions(Arrays.asList("country", "page")), new DimensionValues(Arrays.asList("US", "page1")),
              8, 10);
      level2.add(row1);

      Row row2 =
          new Row(new Dimensions(Arrays.asList("country", "page")), new DimensionValues(Arrays.asList("US", "page2")),
              12, 20);
      level2.add(row2);

      Row row3 =
          new Row(new Dimensions(Arrays.asList("country", "page")), new DimensionValues(Arrays.asList("IN", "page1")),
              10, 15);
      level2.add(row3);

      hierarchicalRows.add(level2);
    }
    return hierarchicalRows;
  }

  private List<List<HierarchyNode>> expectedHierarchicalNodes() {
    List<List<Row>> rows = buildHierarchicalRows();
    List<List<HierarchyNode>> hierarchicalNodes = new ArrayList<>();
    // Root level
    List<HierarchyNode> rootLevel = new ArrayList<>();
    hierarchicalNodes.add(rootLevel);

    Row rootRow = rows.get(0).get(0);
    HierarchyNode rootNode = new HierarchyNode(rootRow);
    rootLevel.add(rootNode);

    // Level 1
    List<HierarchyNode> level1 = new ArrayList<>();
    hierarchicalNodes.add(level1);

    Row USRow = rows.get(1).get(0);
    HierarchyNode USNode = new HierarchyNode(1, 0, USRow, rootNode);
    level1.add(USNode);

    Row INRow = rows.get(1).get(1);
    HierarchyNode INNode = new HierarchyNode(1, 1, INRow, rootNode);
    level1.add(INNode);

    // Level 2
    List<HierarchyNode> level2 = new ArrayList<>();
    hierarchicalNodes.add(level2);

    Row USPage1Row = rows.get(2).get(0);
    HierarchyNode USPage1Node = new HierarchyNode(2, 0, USPage1Row, USNode);
    level2.add(USPage1Node);

    Row USPage2Row = rows.get(2).get(1);
    HierarchyNode USPage2Node = new HierarchyNode(2, 1, USPage2Row, USNode);
    level2.add(USPage2Node);

    Row INPage1Row = rows.get(2).get(2);
    HierarchyNode INPage1Node = new HierarchyNode(2, 2, INPage1Row, INNode);
    level2.add(INPage1Node);

    return hierarchicalNodes;
  }
}
