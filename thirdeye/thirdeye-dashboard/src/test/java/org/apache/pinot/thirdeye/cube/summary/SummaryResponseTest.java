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

package org.apache.pinot.thirdeye.cube.summary;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.thirdeye.cube.additive.AdditiveCubeNode;
import org.apache.pinot.thirdeye.cube.additive.AdditiveRow;
import org.apache.pinot.thirdeye.cube.cost.BalancedCostFunction;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.dbrow.Row;
import org.apache.pinot.thirdeye.cube.data.node.CubeNode;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.cube.summary.SummaryResponse.*;


public class SummaryResponseTest {
  private static double EPSILON = 0.0001d;

  @Test
  public void testBuildDiffSummary() {
    // Create test case
    List<CubeNode> cubeNodes = buildHierarchicalNodes();
    int rootIdx = cubeNodes.size() - 1;
    double baselineTotal = cubeNodes.get(rootIdx).getOriginalBaselineValue();
    double currentTotal = cubeNodes.get(rootIdx).getOriginalCurrentValue();
    double baselineSize = cubeNodes.get(rootIdx).getOriginalBaselineSize();
    double currentSize = cubeNodes.get(rootIdx).getOriginalCurrentSize();
    // Build the response
    SummaryResponse response = new SummaryResponse(baselineTotal, currentTotal, baselineSize, currentSize);
    response.buildDiffSummary(cubeNodes, 2, new BalancedCostFunction());
    response.setMetricUrn("testMetricUrn");

    // Validation
    List<SummaryResponseRow> responseRows = response.getResponseRows();
    Assert.assertEquals(responseRows.size(), 2); // Our test summary contains only root (OTHER) and US node.
    List<SummaryResponseRow> expectedResponseRows = buildExpectedResponseRows();
    for (int i = 0; i < expectedResponseRows.size(); ++i) {
      SummaryResponseRow actualRow = responseRows.get(i);
      SummaryResponseRow expectedRow = expectedResponseRows.get(i);
      Assert.assertEquals(actualRow.names, expectedRow.names);
      Assert.assertEquals(actualRow.otherDimensionValues, expectedRow.otherDimensionValues);
      Assert.assertEquals(actualRow.cost, expectedRow.cost, EPSILON);
      Assert.assertEquals(actualRow.baselineValue, expectedRow.baselineValue);
      Assert.assertEquals(actualRow.currentValue, expectedRow.currentValue);
      Assert.assertEquals(Double.parseDouble(actualRow.percentageChange.split("%")[0]), Double.parseDouble(expectedRow.percentageChange.split("%")[0]));
      Assert.assertEquals(actualRow.sizeFactor, expectedRow.sizeFactor, EPSILON);
    }
  }

  /**
   * Provides data for this hierarchy:
   *       root
   *     /  |  \
   *    US  IN  FR
   */
  private List<List<Row>> buildHierarchicalRows() {
    List<List<Row>> hierarchicalRows = new ArrayList<>();
    List<String> dimensions = Collections.singletonList("country");

    // Root level
    List<Row> rootLevel = new ArrayList<>();
    rootLevel.add(new AdditiveRow(new Dimensions(dimensions), new DimensionValues(), 45, 58));
    hierarchicalRows.add(rootLevel);

    // Level 1
    List<Row> level1 = new ArrayList<>();
    Row row1 =
        new AdditiveRow(new Dimensions(dimensions), new DimensionValues(Collections.singletonList("US")), 20, 30);
    level1.add(row1);

    Row row2 =
        new AdditiveRow(new Dimensions(dimensions), new DimensionValues(Collections.singletonList("IN")), 10, 11);
    level1.add(row2);

    Row row3 =
        new AdditiveRow(new Dimensions(dimensions), new DimensionValues(Collections.singletonList("FR")), 15, 17);
    level1.add(row3);

    hierarchicalRows.add(level1);

    return hierarchicalRows;
  }

  /**
   * Builds hierarchy:
   *      root (IN, FR)
   *     /
   *    US
   */
  private List<CubeNode> buildHierarchicalNodes() {
    List<List<Row>> rows = buildHierarchicalRows();
    // Root level
    AdditiveRow rootRow = (AdditiveRow) rows.get(0).get(0);
    AdditiveCubeNode rootNode = new AdditiveCubeNode(rootRow);

    // Level 1
    AdditiveRow USRow = (AdditiveRow) rows.get(1).get(0);
    AdditiveCubeNode USNode = new AdditiveCubeNode(1, 0, USRow, rootNode);

    AdditiveRow INRow = (AdditiveRow) rows.get(1).get(1);
    AdditiveCubeNode INNode = new AdditiveCubeNode(1, 1, INRow, rootNode);

    AdditiveRow FRRow = (AdditiveRow) rows.get(1).get(2);
    AdditiveCubeNode FRNode = new AdditiveCubeNode(1, 2, FRRow, rootNode);

    // Assume that US is the only child that is picked by the summary
    rootNode.removeNodeValues(USNode);

    List<CubeNode> res = new ArrayList<>();
    res.add(USNode);
    // Root node is located at the end of this list.
    res.add(rootNode);

    return res;
  }

  /**
   * Builds expected hierarchy:
   *      root (IN, FR)
   *     /
   *    US
   */
  private List<SummaryResponseRow> buildExpectedResponseRows() {
    SummaryResponseRow root = new SummaryResponseRow();
    root.names = Collections.singletonList(NOT_ALL);
    root.otherDimensionValues = "IN, FR";
    root.cost = 0d; // root doesn't have cost
    root.baselineValue = 25d;
    root.currentValue = 28d;
    root.sizeFactor = 0.5145d;
    root.percentageChange = (28d - 25d) / 25d * 100 + "%";

    SummaryResponseRow US = new SummaryResponseRow();
    US.names = Collections.singletonList("US");
    US.otherDimensionValues = "";
    US.cost = 1.1587d;
    US.baselineValue = 20d;
    US.currentValue = 30d;
    US.sizeFactor = 0.4854d; // UPDATE THIS
    US.percentageChange = (30d - 20d) / 20d * 100 + "%";

    List<SummaryResponseRow> rows = new ArrayList<>();
    rows.add(root);
    rows.add(US);
    return rows;
  }
}
