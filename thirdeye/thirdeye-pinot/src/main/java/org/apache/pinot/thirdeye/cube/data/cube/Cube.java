/*
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

package org.apache.pinot.thirdeye.cube.data.cube;

import com.google.common.base.Preconditions;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.anomaly.utils.ThirdeyeMetricsUtil;
import org.apache.pinot.thirdeye.cube.additive.AdditiveDBClient;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.cost.CostFunction;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.pinot.thirdeye.cube.data.dbclient.CubeClient;
import org.apache.pinot.thirdeye.cube.data.node.CubeNode;
import org.apache.pinot.thirdeye.cube.data.dbrow.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;


@JsonIgnoreProperties(ignoreUnknown = true)
public class Cube { // the cube (Ca|Cb)
  private static final Logger LOG = LoggerFactory.getLogger(Cube.class);

  private static final int DEFAULT_DEPTH = 3;
  private static final int TOP_COST_ENTRIES_TO_LOG = 20;

  // Node value
  private double baselineTotal;
  private double currentTotal;
  // Node size
  private double baselineTotalSize;
  private double currentTotalSize;

  private List<DimNameValueCostEntry> costSet = Collections.emptyList();
  private List<DimensionCost> sortedDimensionCosts = Collections.emptyList();

  @JsonProperty("dimensions")
  private Dimensions dimensions;

  // The actual data is stored in levels
  @JsonProperty("hierarchicalRows")
  private List<List<Row>> hierarchicalRows = new ArrayList<>();

  // The logical nodes of the hierarchy among the actual data
  @JsonIgnore
  private List<List<CubeNode>> hierarchicalNodes = new ArrayList<>();

  public double getBaselineTotal() {
    return baselineTotal;
  }

  public double getCurrentTotal() {
    return currentTotal;
  }

  public double getBaselineTotalSize() {
    return baselineTotalSize;
  }

  public double getCurrentTotalSize() {
    return currentTotalSize;
  }

  public Dimensions getDimensions() {
    return dimensions;
  }

  private CostFunction costFunction;

  public Cube(CostFunction costFunction) {
    this.costFunction = costFunction;
  }

  @JsonIgnore
  public CubeNode getRoot() {
    if (hierarchicalNodes.size() != 0 && hierarchicalNodes.get(0).size() != 0) {
      return hierarchicalNodes.get(0).get(0);
    } else {
      return null;
    }
  }

  public List<DimNameValueCostEntry> getCostSet() {
    return costSet;
  }

  public List<DimensionCost> getSortedDimensionCosts() {
    return sortedDimensionCosts;
  }

  /**
   * Automatically orders of the given dimensions depending on their error cost and builds the subcube of data according
   * to that order.
   *
   * @param olapClient the client to retrieve the data.
   * @param dimensions the dimensions to be ordered.
   * @param dataFilter the filter to be applied on the incoming data.
   * @param depth the number of the top dimensions to be considered in the subcube.
   * @param hierarchy the hierarchy among the given dimensions, whose order will be honors before dimensions' cost.
   */
  public void buildWithAutoDimensionOrder(CubeClient olapClient,
      Dimensions dimensions, Multimap<String, String> dataFilter, int depth, List<List<String>> hierarchy)
      throws Exception {
    long tStart = System.nanoTime();
    try {
      Preconditions.checkArgument((dimensions != null && dimensions.size() != 0), "Dimensions cannot be empty.");
      Preconditions.checkNotNull(hierarchy, "hierarchy cannot be null.");

      initializeBasicInfo(olapClient, dataFilter);
      Dimensions shrankDimensions = CubeUtils.shrinkDimensionsByFilterSets(dimensions, dataFilter);
      costSet = computeOneDimensionCost(olapClient, baselineTotal, currentTotal, baselineTotalSize, currentTotalSize,
          shrankDimensions, dataFilter);
      sortedDimensionCosts = calculateSortedDimensionCost(costSet);
      this.dimensions = sortDimensions(sortedDimensionCosts, depth, hierarchy);

      LOG.info("Auto-dimension order: " + this.dimensions);

      buildSubCube(olapClient, this.dimensions, dataFilter);

    } catch (Exception e) {
      ThirdeyeMetricsUtil.cubeExceptionCounter.inc();
      throw e;

    } finally {
      ThirdeyeMetricsUtil.cubeCallCounter.inc();
      ThirdeyeMetricsUtil.cubeDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  /**
   * Builds the subcube of data according to the given dimensions.
   *
   * @param olapClient the client to retrieve the data.
   * @param dimensions the dimensions, whose order has been given, of the subcube.
   * @param dataFilter the filter to be applied on the incoming data.
   */
  public void buildWithManualDimensionOrder(CubeClient olapClient, Dimensions dimensions,
      Multimap<String, String> dataFilter)
      throws Exception {
    long tStart = System.nanoTime();
    try {
      buildSubCube(olapClient, dimensions, dataFilter);
    } catch (Exception e) {
      ThirdeyeMetricsUtil.cubeExceptionCounter.inc();
      throw e;
    } finally {
      ThirdeyeMetricsUtil.cubeCallCounter.inc();
      ThirdeyeMetricsUtil.cubeDurationCounter.inc(System.nanoTime() - tStart);
    }
  }

  /**
   * Builds the subcube according to the given dimension order.
   *
   * @param olapClient the data client to retrieve data.
   * @param dimensions the given dimension order.
   * @param dataFilter the data filter to applied on the data cube.
   */
  private void buildSubCube(CubeClient olapClient, Dimensions dimensions, Multimap<String, String> dataFilter)
      throws Exception {
    Preconditions.checkArgument((dimensions != null && dimensions.size() != 0), "Dimensions cannot be empty.");
    if (this.dimensions == null) { // which means buildWithAutoDimensionOrder is not triggered
      initializeBasicInfo(olapClient, dataFilter);
      this.dimensions = dimensions;
      costSet = computeOneDimensionCost(olapClient, baselineTotal, currentTotal, baselineTotalSize, currentTotalSize,
          dimensions, dataFilter);
    }

    int size = 0;
    // Get the rows at each level and sort them in the post-order of their hierarchical relationship,
    // in which a parent row aggregates the details rows under it. For instance, in the following
    // hierarchy row b aggregates rows d and e, and row a aggregates rows b and c.
    //     Level 0              a
    //                         / \
    //     Level 1            b   c
    //                       / \   \
    //     Level 2          d   e   f
    // The Comparator for generating the order is implemented in the class DimensionValues.
    List<List<Row>> rowOfLevels = olapClient.getAggregatedValuesOfLevels(dimensions, dataFilter);
    for (int i = 0; i <= dimensions.size(); ++i) {
      List<Row> rowAtLevelI = rowOfLevels.get(i);
      rowAtLevelI.sort(new RowDimensionValuesComparator());
      hierarchicalRows.add(rowAtLevelI);
      size += rowAtLevelI.size();
    }
    LOG.info("Size of the cube for generating summary: " + size);

    hierarchicalNodes = dataRowToCubeNode(hierarchicalRows, dimensions);
  }

  /**
   * Calculate the change changeRatio of the top aggregated values.
   * @throws Exception An exception is thrown if OLAP database cannot be connected.
   */
  private void initializeBasicInfo(CubeClient olapClient, Multimap<String, String> filterSets)
      throws Exception {

    String metric = null;
    if (olapClient instanceof AdditiveDBClient) {
      metric = ((AdditiveDBClient)olapClient).getMetric();
      filterSets.put("metric", metric);
    }
    Row topAggValues = olapClient.getTopAggregatedValues(filterSets);
    CubeNode node = topAggValues.toNode();

    baselineTotal =  node.getBaselineValue();
    currentTotal = node.getCurrentValue();

    baselineTotalSize = node.getBaselineSize();
    currentTotalSize = node.getCurrentSize();
  }

  /**
   * Sort the rows in the post-order of their hierarchical relationship
   */
  static class RowDimensionValuesComparator implements Comparator<Row> {
    @Override
    public int compare(Row r1, Row r2) {
      return r1.getDimensionValues().compareTo(r2.getDimensionValues());
    }
  }

  /**
   * Establishes the hierarchical relationship between the aggregated data (parent) and detailed data (children).
   *
   * @param dataRows the actual data.
   * @param dimensions the dimension names of the actual data.
   *
   * @return CubeNode that contains the hierarchical relationship.
   */
  public static List<List<CubeNode>> dataRowToCubeNode(List<List<Row>> dataRows, Dimensions dimensions) {

    List<List<CubeNode>> hierarchicalNodes = new ArrayList<>();
    HashMap<String, CubeNode> curParent = new HashMap<>();
    HashMap<String, CubeNode> nextParent = new HashMap<>();

    for (int level = 0; level <= dimensions.size(); ++level) {
      hierarchicalNodes.add(new ArrayList<>(dataRows.get(level).size()));

      if (level != 0) {
        for (int index = 0; index < dataRows.get(level).size(); ++index) {
          Row row = dataRows.get(level).get(index);
          StringBuilder parentDimValues = new StringBuilder();
          for (int i = 0; i < level - 1; ++i) {
            parentDimValues.append(row.getDimensionValues().get(i));
          }
          CubeNode parentNode = curParent.get(parentDimValues.toString());
          // Sometimes Pinot returns a node without any matching parent; we discard those nodes.
          if (parentNode == null) {
            continue;
          }
          CubeNode node = row.toNode(level, index, parentNode);
          hierarchicalNodes.get(level).add(node);
          // Add current node's dimension values to next parent lookup table for the next level of nodes
          parentDimValues.append(row.getDimensionValues().get(level - 1));
          nextParent.put(parentDimValues.toString(), node);
        }
      } else { // root
        Row row = dataRows.get(0).get(0);
        CubeNode node = row.toNode();
        hierarchicalNodes.get(0).add(node);
        nextParent.put("", node);
      }

      // The last level of nodes won't be a parent of any other nodes, so we don't need to initialized
      // the hashmap of parent nodes for it.
      if (level != dimensions.size()) {
        curParent = nextParent;
        nextParent = new HashMap<>();
      }
    }

    return hierarchicalNodes;
  }

  private List<DimNameValueCostEntry> computeOneDimensionCost(CubeClient olapClient, double topBaselineValue,
      double topCurrentValue, double topBaselineSize, double topCurrentSize, Dimensions dimensions,
      Multimap<String, String> filterSets) throws Exception {

    double topRatio = topCurrentValue / topBaselineValue;
    LOG.info("topBaselineValue:{}, topCurrentValue:{}, changeRatio:{}", topBaselineValue, topCurrentValue, topRatio);

    List<DimNameValueCostEntry> costSet = new ArrayList<>();
    List<List<Row>> wowValuesOfDimensions = olapClient.getAggregatedValuesOfDimension(dimensions, filterSets);
    for (int i = 0; i < dimensions.size(); ++i) {
      String dimensionName = dimensions.get(i);
      List<Row> wowValuesOfOneDimension = wowValuesOfDimensions.get(i);
      for (Row wowValues : wowValuesOfOneDimension) {
        CubeNode wowNode = wowValues.toNode();
        String dimensionValue = wowNode.getDimensionValues().get(0);
        double contributionFactor =
            (wowNode.getBaselineSize() + wowNode.getCurrentSize()) / (topBaselineSize + topCurrentSize);
        double cost = costFunction.computeCost(topRatio, wowNode.getBaselineValue(), wowNode.getCurrentValue(),
            wowNode.getBaselineSize(), wowNode.getCurrentSize(), topBaselineValue, topCurrentValue, topBaselineSize,
            topCurrentSize);

        costSet.add(new DimNameValueCostEntry(dimensionName, dimensionValue, wowNode.getBaselineValue(),
            wowNode.getCurrentValue(), wowNode.changeRatio(), wowNode.getCurrentValue() - wowNode.getBaselineValue(),
            wowNode.getBaselineSize(), wowNode.getCurrentSize(), contributionFactor, cost));
      }
    }

    costSet.sort(Collections.reverseOrder());
    LOG.info("Top {} nodes (depth=1):", TOP_COST_ENTRIES_TO_LOG);
    for (DimNameValueCostEntry entry : costSet.subList(0, Math.min(costSet.size(), TOP_COST_ENTRIES_TO_LOG))) {
      LOG.info("\t{}", entry);
    }

    return costSet;
  }

  public static class DimensionCost {
    private String dimensinoName;
    private double cost;

    public DimensionCost(String dimensinoName, double cost) {
      this.dimensinoName = Preconditions.checkNotNull(dimensinoName);
      this.cost = cost;
    }

    public String getDimensinoName() {
      return dimensinoName;
    }

    public double getCost() {
      return cost;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DimensionCost that = (DimensionCost) o;
      return Double.compare(that.cost, cost) == 0 && Objects.equals(dimensinoName, that.dimensinoName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dimensinoName, cost);
    }
  }

  private static class HierarchicalDimensionCost {
    List<String> dimensionNames;
    double cost;

    public HierarchicalDimensionCost(List<String> dimensionNames, double cost) {
      this.dimensionNames = dimensionNames;
      this.cost = cost;
    }
  }

  /**
   * Calculates the cost of each level-1 dimension and sorts them by their cost.
   *
   * @param costSet the cost of level-1 dimension values.
   *
   * @return A list of dimension names to their cost that are sorted by their costs.
   */
  static List<DimensionCost> calculateSortedDimensionCost(List<DimNameValueCostEntry> costSet) {
    Map<String, Double> dimNameToCost = new HashMap<>();
    for (DimNameValueCostEntry dimNameValueCostEntry : costSet) {
      double cost = dimNameValueCostEntry.getCost();
      if (dimNameToCost.containsKey(dimNameValueCostEntry.getDimName())) {
        cost += dimNameToCost.get(dimNameValueCostEntry.getDimName());
      }
      dimNameToCost.put(dimNameValueCostEntry.getDimName(), cost);
    }

    // Sort dimensions by their cost
    List<DimensionCost> dimensionCosts = new ArrayList<>(dimNameToCost.size());
    for (Map.Entry<String, Double> dimNameToCostEntry : dimNameToCost.entrySet()) {
      dimensionCosts.add(new DimensionCost(dimNameToCostEntry.getKey(), dimNameToCostEntry.getValue()));
    }
    dimensionCosts.sort(new Comparator<DimensionCost>() {
      @Override
      public int compare(DimensionCost d1, DimensionCost d2) {
        return Double.compare(d2.cost, d1.cost);
      }
    });

    return dimensionCosts;
  }


  /**
   * Sort dimensions according to their cost, which is the sum of the error for aggregating all its children rows.
   * Dimensions with larger error is ordered in the front of the list.
   * The order among the dimensions that belong to the same hierarchical group will be maintained. An example of
   * a hierarchical group is {continent, country}. The cost of a group is the average of member costs.
   */
  static Dimensions sortDimensions(List<DimensionCost> sortedDimensionCosts, int depth,
      List<List<String>> suggestedHierarchies) {

    // Trim the list of dimension cost to the max depth that is specified by users
    List<DimensionCost> trimmedSortedDimensionCosts =
        sortedDimensionCosts.subList(0, Math.min(sortedDimensionCosts.size(), Math.max(1, depth)));

    // Reorder the dimensions based on the given hierarchy
    List<String> dimensionsToBeOrdered = new ArrayList<>(trimmedSortedDimensionCosts.size());
    for (DimensionCost dimensionCost : trimmedSortedDimensionCosts) {
      dimensionsToBeOrdered.add(dimensionCost.dimensinoName);
    }
    List<HierarchicalDimensionCost> hierarchicalDimensionCosts =
        getInitialHierarchicalDimensionList(dimensionsToBeOrdered, suggestedHierarchies);
    sortHierarchicalDimensions(hierarchicalDimensionCosts, trimmedSortedDimensionCosts);

    // The ordered dimension names
    List<String> dimensionNames = new ArrayList<>();
    for (HierarchicalDimensionCost value : hierarchicalDimensionCosts) {
      dimensionNames.addAll(value.dimensionNames);
    }
    return new Dimensions(dimensionNames);
  }

  /**
   * Given a list of dimension to be sorted and a list of hierarchy, returns a list of hierarchical dimensions.
   * Example:
   *   dimensionsToBeOrdered = ["country", "continent", "page"]
   *   suggestedHierarchies = [["continent", "country", "postcode"], ["pageGroup", "page"]]
   *
   * Returns: [["continent", "country"], ["page"]] (Note: the order of the two group could be arbitrary.)
   *
   * @param dimensionsToBeOrdered the dimensions to be considered during auto-dimension.
   * @param suggestedHierarchies the hierarchy among the dimensions.
   *
   * @return a list of hierarchical dimensions.
   */
  private static List<HierarchicalDimensionCost> getInitialHierarchicalDimensionList(List<String> dimensionsToBeOrdered,
      List<List<String>> suggestedHierarchies) {

    List<HierarchicalDimensionCost> hierarchicalDimensionCosts = new ArrayList<>();
    Set<String> availableDimensionKeySet = new HashSet<>(dimensionsToBeOrdered);
    // Process the suggested hierarchy list and filter out only the hierarchies that can be applied to the
    // dimensions to be ordered.
    for (List<String> suggestedHierarchy : suggestedHierarchies) {
      if (suggestedHierarchy == null || suggestedHierarchy.size() < 2) {
        continue;
      }

      List<String> sanitizedHierarchy = new ArrayList<>();
      for (String dimension : suggestedHierarchy) {
        if (availableDimensionKeySet.contains(dimension)) {
          sanitizedHierarchy.add(dimension);
          availableDimensionKeySet.remove(dimension);
        }
      }

      hierarchicalDimensionCosts.add(new HierarchicalDimensionCost(sanitizedHierarchy, 0));
    }

    for (String remainDimension : availableDimensionKeySet) {
      hierarchicalDimensionCosts.add(new HierarchicalDimensionCost(Collections.singletonList(remainDimension), 0));
    }

    return hierarchicalDimensionCosts;
  }

  /**
   * Sort the hierarchical dimension by their group cost, which is the average of the costs of the dimensions in the
   * same group.
   *
   * @param hierarchicalDimensionCosts the list of hierarchical dimensions to be sorted.
   * @param sortedDimensionCosts the cost of every level-1 dimension.
   */
  private static void sortHierarchicalDimensions(List<HierarchicalDimensionCost> hierarchicalDimensionCosts,
      List<DimensionCost> sortedDimensionCosts) {
    Map<String, HierarchicalDimensionCost> hierarchicalDimensionCostTable = new HashMap<>();
    for (HierarchicalDimensionCost hierarchicalDimensionCost : hierarchicalDimensionCosts) {
      List<String> dimensions = hierarchicalDimensionCost.dimensionNames;
      for (String dimension : dimensions) {
        hierarchicalDimensionCostTable.put(dimension, hierarchicalDimensionCost);
      }
    }

    // Average the cost of each group of hierarchical dimensions
    for (DimensionCost dimensionCost : sortedDimensionCosts) {
      HierarchicalDimensionCost hierarchicalDimensionCost =
          hierarchicalDimensionCostTable.get(dimensionCost.dimensinoName);
      hierarchicalDimensionCost.cost += dimensionCost.cost;
    }
    for (HierarchicalDimensionCost hierarchicalDimensionCost : hierarchicalDimensionCosts) {
      hierarchicalDimensionCost.cost /= hierarchicalDimensionCost.dimensionNames.size();
    }

    // Sort the groups of hierarchical dimensions by their average cost
    hierarchicalDimensionCosts.sort(new Comparator<HierarchicalDimensionCost>() {
      @Override
      public int compare(HierarchicalDimensionCost o1, HierarchicalDimensionCost o2) {
        return Double.compare(o2.cost, o1.cost);
      }
    });
  }

  public void toJson(String fileName)
      throws IOException {
    new ObjectMapper().writeValue(new File(fileName), this);
  }

  public static Cube fromJson(String fileName)
      throws IOException {
    Cube cube = new ObjectMapper().readValue(new File(fileName), Cube.class);
    cube.rebuildHierarchy();
    return cube;
  }

  /**
   * Reestablishes the hierarchy between aggregated and detailed rows after the cube is read from a Json string.
   */
  private void rebuildHierarchy() {
    this.hierarchicalNodes = dataRowToCubeNode(hierarchicalRows, dimensions);
  }

  @Override
  public String toString() {
    ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE);
    tsb.append("Baseline Value", baselineTotal)
        .append("Current Value", currentTotal)
        .append("Change Ratio", currentTotal / baselineTotal)
        .append("Dimensions", this.dimensions)
        .append("#Detailed Rows", hierarchicalRows.get(hierarchicalRows.size() - 1).size());
    return tsb.toString();
  }
}

