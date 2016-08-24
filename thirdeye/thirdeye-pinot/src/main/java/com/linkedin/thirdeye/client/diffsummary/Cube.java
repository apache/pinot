package com.linkedin.thirdeye.client.diffsummary;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;


@JsonIgnoreProperties(ignoreUnknown = true)
public class Cube { // the cube (Ca|Cb)
  private static final Logger LOG = LoggerFactory.getLogger(Cube.class);
  private static final int DEFAULT_TOP_DIMENSION = 3;

  private double topBaselineValue;
  private double topCurrentValue;
  private double topRatio;

  @JsonProperty("dimensions")
  private Dimensions dimensions;

  // The data stored in levels
  @JsonProperty("hierarchicalRows")
  private List<List<Row>> hierarchicalRows = new ArrayList<>();

  // The logical nodes of the hierarchy among rows (i.e., the actual data)
  @JsonIgnore
  private List<List<HierarchyNode>> hierarchicalNodes = new ArrayList<>();


  public double getTopBaselineValue() {
    return topBaselineValue;
  }

  public double getTopCurrentValue() {
    return topCurrentValue;
  }

  public double getTopRatio() {
    return topRatio;
  }

  public Dimensions getDimensions() {
    return dimensions;
  }

  @JsonIgnore
  public HierarchyNode getRoot() {
    if (hierarchicalNodes.size() != 0 && hierarchicalNodes.get(0).size() != 0) {
      return hierarchicalNodes.get(0).get(0);
    } else {
      return null;
    }
  }

  public void buildWithAutoDimensionOrder(OLAPDataBaseClient olapClient, Dimensions dimensions) throws Exception {
    buildWithAutoDimensionOrder(olapClient, dimensions, DEFAULT_TOP_DIMENSION, Collections.emptyList());
  }

  public void buildWithAutoDimensionOrder(OLAPDataBaseClient olapClient, Dimensions dimensions, int topDimensions)
      throws Exception {
    buildWithAutoDimensionOrder(olapClient, dimensions, topDimensions, Collections.emptyList());
  }

  public void buildWithAutoDimensionOrder(OLAPDataBaseClient olapClient, Dimensions dimensions, int topDimension,
      List<List<String>> hierarchy) throws Exception {

    initializeBasicInfo(olapClient);
    if (dimensions == null || dimensions.size() == 0) {
      throw new IllegalArgumentException("Dimensions cannot be empty.");
    }
    if (hierarchy == null) {
      hierarchy = Collections.emptyList();
    }
    this.dimensions = sortDimensionOrder(olapClient, topRatio, dimensions, topDimension, hierarchy);
    LOG.info("Auto decided dimensions: " + this.dimensions);

    buildWithManualDimensionOrder(olapClient, this.dimensions);
  }

  public void buildWithManualDimensionOrder(OLAPDataBaseClient olapClient, Dimensions dimensions) throws Exception {
    if (dimensions == null || dimensions.size() == 0) {
      throw new IllegalArgumentException("Dimensions cannot be empty.");
    }
    if (this.dimensions == null) {
      initializeBasicInfo(olapClient);
      this.dimensions = dimensions;
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
    List<List<Row>> rowOfLevels = olapClient.getAggregatedValuesOfLevels(dimensions);
    for (int i = 0; i <= dimensions.size(); ++i) {
      List<Row> rowAtLevelI = rowOfLevels.get(i);
      rowAtLevelI.sort(new RowDimensionValuesComparator());
      hierarchicalRows.add(rowAtLevelI);
      size += rowAtLevelI.size();
    }
    LOG.info("Size of the cube for generating summary: " + size);

    buildHierarchy();
  }

  /**
   * Calculate the change ratio of the top aggregated values.
   * @throws Exception An exception is thrown if OLAP database cannot be connected.
   */
  private void initializeBasicInfo(OLAPDataBaseClient olapClient) throws Exception {
    Row topAggValues = olapClient.getTopAggregatedValues();
    topBaselineValue = topAggValues.baselineValue; // aggregated baseline values
    topCurrentValue = topAggValues.currentValue; // aggregated current values
    topRatio = topCurrentValue / topBaselineValue; // change ratio
  }

  /**
   * Sort the rows in the post-order of their hierarchical relationship
   */
  static class RowDimensionValuesComparator implements Comparator<Row> {
    @Override
    public int compare(Row r1, Row r2) {
      return r1.dimensionValues.compareTo(r2.dimensionValues);
    }
  }

  /**
   * Establish the hierarchy between aggregated and detailed rows.
   */
  private void buildHierarchy() {
    for (int level = 0; level <= this.dimensions.size(); ++level) {
      List<HierarchyNode> nodesAtCurrentLevel = new ArrayList<>(hierarchicalRows.get(level).size());
      hierarchicalNodes.add(nodesAtCurrentLevel);

      if (level != 0) {
        int parentIndex = 0;
        HierarchyNode parentNode = hierarchicalNodes.get(level-1).get(parentIndex);
        for (int index = 0; index < hierarchicalRows.get(level).size(); ++index) {
          Row row = hierarchicalRows.get(level).get(index);
          HierarchyNode node = new HierarchyNode(level, index, row, parentNode);
          parentNode.children.add(node);
          hierarchicalNodes.get(level).add(node);

          // For testing if a node has the correct parent node
          boolean haveProblem = false;
          for (int i = 0; i < level-1; ++i) {
            if (parentNode == null || !parentNode.data.dimensionValues.get(i).equals(row.dimensionValues.get(i))) {
              haveProblem = true;
              break;
            }
          }
          if (haveProblem)
            LOG.info(row + " is incorrectly connected to " + parentNode.data);

          // If the next data does not have the same prefix of dimension values, then it belongs to a different present.
          if ( (level > 1) && (index != hierarchicalRows.get(level).size()-1) ) {
            Row nextRow = hierarchicalRows.get(level).get(index + 1);
            for (int i = level - 2; i >= 0; --i) {
              if ( !nextRow.dimensionValues.get(i).equals(row.dimensionValues.get(i)) ) {
                ++parentIndex;
                parentNode = hierarchicalNodes.get(level - 1).get(parentIndex);
                break;
              }
            }
          }
        }
      } else { // root
        Row row = hierarchicalRows.get(0).get(0);
        HierarchyNode node = new HierarchyNode(0, 0, row, null);
        hierarchicalNodes.get(0).add(node);
      }
    }
  }

  /**
   * Sort dimensions according to their cost, which is the sum of the error for aggregating all its children rows.
   * Dimensions with larger error is sorted in the front of the list.
   * The order among the dimensions that belong to the same hierarchical group will be maintained. An example of
   * a hierarchical group {continent, country}. The cost of a group is the average of member costs.
   * @throws Exception An exception is thrown if OLAP database cannot be connected.
   */
  private static Dimensions sortDimensionOrder(OLAPDataBaseClient olapClient, double topRatio, Dimensions dimensions,
      int topDimension, List<List<String>> hierarchy) throws Exception {
    List<MutablePair<String, Double>> dimensionCostPairs = new ArrayList<>();

    // Given one dimension name D, returns the hierarchical dimension to which D belong.
    Map<String, HierarchicalDimension> hierarchicalDimensionMap = new HashMap<>();
    Set<String> availableDimensionKeySet = new HashSet<>(dimensions.allDimensions());
    // Process the suggested hierarchy list and filter out only the hierarchies that can be applied to the available
    // dimensions of the dataset.
    for (List<String> suggestedHierarchyList : hierarchy) {
      if (suggestedHierarchyList == null || suggestedHierarchyList.size() < 2) continue;

      List<String> actualHierarchy = new ArrayList<>();
      for (String dimension : suggestedHierarchyList) {
        if (availableDimensionKeySet.contains(dimension)) {
          actualHierarchy.add(dimension);
        }
      }

      if (actualHierarchy.size() > 1) {
        HierarchicalDimension hierarchicalDimension = new HierarchicalDimension();
        hierarchicalDimension.hierarchy = actualHierarchy;
        for (String dimension : actualHierarchy) {
          hierarchicalDimensionMap.put(dimension, hierarchicalDimension);
        }
        hierarchicalDimension.index = dimensionCostPairs.size();
        dimensionCostPairs.add(new MutablePair<>(actualHierarchy.get(0), .0));
      }
    }

    List<List<Row>> wowValuesOfDimensions = olapClient.getAggregatedValuesOfDimension(dimensions);
    // Calculate cost for each dimension. The costs of the dimensions of the same hierarchical group will be the max
    // cost among all the children in that hierarchy.
    for (int i = 0; i < dimensions.size(); ++i) {
      String dimension = dimensions.get(i);
      double cost = .0;
      List<Row> wowValuesOfOneDimension = wowValuesOfDimensions.get(i);
      for (int j = 0; j < wowValuesOfOneDimension.size(); ++j) {
        Row wowValues = wowValuesOfOneDimension.get(j);
        cost += CostFunction.err4EmptyValues(wowValues.baselineValue, wowValues.currentValue, topRatio);
      }

      if (hierarchicalDimensionMap.containsKey(dimension)) {
        HierarchicalDimension hierarchicalDimension = hierarchicalDimensionMap.get(dimension);
        MutablePair<String, Double> costOfDimensionPair = dimensionCostPairs.get(hierarchicalDimension.index);
        // The max cost of children will be the cost of a group
        costOfDimensionPair.right = Math.max(cost, costOfDimensionPair.right);
      } else { // The dimension does not belong to any hierarchy
        MutablePair<String, Double> costOfDimensionPair = new MutablePair<>(dimension, cost);
        dimensionCostPairs.add(costOfDimensionPair);
      }
    }

    // Sort dimensions according to their costs in a descending order
    dimensionCostPairs.sort((new DimensionCostPairSorter()).reversed());

    // Create a new Dimension instance whose dimensions follow the calculated order
    ArrayList<String> newDimensions = new ArrayList<>();
    for (MutablePair<String, Double> dimensionCostPair : dimensionCostPairs) {
      StringBuilder sb = new StringBuilder("  Dimension: ");
      if (hierarchicalDimensionMap.containsKey(dimensionCostPair.getLeft())) {
        HierarchicalDimension hierarchicalDimension = hierarchicalDimensionMap.get(dimensionCostPair.getLeft());
        newDimensions.addAll(hierarchicalDimension.hierarchy);
        sb.append(hierarchicalDimension.hierarchy);
      } else { // The dimension does not belong to any hierarchy
        newDimensions.add(dimensionCostPair.getLeft());
        sb.append(dimensionCostPair.getLeft());
      }
      sb.append(", Cost: ");
      sb.append(dimensionCostPair.getRight());
      LOG.info(sb.toString());
    }
    return new Dimensions(newDimensions.subList(0, Math.min(topDimension, dimensions.size())));
  }

  static class DimensionCostPairSorter implements Comparator<MutablePair<String, Double>> {
    @Override
    public int compare(MutablePair<String, Double> o1, MutablePair<String, Double> o2) {
      return Double.compare(o1.getRight(), o2.getRight());
    }
  }

  static class HierarchicalDimension {
    int index = -1;
    List<String> hierarchy;
  }

  public void toJson(String fileName) throws IOException {
    new ObjectMapper().writeValue(new File(fileName), this);
  }

  public static Cube fromJson(String fileName) throws IOException {
    Cube cube = new ObjectMapper().readValue(new File(fileName), Cube.class);
    cube.buildHierarchy();
    return cube;
  }

  @Override
  public String toString() {
    ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE);
    tsb.append("Baseline Value", topBaselineValue).append("Current Value", topCurrentValue)
       .append("Ratio", topRatio)
       .append("Dimentions", this.dimensions)
       .append("#Detailed Rows", hierarchicalRows.get(hierarchicalRows.size()-1).size());
    return tsb.toString();
  }


}

