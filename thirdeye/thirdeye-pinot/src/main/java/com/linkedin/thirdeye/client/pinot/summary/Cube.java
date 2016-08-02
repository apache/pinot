package com.linkedin.thirdeye.client.pinot.summary;

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
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;


@JsonIgnoreProperties(ignoreUnknown = true)
public class Cube { // the cube (Ca|Cb)
  private static final int topDimension = 3;

  @JsonProperty("dimensions")
  Dimensions dimensions;

  // The data stored in levels
  @JsonProperty("hierarchicalRows")
  private List<List<Row>> hierarchicalRows = new ArrayList<>();

  // The logical nodes of the hierarchy among rows (i.e., the actual data)
  @JsonIgnore
  private List<List<HierarchyNode>> hierarchicalNodes = new ArrayList<>();


  public List<List<HierarchyNode>> getHierarchicalNodes() {
    return hierarchicalNodes;
  }

  public void buildFromOALPDataBase(OLAPDataBaseClient olapClient, Dimensions dimensions) {
    buildFromOALPDataBase(olapClient, dimensions, Collections.<List<String>>emptyList());
  }

  public void buildFromOALPDataBase(OLAPDataBaseClient olapClient, Dimensions dimensions, List<List<String>> hierarchy) {
    // Calculate the change ratio of the top aggregated values
    Pair<Double, Double> topAggValues = olapClient.getTopAggregatedValues();
    double topBaselineValue = topAggValues.getLeft(); // aggregated baseline values
    double topCurrentValue = topAggValues.getRight(); // aggregated current values
    double topRatio = topCurrentValue / topBaselineValue; // change ratio

    if (hierarchy == null || hierarchy.size() == 0) {
      this.dimensions = sortDimensionLevel(olapClient, topRatio, dimensions);
    } else {
      this.dimensions = sortDimensionLevel(olapClient, topRatio, dimensions, hierarchy);
    }
    System.out.println(this.dimensions);

    // Get the rows at each level and sort them in the post-order of their hierarchical relationship,
    // in which a parent row aggregates the details rows under it. For instance, in the following
    // hierarchy row b aggregates rows d and e, and row a aggregates rows b and c.
    //     Level 0              a
    //                         / \
    //     Level 1            b   c
    //                       / \   \
    //     Level 2          d   e   f
    // The Comparator for generating the order is implemented in the class DimensionValues.
    long startTime = System.currentTimeMillis();
    for (int i = 0; i <= this.dimensions.size(); ++i) {
      List<Row> rowAtLevelI = olapClient.getAggregatedValuesAtLevel(this.dimensions, i);
      rowAtLevelI.sort(new RowDimensionValuesComparator());
      hierarchicalRows.add(rowAtLevelI);
    }
    long endTime = System.currentTimeMillis();
    System.out.println("Data preparation time: " + (endTime - startTime));
    for (int i = 0; i <= this.dimensions.size(); ++i) {
      System.out.println(hierarchicalRows.get(i).toString());
    }
    buildHierarchy();
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
   * Establish the hierarchy between aggregated and detailed rows
   */
  void buildHierarchy() {
    for (int level = 0; level <= this.dimensions.size(); ++level) {
      List<HierarchyNode> nodesAtLevelI = new ArrayList<>(hierarchicalRows.get(level).size());
      hierarchicalNodes.add(nodesAtLevelI);

      if (level != 0) {
        int parentIndex = 0;
        HierarchyNode parentNode = hierarchicalNodes.get(level-1).get(parentIndex);
        for (int index = 0; index < hierarchicalRows.get(level).size(); ++index) {
          Row row = hierarchicalRows.get(level).get(index);
          HierarchyNode node = new HierarchyNode(level, index, row, parentNode);
          parentNode.children.add(node);
          hierarchicalNodes.get(level).add(node);

          if (level > 1) {
            Row nextRow = index == hierarchicalRows.get(level).size()-1 ? null : hierarchicalRows.get(level).get(index+1);
            if (nextRow != null &&
               !nextRow.dimensionValues.get(level-2).equals(row.dimensionValues.get(level-2)))
            {
                ++parentIndex;
                parentNode = hierarchicalNodes.get(level-1).get(parentIndex);
            }
          }
        }
      } else { // root
        Row row = hierarchicalRows.get(0).get(0);
        HierarchyNode node = new HierarchyNode(level, 0, row, null);
        node.isRoot = true;
        hierarchicalNodes.get(0).add(node);
      }
    }

    {
      int level = this.dimensions.size() - 1;
      for (int index = 0; index < hierarchicalRows.get(level).size(); ++index) {
        hierarchicalNodes.get(level).get(index).isWorker = true;
      }
    }

//    for (int level = 0; level <= this.dimensions.size(); ++level) {
//      for (int index = 0; index < hierarchicalRows.get(level).size(); ++index) {
//        HierarchyNode node = hierarchicalNodes.get(level).get(index);
//        System.out.println(node);
//      }
//    }
  }

  /**
   * Sort dimensions according to their cost, which is the sum of the error for aggregating all its children rows.
   * Dimensions with larger error is sorted in the front of the list.
   */
  private static Dimensions sortDimensionLevel(OLAPDataBaseClient olapClient, double topRatio, Dimensions dimensions) {
    // Calculate the cost of each dimension
    List<Pair<Double, String>> costOfDimensionPairs = new ArrayList<>(dimensions.size());
    for (int i = 0; i < dimensions.size(); ++i) {
      List<Pair<Double, Double>> wowValuesInOneDimension =
          olapClient.getAggregatedValuesInOneDimension(dimensions.get(i));
      MutablePair<Double, String> costOfDimensionPair = new MutablePair<>();
      costOfDimensionPair.setRight(dimensions.get(i));
      double cost = .0;
      for (int j = 0; j < wowValuesInOneDimension.size(); ++j) {
        Pair<Double, Double> wowValues = wowValuesInOneDimension.get(j);
        cost += CostFunction.err4EmptyValues(wowValues.getLeft(), wowValues.getRight(), topRatio, topRatio);
      }
      costOfDimensionPair.setLeft(cost);
      costOfDimensionPairs.add(costOfDimensionPair);
    }

    // Create a new Dimension instance whose dimensions follow the calculated order
    Collections.sort(costOfDimensionPairs, Collections.reverseOrder());
    ArrayList<String> sortedDimensionsList = new ArrayList<>(costOfDimensionPairs.size());
    System.out.println("Dimension order:");
    for (int i = 0; i < costOfDimensionPairs.size(); ++i) {
      sortedDimensionsList.add(costOfDimensionPairs.get(i).getRight());
      System.out.print("  Dimension: " + costOfDimensionPairs.get(i).getRight() + ", Cost: ");
      System.out.println(costOfDimensionPairs.get(i).getLeft());
    }

    return new Dimensions(sortedDimensionsList.subList(0, topDimension));
  }

  private static Dimensions sortDimensionLevel(OLAPDataBaseClient olapClient, double topRatio, Dimensions dimensions, List<List<String>> hierarchy) {
    List<MutablePair<String, Double>> dimensionCostPairs = new ArrayList<>();

    Map<String, DimensionGroup> groupMap = new HashMap<>();
    Set<String> dimensionKeySet = new HashSet<>(dimensions.names);
    for (List<String> groupList : hierarchy) {
      if (groupList == null || groupList.size() == 0) continue;
      DimensionGroup group = new DimensionGroup();
      group.hierarchy = groupList;
      for (String name : groupList) {
        if (dimensionKeySet.contains(name)) {
          groupMap.put(name, group);
          ++group.count;
        }
      }
      if (group.count > 0) {
        group.index = dimensionCostPairs.size();
        dimensionCostPairs.add(new MutablePair<>(groupList.get(0), .0));
      }
    }

    // Calculate cost for each dimension. The cost of the dimensions of the same hierarchical group will be sum up.
    for (int i = 0; i < dimensions.size(); ++i) {
      String dimension = dimensions.get(i);
      double cost = .0;
      List<Pair<Double, Double>> wowValuesInOneDimension = olapClient.getAggregatedValuesInOneDimension(dimension);
      for (int j = 0; j < wowValuesInOneDimension.size(); ++j) {
        Pair<Double, Double> wowValues = wowValuesInOneDimension.get(j);
        cost += CostFunction.err4EmptyValues(wowValues.getLeft(), wowValues.getRight(), topRatio, topRatio);
      }

      if (groupMap.containsKey(dimension)) {
        DimensionGroup dimensionGroup = groupMap.get(dimension);
        MutablePair<String, Double> costOfDimensionPair = dimensionCostPairs.get(dimensionGroup.index);
        costOfDimensionPair.right += cost;
      } else {
        MutablePair<String, Double> costOfDimensionPair = new MutablePair<>(dimension, cost);
        dimensionCostPairs.add(costOfDimensionPair);
      }
    }

    // Calculate average cost for each hierarchical group
    for (MutablePair<String, Double> costOfDimensionPair : dimensionCostPairs) {
      if (groupMap.containsKey(costOfDimensionPair.getLeft())) {
        DimensionGroup group = groupMap.get(costOfDimensionPair.getLeft());
        costOfDimensionPair.right /= group.count;
      }
    }
    System.out.println("Before Sort: " + dimensionCostPairs);
    dimensionCostPairs.sort((new DimensionCostPairSorter()).reversed());
    System.out.println("After Sort: " + dimensionCostPairs);

    // Create a new Dimension instance whose dimensions follow the calculated order
    ArrayList<String> newDimensions = new ArrayList<>();
    for (MutablePair<String, Double> dimensionCostPair : dimensionCostPairs) {
      if (groupMap.containsKey(dimensionCostPair.getLeft())) {
        DimensionGroup dimensionGroup = groupMap.get(dimensionCostPair.getLeft());
        if (dimensionGroup.count == dimensionGroup.hierarchy.size()) {
          newDimensions.addAll(dimensionGroup.hierarchy);
        } else {
          for (String subdimension : dimensionGroup.hierarchy) {
            if (groupMap.containsKey(subdimension)) {
              newDimensions.add(subdimension);
            }
          }
        }

        System.out.print("  Dimension: " + dimensionGroup.hierarchy + ", Cost: ");
      } else {
        newDimensions.add(dimensionCostPair.getLeft());
        System.out.print("  Dimension: " + dimensionCostPair.getLeft() + ", Cost: ");
      }
      System.out.println(dimensionCostPair.getRight());
    }
    return new Dimensions(newDimensions.subList(0, topDimension));
  }

  static class DimensionCostPairSorter implements Comparator<MutablePair<String, Double>> {
    @Override
    public int compare(MutablePair<String, Double> o1, MutablePair<String, Double> o2) {
      return Double.compare(o1.getRight(), o2.getRight());
    }
  }

  static class DimensionGroup {
    int index = -1;
    int count = 0;
    List<String> hierarchy;
  }

  public void toJson(String fileName) throws IOException {
    new ObjectMapper().writeValue(new File(fileName), this);
  }

  public static Cube fromJson(String fileName) throws IOException {
    return new ObjectMapper().readValue(new File(fileName), Cube.class);
  }

  public void removeEmptyRecords(List<Row> rows) {
    for (int i = rows.size() - 1; i >= 0; --i) {
      if (Double.compare(rows.get(i).baselineValue, .0) == 0 || Double.compare(rows.get(i).currentValue, .0) == 0) {
        rows.remove(i);
      }
    }
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
  }

  public static void main (String[] argc) {
    String oFileName = "MLCube.json";

    Cube cube = null;
    try {
      cube = Cube.fromJson(oFileName);
      System.out.println("Restored Cube:");
      System.out.println(cube);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
    cube.buildHierarchy();
    Summary.computeSummary(cube.hierarchicalNodes);
  }
}
