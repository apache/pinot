package com.linkedin.thirdeye.client.pinot.summary;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;


@JsonIgnoreProperties(ignoreUnknown = true)
public class Cube { // the cube (Ca|Cb)
  @JsonProperty("dimensions")
  Dimensions dimensions;

  private double topBaselineValue; // aggregated baseline values
  private double topCurrentValue; // aggregated current values
  private double topRatio; // change ratio = topCurrentValue / topBaselineValue

  @JsonProperty("rows")
  private List<Row> rows = new ArrayList<Row>();

  public double getTopBaselineValue() {
    return topBaselineValue;
  }

  public double getTopCurrentValue() {
    return topCurrentValue;
  }

  public double getTopRatio() {
    return topRatio;
  }

  public int getRowCount() {
    return rows.size();
  }

  public Row getRowAt(int index) {
    return rows.get(index);
  }

  public void setGlobalInfo(Double responseGa, Double responseGb, int multiplier) {
    this.topBaselineValue = responseGa * multiplier;
    this.topCurrentValue = responseGb * multiplier;
    this.topRatio = this.topCurrentValue / this.topBaselineValue;
  }

  public void buildFromOALPDataBase(OLAPDataBaseClient olapClient, Dimensions dimensions) {
    // Calculate the change ratio of the top aggregated values
    Pair<Double, Double> topAggValues = olapClient.getTopAggregatedValues();
    topBaselineValue = topAggValues.getLeft();
    topCurrentValue = topAggValues.getRight();
    topRatio = topCurrentValue / topBaselineValue;

    this.dimensions = sortDimensionLevel(olapClient, topRatio, dimensions);

    // Get the rows at each level and sort them in the post-order of their hierarchical relationship,
    // in which a parent row aggregates the details rows under it. For instance, in the following
    // hierarchy row b aggregates rows d and e, and row a aggregates rows b and c.
    //                   a
    //                  / \
    //                 b   c
    //                / \   \
    //               d   e   f
    // The Comparator for generating the order is implemented in the class DimensionValues.
    long startTime = System.currentTimeMillis();
    for (int i = this.dimensions.size(); i > 0; --i) {
      rows.addAll(olapClient.getAggregatedValuesAtLevel(this.dimensions, i));
    }
    long endTime = System.currentTimeMillis();
    System.out.println("Data preparation time: " + (endTime - startTime));
    System.out.println("# rows: " + rows.size());
    rows.sort(new RowDimensionValuesComparator());
    //    System.out.println(rows);
  }

  /**
   * Sort the rows in the post-order of their hierarchical relationship
   */
  private static class RowDimensionValuesComparator implements Comparator<Row> {
    @Override
    public int compare(Row r1, Row r2) {
      return r1.dimensionValues.compareTo(r2.dimensionValues);
    }
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
      double tmpCost = .0;
      for (int j = 0; j < wowValuesInOneDimension.size(); ++j) {
        Pair<Double, Double> wowValues = wowValuesInOneDimension.get(j);
        tmpCost += CostFunction.err(wowValues.getLeft(), wowValues.getRight(), topRatio);
      }
      costOfDimensionPair.setLeft(tmpCost);
      costOfDimensionPairs.add(costOfDimensionPair);
    }

    // Create a new Dimension instance whose dimensions follow the calculated order
    Collections.sort(costOfDimensionPairs, Collections.reverseOrder());
    List<String> sortedDimensionsList = new ArrayList<>(costOfDimensionPairs.size());
    System.out.println("Dimension order:");
    for (int i = 0; i < costOfDimensionPairs.size(); ++i) {
      sortedDimensionsList.add(costOfDimensionPairs.get(i).getRight());
      System.out.print("  Dimension: " + costOfDimensionPairs.get(i).getRight() + ", Cost: ");
      System.out.println(costOfDimensionPairs.get(i).getLeft());
    }

    return new Dimensions(sortedDimensionsList);
  }

  public void toJson(String fileName) throws IOException {
    new ObjectMapper().writeValue(new File(fileName), this);
  }

  public static Cube fromJson(String fileName) throws IOException {
    return new ObjectMapper().readValue(new File(fileName), Cube.class);
  }

  public void removeEmptyRecords() {
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
}
