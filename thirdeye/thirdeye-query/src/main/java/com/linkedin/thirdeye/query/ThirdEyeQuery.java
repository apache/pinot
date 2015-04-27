package com.linkedin.thirdeye.query;

import com.google.common.base.Objects;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConstants;
import org.joda.time.DateTime;

import java.util.*;

public class ThirdEyeQuery {
  private final List<String> metricNames;
  private final Multimap<String, String> dimensionValues;
  private final List<ThirdEyeFunction> functions;
  private final List<ThirdEyeFunction> derivedMetrics;
  private final List<String> groupByColumns;

  private String collection;
  private DateTime start;
  private DateTime end;

  public ThirdEyeQuery() {
    this.metricNames = new ArrayList<>();
    this.dimensionValues = LinkedListMultimap.create();
    this.functions = new ArrayList<>();
    this.derivedMetrics = new ArrayList<>();
    this.groupByColumns = new ArrayList<>();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(ThirdEyeQuery.class)
        .add("collection", collection)
        .add("metricNames", metricNames)
        .add("start", start)
        .add("end", end)
        .add("dimensionValues", dimensionValues)
        .add("groupByColumns", groupByColumns)
        .add("functions", functions)
        .add("derivedMetrics", derivedMetrics)
        .toString();
  }

  // Setters

  public ThirdEyeQuery setCollection(String collection) {
    this.collection = collection;
    return this;
  }

  public ThirdEyeQuery addMetricName(String metricName) {
    this.metricNames.add(metricName);
    return this;
  }

  public ThirdEyeQuery addDimensionValue(String dimensionName, String dimensionValue) {
    dimensionValues.put(dimensionName, dimensionValue);
    return this;
  }

  public ThirdEyeQuery addGroupByColumn(String column) {
    this.groupByColumns.add(column);
    return this;
  }

  public ThirdEyeQuery setStart(DateTime start) {
    this.start = start;
    return this;
  }

  public ThirdEyeQuery setEnd(DateTime end) {
    this.end = end;
    return this;
  }

  public ThirdEyeQuery addFunction(ThirdEyeFunction function) {
    this.functions.add(function);
    return this;
  }

  public ThirdEyeQuery addDerivedMetric(ThirdEyeFunction function) {
    this.derivedMetrics.add(function);
    return this;
  }

  // Getters

  public String getCollection() {
    return collection;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  public DateTime getStart() {
    return start;
  }

  public DateTime getEnd() {
    return end;
  }

  public List<String> getGroupByColumns() {
    return groupByColumns;
  }

  public Multimap<String, String> getDimensionValues() {
    return dimensionValues;
  }

  public List<ThirdEyeFunction> getFunctions() {
    return functions;
  }

  public List<ThirdEyeFunction> getDerivedMetrics() {
    return derivedMetrics;
  }

  // Utilities

  public List<String[]> getDimensionCombinations(List<DimensionSpec> dimensions) {
    List<String[]> collector = new LinkedList<String[]>();
    String[] current = new String[dimensions.size()];

    for (int i = 0; i < dimensions.size(); i++) {
      if (dimensionValues.containsKey(dimensions.get(i).getName())) {
        current[i] = null; // should explore
      } else {
        current[i] = StarTreeConstants.STAR;
      }
    }

    getDimensionCombinations(dimensions, current, collector, 0);

    return collector;
  }

  private void getDimensionCombinations(List<DimensionSpec> dimensions,
                                        String[] current,
                                        List<String[]> collector,
                                        int dimensionIndex) {
    // Check if we have non-null value for each of current
    boolean allSelected = true;
    for (String value : current) {
      if (value == null) {
        allSelected = false;
        break;
      }
    }

    if (allSelected) {
      collector.add(Arrays.copyOf(current, current.length));
    } else {
      for (int i = dimensionIndex; i < dimensions.size(); i++) {
        if (current[i] == null) {
          // Explore for each dimension value
          Collection<String> values = dimensionValues.get(dimensions.get(i).getName());
          if (values != null) {
            for (String value : values) {
              current[i] = value;
              getDimensionCombinations(dimensions, current, collector, i + 1);
              current[i] = null;
            }
          }
        }
      }
    }
  }
}
