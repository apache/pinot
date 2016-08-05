package com.linkedin.thirdeye.client.pinot.summary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jersey.repackaged.com.google.common.collect.Lists;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SummaryResponse {
  @JsonProperty("dimensions")
  List<String> dimensions = new ArrayList<>();;

  @JsonProperty("responseRows")
  private
  List<SummaryResponseRow> responseRows = new ArrayList<>();

  public List<SummaryResponseRow> getResponseRows() {
    return responseRows;
  }

  public static SummaryResponse buildResponse(List<HierarchyNode> nodes, int levelCount) {
    SummaryResponse response = new SummaryResponse();

    // Build the header
    Dimensions dimensions = nodes.get(0).data.dimensions;
    for (int i = 0; i < levelCount; ++i) {
      response.dimensions.add(dimensions.names.get(i));
    }

    // Build the response
    nodes.sort(Summary.NODE_COMPARATOR.reversed());
    //   Build name tag for each row of responses
    Map<HierarchyNode, NameTag> nameTags = new HashMap<>();
    for (HierarchyNode node : nodes) {
      NameTag tag = new NameTag(levelCount);
      nameTags.put(node, tag);
      tag.copyNames(node.data.dimensionValues);
    }
    for (HierarchyNode node : nodes) {
      HierarchyNode parent = node;
      int levelDiff = 1;
      while ((parent = parent.parent) != null) {
        NameTag parentNameTag = nameTags.get(parent);
        if (parentNameTag != null) {
          parentNameTag.setNotAll(node.level-levelDiff);
          break;
        }
        ++levelDiff;
      }
    }
    //    Fill in the information of each response row
    for (HierarchyNode node : nodes) {
      SummaryResponseRow row = new SummaryResponseRow();
      row.names = nameTags.get(node).names;
      row.baselineValue = node.baselineValue;
      row.currentValue = node.currentValue;
      row.ratio = node.currentRatio();
      response.responseRows.add(row);
    }

    return response;
  }

  public String toString() {
    ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
    tsb.append('\n').append(this.dimensions);
    for (SummaryResponseRow row : getResponseRows()) {
      tsb.append('\n').append(row);
    }
    return tsb.toString();
  }

  public static class SummaryResponseRow {
    private List<String> names;
    private double baselineValue;
    private double currentValue;
    private double ratio;

    public List<String> getNames() {
      return names;
    }

    public double getBaselineValue() {
      return baselineValue;
    }

    public double getCurrentValue() {
      return currentValue;
    }

    public double getRatio() {
      return ratio;
    }

    public String toString() {
      return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
    }
  }

  private static class NameTag {
    private static final String ALL = "(ALL)";
    private static final String NOT_ALL = "(ALL)-";

    private List<String> names;

    public NameTag(int levelCount) {
      names = new ArrayList<>(levelCount);
      for (int i = 0; i < levelCount; ++i) {
        names.add(ALL);
      }
    }

    public void copyNames(DimensionValues dimensionValues) {
      for (int i = 0; i < dimensionValues.values.size(); ++i) {
        names.set(i, dimensionValues.values.get(i));
      }
    }

    public void setNotAll(int index) {
      names.set(index, NOT_ALL);
    }
  }
}
