package com.linkedin.thirdeye.dashboard.views.diffsummary;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.client.diffsummary.DimensionValues;
import com.linkedin.thirdeye.client.diffsummary.Dimensions;
import com.linkedin.thirdeye.client.diffsummary.HierarchyNode;

public class SummaryResponse {
  private final static NumberFormat DOUBLE_FORMATTER = new DecimalFormat("#0.00");
  static final  String INFINITE = "";

  static final String ALL = "(ALL)";
  static final String NOT_ALL = "(ALL)-";
  static final String NOT_AVAILABLE = "-na-";

  @JsonProperty("metricName")
  private String metricName;

  @JsonProperty("dimensions")
  List<String> dimensions = new ArrayList<>();

  @JsonProperty("responseRows")
  private List<SummaryResponseRow> responseRows = new ArrayList<>();

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public List<SummaryResponseRow> getResponseRows() {
    return responseRows;
  }

  public static SummaryResponse buildNotAvailableResponse() {
    SummaryResponse response = new SummaryResponse();
    response.dimensions.add(NOT_AVAILABLE);
//    response.responseRows.add(SummaryResponseRow.buildNotAvailableRow());
    return response;
  }

  public static SummaryResponse buildResponse(List<HierarchyNode> nodes, int targetLevelCount) {
    SummaryResponse response = new SummaryResponse();

    // Compute the total baseline and current value
    double totalBaselineValue = 0d;
    double totalCurrentValue = 0d;
    for(HierarchyNode node : nodes) {
      totalBaselineValue += node.getBaselineValue();
      totalCurrentValue += node.getCurrentValue();
    }

    // If all nodes have a lower level count than targetLevelCount, then it is not necessary to print the summary with
    // height higher than the available level.
    int maxNodeLevelCount = 0;
    for (HierarchyNode node : nodes) {
      maxNodeLevelCount = Math.max(maxNodeLevelCount, node.getLevel());
    }
    targetLevelCount = Math.min(maxNodeLevelCount, targetLevelCount);

    // Build the header
    Dimensions dimensions = nodes.get(0).getDimensions();
    for (int i = 0; i < targetLevelCount; ++i) {
      response.dimensions.add(dimensions.get(i));
    }

    // Build the response
    nodes = SummaryResponseTree.sortResponseTree(nodes, targetLevelCount);
    //   Build name tag for each row of responses
    Map<HierarchyNode, NameTag> nameTags = new HashMap<>();
    Map<HierarchyNode, List<String>> otherDimensionValues = new HashMap<>();
    for (HierarchyNode node : nodes) {
      NameTag tag = new NameTag(targetLevelCount);
      nameTags.put(node, tag);
      tag.copyNames(node.getDimensionValues());
      otherDimensionValues.put(node, new ArrayList<>());
    }
    //   pre-condition: parent node is processed before its children nodes
    for (HierarchyNode node : nodes) {
      HierarchyNode parent = node;
      int levelDiff = 1;
      while ((parent = parent.getParent()) != null) {
        NameTag parentNameTag = nameTags.get(parent);
        if (parentNameTag != null) {
          // Set tag from ALL to NOT_ALL String.
          int notAllLevel = node.getLevel()-levelDiff;
          parentNameTag.setNotAll(notAllLevel);
          // For users' ease of understanding, we append what dimension values are excluded from NOT_ALL
          StringBuilder sb = new StringBuilder();
          String separator = "";
          for (int i = notAllLevel; i < node.getDimensionValues().size(); ++i) {
            sb.append(separator).append(node.getDimensionValues().get(i));
            separator = ".";
          }
          otherDimensionValues.get(parent).add(sb.toString());
          break;
        }
        ++levelDiff;
      }
    }
    //    Fill in the information of each response row
    for (HierarchyNode node : nodes) {
      SummaryResponseRow row = new SummaryResponseRow();
      row.names = nameTags.get(node).names;
      row.baselineValue = node.getBaselineValue();
      row.currentValue = node.getCurrentValue();
      row.percentageChange = computePercentageChange(row.baselineValue, row.currentValue);
      row.contributionChange =
          computeContributionChange(row.baselineValue, row.currentValue, totalBaselineValue, totalCurrentValue);
      row.contributionToOverallChange =
          computeContributionToOverallChange(row.baselineValue, row.currentValue, totalBaselineValue);
      StringBuilder sb = new StringBuilder();
      String separator = "";
      for (String s : otherDimensionValues.get(node)) {
        sb.append(separator).append(s);
        separator = ", ";
      }
      row.otherDimensionValues = sb.toString();
      response.responseRows.add(row);
    }

    return response;
  }

  private static String computePercentageChange(double baseline, double current) {
    if (baseline != 0d) {
      double percentageChange = ((current - baseline) / baseline) * 100d;
      return DOUBLE_FORMATTER.format(roundUp(percentageChange)) + "%";
    } else {
      return INFINITE;
    }
  }

  private static String computeContributionChange(double baseline, double current, double totalBaseline, double totalCurrent) {
    if (totalCurrent != 0d && totalBaseline != 0d) {
      double contributionChange = ((current / totalCurrent) - (baseline / totalBaseline)) * 100d;
      return DOUBLE_FORMATTER.format(roundUp(contributionChange)) + "%";
    } else {
      return INFINITE;
    }
  }

  private static String computeContributionToOverallChange(double baseline, double current, double totalBaseline) {
    if (totalBaseline != 0d) {
      double contributionToOverallChange = ((current - baseline) / (totalBaseline)) * 100d;
      return DOUBLE_FORMATTER.format(roundUp(contributionToOverallChange)) + "%";
    } else {
      return INFINITE;
    }
  }

  private static double roundUp(double number) {
    return Math.round(number * 100d) / 100d;
  }

  public String toString() {
    ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
    tsb.append('\n').append(this.dimensions);
    for (SummaryResponseRow row : getResponseRows()) {
      tsb.append('\n').append(row);
    }
    return tsb.toString();
  }

  private static class NameTag {
    private List<String> names;

    public NameTag(int levelCount) {
      names = new ArrayList<>(levelCount);
      for (int i = 0; i < levelCount; ++i) {
        names.add(ALL);
      }
    }

    public void copyNames(DimensionValues dimensionValues) {
      for (int i = 0; i < dimensionValues.size(); ++i) {
        names.set(i, dimensionValues.get(i));
      }
    }

    public void setNotAll(int index) {
      names.set(index, NOT_ALL);
    }
  }
}
