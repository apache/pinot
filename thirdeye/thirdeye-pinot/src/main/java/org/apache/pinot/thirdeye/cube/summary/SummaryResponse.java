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

package org.apache.pinot.thirdeye.cube.summary;

import org.apache.pinot.thirdeye.cube.data.cube.Cube;
import org.apache.pinot.thirdeye.cube.cost.CostFunction;
import org.apache.pinot.thirdeye.cube.data.cube.DimNameValueCostEntry;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.thirdeye.cube.data.dbrow.DimensionValues;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;
import org.apache.pinot.thirdeye.cube.data.node.CubeNode;
import org.apache.pinot.thirdeye.cube.ratio.RatioCubeNode;


public class SummaryResponse {
  private final static int MAX_GAINER_LOSER_COUNT = 5;
  private final static NumberFormat DOUBLE_FORMATTER = new DecimalFormat("#0.00");
  static final  String INFINITE = "";

  static final String ALL = "(ALL)";
  static final String NOT_ALL = "(ALL)-";
  static final String NOT_AVAILABLE = "-na-";

  @JsonProperty("dataset")
  private String dataset;

  @JsonProperty("metricName")
  private String metricName;

  @JsonProperty("baselineTotal")
  private double baselineTotal = 0d;

  @JsonProperty("currentTotal")
  private double currentTotal = 0d;

  @JsonProperty("baselineTotalSize")
  private double baselineTotalSize = 0d;

  @JsonProperty("currentTotalSize")
  private double currentTotalSize = 0d;

  @JsonProperty("globalRatio")
  private double globalRatio = 1d;

  @JsonProperty("dimensions")
  private List<String> dimensions = new ArrayList<>();

  @JsonProperty("responseRows")
  private List<SummaryResponseRow> responseRows = new ArrayList<>();

  @JsonProperty("gainer")
  private List<SummaryGainerLoserResponseRow> gainer = new ArrayList<>();

  @JsonProperty("loser")
  private List<SummaryGainerLoserResponseRow> loser = new ArrayList<>();

  @JsonProperty("dimensionCosts")
  private List<Cube.DimensionCost> dimensionCosts = new ArrayList<>();

  public String getDataset() {
    return dataset;
  }

  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public String getMetricName() {
    return metricName;
  }

  public void setMetricName(String metricName) {
    this.metricName = metricName;
  }

  public List<SummaryResponseRow> getResponseRows() {
    return responseRows;
  }

  public List<SummaryGainerLoserResponseRow> getGainer() {
    return gainer;
  }

  public List<SummaryGainerLoserResponseRow> getLoser() {
    return loser;
  }

  public List<Cube.DimensionCost> getDimensionCosts() {
    return dimensionCosts;
  }

  public void setDimensionCosts(List<Cube.DimensionCost> dimensionCosts) {
    this.dimensionCosts = dimensionCosts;
  }

  public static SummaryResponse buildNotAvailableResponse(String dataset, String metricName) {
    SummaryResponse response = new SummaryResponse();
    response.setDataset(dataset);
    response.setMetricName(metricName);
    response.dimensions.add(NOT_AVAILABLE);
    return response;
  }

  public void buildGainerLoserGroup(List<DimNameValueCostEntry> costSet) {
    for (DimNameValueCostEntry dimNameValueCostEntry : costSet) {
      if (Double.compare(dimNameValueCostEntry.getCost(), 0d) <= 0) {
        continue;
      }
      if (dimNameValueCostEntry.getCurrentValue() >= dimNameValueCostEntry.getBaselineValue()
          && gainer.size() < MAX_GAINER_LOSER_COUNT) {
        gainer.add(buildGainerLoserRow(dimNameValueCostEntry));
      } else if (dimNameValueCostEntry.getCurrentValue() < dimNameValueCostEntry.getBaselineValue()
          && loser.size() < MAX_GAINER_LOSER_COUNT) {
        loser.add(buildGainerLoserRow(dimNameValueCostEntry));
      }
      if (gainer.size() >= MAX_GAINER_LOSER_COUNT && loser.size() >= MAX_GAINER_LOSER_COUNT) {
        break;
      }
    }
  }

  private SummaryGainerLoserResponseRow buildGainerLoserRow(DimNameValueCostEntry costEntry) {
    SummaryGainerLoserResponseRow row = new SummaryGainerLoserResponseRow();
    row.baselineValue = costEntry.getBaselineValue();
    row.currentValue = costEntry.getCurrentValue();
    row.baselineSize = costEntry.getBaselineSize();
    row.currentSize = costEntry.getCurrentSize();
    row.dimensionName = costEntry.getDimName();
    row.dimensionValue = costEntry.getDimValue();
    row.percentageChange = computePercentageChange(row.baselineValue, row.currentValue);
    row.contributionChange =
        computeContributionChange(row.baselineValue, row.currentValue, baselineTotal, currentTotal);
    row.contributionToOverallChange =
        computeContributionToOverallChange(row.baselineValue, row.currentValue, baselineTotal, currentTotal);
    row.cost = DOUBLE_FORMATTER.format(roundUp(costEntry.getCost()));
    return row;
  }

  public void buildDiffSummary(List<CubeNode> nodes, int targetLevelCount, CostFunction costFunction) {
    // Compute the total baseline and current value

    double baselineNumerator = 0d;
    double baselineDenominator = 0d;
    double currentNumerator = 0d;
    double currentDenominator = 0d;
    boolean isRatio = false;
    for(CubeNode node : nodes) {
      if (node instanceof RatioCubeNode) {
        RatioCubeNode ratioNode = (RatioCubeNode) node;
        baselineNumerator += ratioNode.getBaselineNumeratorValue();
        baselineDenominator += ratioNode.getBaselineDenominatorValue();
        currentNumerator += ratioNode.getCurrentNumeratorValue();
        currentDenominator += ratioNode.getCurrentDenominatorValue();
        ((RatioCubeNode) node).computeStatus();
        isRatio = true;
      } else {
        baselineTotal += node.getBaselineValue();
        baselineTotalSize += node.getBaselineValue();
        currentTotal += node.getCurrentValue();
        currentTotalSize += node.getCurrentValue();
      }
    }
    if (isRatio) {
      baselineTotal = baselineNumerator / baselineDenominator;
      currentTotal = currentNumerator / currentDenominator;
      baselineTotalSize = baselineNumerator + baselineDenominator;
      currentTotalSize = currentNumerator + currentDenominator;
    }
    if (Double.compare(baselineTotal, 0d) != 0) {
      globalRatio = roundUp(currentTotal / baselineTotal);
    }

    // If all nodes have a lower level count than targetLevelCount, then it is not necessary to print the summary with
    // height higher than the available level.
    int maxNodeLevelCount = 0;
    for (CubeNode node : nodes) {
      maxNodeLevelCount = Math.max(maxNodeLevelCount, node.getLevel());
    }
    targetLevelCount = Math.min(maxNodeLevelCount, targetLevelCount);

    // Build the header
    Dimensions dimensions = nodes.get(0).getDimensions();
    for (int i = 0; i < dimensions.size(); ++i) {
      this.dimensions.add(dimensions.get(i));
    }

    // Build the response
    nodes = SummaryResponseTree.sortResponseTree(nodes, targetLevelCount, costFunction);
    //   Build name tag for each row of responses
    Map<CubeNode, NameTag> nameTags = new HashMap<>();
    Map<CubeNode, List<String>> otherDimensionValues = new HashMap<>();
    for (CubeNode node : nodes) {
      NameTag tag = new NameTag(targetLevelCount);
      nameTags.put(node, tag);
      tag.copyNames(node.getDimensionValues());
      otherDimensionValues.put(node, new ArrayList<String>());
    }
    //   pre-condition: parent node is processed before its children nodes
    for (CubeNode node : nodes) {
      CubeNode parent = node;
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
    for (CubeNode node : nodes) {
      SummaryResponseRow row = new SummaryResponseRow();
      row.names = nameTags.get(node).names;
      row.baselineValue = node.getBaselineValue();
      row.currentValue = node.getCurrentValue();
      row.percentageChange = computePercentageChange(row.baselineValue, row.currentValue);
      row.baselineSize = node.getBaselineSize();
      row.currentSize = node.getCurrentSize();
      row.contributionChange =
          computeContributionChange(row.baselineValue, row.currentValue, baselineTotal, currentTotal);
      row.contributionToOverallChange =
          computeContributionToOverallChange(row.baselineValue, row.currentValue, baselineTotal, currentTotal);
      row.cost = node.getCost();
      StringBuilder sb = new StringBuilder();
      String separator = "";
      for (String s : otherDimensionValues.get(node)) {
        sb.append(separator).append(s);
        separator = ", ";
      }
      row.otherDimensionValues = sb.toString();
      this.responseRows.add(row);
    }
  }

  private static String computePercentageChange(double baseline, double current) {
    if (baseline != 0d) {
      double percentageChange = ((current - baseline) / baseline) * 100d;
      return DOUBLE_FORMATTER.format(roundUp(percentageChange)) + "%";
    } else {
      return INFINITE;
    }
  }

  private static String computeContributionChange(double baseline, double current, double baselineTotal,
      double currentTotal) {
    if (currentTotal != 0d && baselineTotal != 0d) {
      double contributionChange = ((current / currentTotal) - (baseline / baselineTotal)) * 100d;
      return DOUBLE_FORMATTER.format(roundUp(contributionChange)) + "%";
    } else {
      return INFINITE;
    }
  }

  private static String computeContributionToOverallChange(double baseline, double current, double baselineTotal,
      double currentTotal) {
    if (baselineTotal != 0d) {
      double contributionToOverallChange = ((current - baseline) / Math.abs(currentTotal - baselineTotal)) * 100d;
      return DOUBLE_FORMATTER.format(roundUp(contributionToOverallChange)) + "%";
    } else {
      return INFINITE;
    }
  }

  private static double roundUp(double number) {
    return Math.round(number * 100d) / 100d;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder(SummaryResponse.class.getSimpleName());
    sb.append("\n\t").append(this.dimensions);
    for (SummaryResponseRow row : getResponseRows()) {
      sb.append("\n\t").append(row);
    }
    return sb.toString();
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
