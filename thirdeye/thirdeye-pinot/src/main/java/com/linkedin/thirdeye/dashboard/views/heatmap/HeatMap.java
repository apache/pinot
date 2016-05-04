package com.linkedin.thirdeye.dashboard.views.heatmap;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HeatMap {

  private static final Logger LOG = LoggerFactory.getLogger(HeatMap.class);

  List<HeatMapCell> heatMapCells;

  private String dimensionName;

  public HeatMap(String dimensionName, List<HeatMapCell> heatMapCells) {
    this.dimensionName = dimensionName;
    this.heatMapCells = heatMapCells;
  }

  public static class Builder {
    List<String> dimensionValues = new ArrayList<>();
    DescriptiveStatistics baselineStats = new DescriptiveStatistics();

    DescriptiveStatistics currentStats = new DescriptiveStatistics();

    List<HeatMapCell> heatMapCells = new ArrayList<>();

    private String dimensionName;

    public Builder(String dimensionName) {
      this.dimensionName = dimensionName;

    }

    public void addCell(String dimensionValue, double baselineValue, double currentValue) {
      dimensionValues.add(dimensionValue);
      baselineStats.addValue(baselineValue);
      currentStats.addValue(currentValue);
    }

    HeatMap build() {
      int numCells = dimensionValues.size();
      NormalDistribution baselineDist = getDistribution(baselineStats);
      NormalDistribution currentDist = getDistribution(currentStats);

      double baselineTotal = baselineStats.getSum();
      double currentTotal = currentStats.getSum();
      for (int i = 0; i < numCells; i++) {
        String dimensionValue = dimensionValues.get(i);
        double baselineValue = baselineStats.getElement(i);
        double currentValue = currentStats.getElement(i);

        // contribution

        double baselineContribution = baselineValue *100/ baselineTotal;
        double currentContribution = currentValue * 100 / currentTotal;
        double baselineCDFValue = 0;
        if (baselineDist != null) {
          baselineCDFValue = baselineDist.cumulativeProbability(baselineValue);
        }
        double currentCDFValue = 0;
        if (currentDist != null) {
          currentCDFValue = currentDist.cumulativeProbability(currentValue);
        }
        double absoluteChange = (currentValue - baselineValue);

        double percentageChange = 0;
        if (baselineValue != 0) {
          percentageChange = ((currentValue - baselineValue) / baselineValue) * 100;
        } else {
          if (currentValue != 0) {
            percentageChange = -100;
          }
        }

        double contributionDifference = currentContribution - baselineContribution;

        double deltaColor = percentageChange;
        double deltaSize = currentValue;
        double contributionColor = (currentContribution - baselineContribution);
        double contributionSize = currentValue;
        double contributionToOverallChange = ((currentValue - baselineValue) / baselineTotal) * 100;
        double contributionToOverallColor = contributionToOverallChange;
        double contributionToOverallSize = currentValue;

        HeatMapCell cell = new HeatMapCell(dimensionValue, baselineValue, currentValue,
            baselineContribution, currentContribution, baselineCDFValue, currentCDFValue,
            percentageChange, absoluteChange, contributionDifference, contributionToOverallChange,
            deltaColor, deltaSize, contributionColor, contributionSize, contributionToOverallColor,
            contributionToOverallSize);
        heatMapCells.add(cell);
      }
      return new HeatMap(dimensionName, heatMapCells);
    }

    private NormalDistribution getDistribution(DescriptiveStatistics stats) {
      NormalDistribution dist = null;
      try {
        dist = new NormalDistribution(stats.getMean(), stats.getStandardDeviation());
      } catch (Exception e) {
        LOG.warn("Could not get statistics for dimension:{}, error message:{}", dimensionName,
            e.getMessage());
      }
      return dist;
    }

  }

}
