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

package org.apache.pinot.thirdeye.dashboard.views.heatmap;

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
    DescriptiveStatistics numeratorBaselineStats = new DescriptiveStatistics();
    DescriptiveStatistics numeratorCurrentStats = new DescriptiveStatistics();
    DescriptiveStatistics denominatorBaselineStats = new DescriptiveStatistics();
    DescriptiveStatistics denominatorCurrentStats = new DescriptiveStatistics();

    DescriptiveStatistics cellSizeStats = new DescriptiveStatistics();
    String cellSizeExpression = null;

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

    public void addCell(String dimensionValue, double baselineValue, double currentValue, Double cellSize, String cellSizeExpression,
        Double numeratorBaseline, Double denominatorBaseline, Double numeratorCurrent, Double denominatorCurrent) {
      addCell(dimensionValue, baselineValue, currentValue);
      cellSizeStats.addValue(cellSize);
      this.cellSizeExpression = cellSizeExpression;
      numeratorBaselineStats.addValue(numeratorBaseline);
      numeratorCurrentStats.addValue(numeratorCurrent);
      denominatorBaselineStats.addValue(denominatorBaseline);
      denominatorCurrentStats.addValue(denominatorCurrent);
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
        double numeratorBaseline = numeratorBaselineStats.getValues().length == 0 ? 0 : numeratorBaselineStats.getElement(i);
        double numeratorCurrent = numeratorCurrentStats.getValues().length == 0 ? 0 : numeratorCurrentStats.getElement(i);
        double denominatorBaseline = denominatorBaselineStats.getValues().length == 0 ? 0 : denominatorBaselineStats.getElement(i);
        double denominatorCurrent = denominatorCurrentStats.getValues().length == 0 ? 0 : denominatorCurrentStats.getElement(i);

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
        if (Double.compare(baselineValue, 0d) != 0) {
          percentageChange = ((currentValue - baselineValue) / baselineValue) * 100;
        } else {
          if (Double.compare(currentValue, 0d) != 0) {
            percentageChange = 100;
          }
        }

        double contributionDifference = currentContribution - baselineContribution;

        double cellSize = cellSizeStats.getValues().length == 0 ? (currentValue + baselineValue) / 2 : cellSizeStats.getElement(i);
        double deltaColor = percentageChange;
        double deltaSize = cellSize;
        double contributionColor = (currentContribution - baselineContribution);
        double contributionSize = cellSize;
        double contributionToOverallChange = ((currentValue - baselineValue) / baselineTotal) * 100;
        double contributionToOverallColor = contributionToOverallChange;
        double contributionToOverallSize = cellSize;

        HeatMapCell cell = new HeatMapCell(dimensionValue, baselineValue, currentValue,
            numeratorBaseline, denominatorBaseline, numeratorCurrent, denominatorCurrent,
            baselineContribution, currentContribution, baselineCDFValue, currentCDFValue,
            percentageChange, absoluteChange, contributionDifference, contributionToOverallChange,
            deltaColor, deltaSize, contributionColor, contributionSize, contributionToOverallColor,
            contributionToOverallSize, cellSizeExpression);
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
