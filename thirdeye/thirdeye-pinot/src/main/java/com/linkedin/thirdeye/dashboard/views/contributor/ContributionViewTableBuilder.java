/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.dashboard.views.contributor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.linkedin.thirdeye.dashboard.views.TimeBucket;

public class ContributionViewTableBuilder {

  boolean finished = false;
  TreeSet<TimeBucket> timeBuckets;
  Map<TimeBucket, DescriptiveStatistics> baselineStatsMap;
  Map<TimeBucket, DescriptiveStatistics> currentStatsMap;
  Map<TimeBucket, DescriptiveStatistics> cumulativeBaselineStatsMap;
  Map<TimeBucket, DescriptiveStatistics> cumulativeCurrentStatsMap;
  Set<String> dimensionValueSet;
  List<ContributionCell> cells;
  private String metricName;
  private String dimensionName;
  Map<TimeBucket, Map<String, ContributionCell>> timeBucketToDimensionValuesMap;

  public ContributionViewTableBuilder(String metricName, String dimensionName) {
    this.metricName = metricName;
    this.dimensionName = dimensionName;
    timeBuckets = new TreeSet<>();
    baselineStatsMap = new TreeMap<>();
    currentStatsMap = new TreeMap<>();
    cumulativeBaselineStatsMap = new TreeMap<>();
    cumulativeCurrentStatsMap = new TreeMap<>();
    dimensionValueSet = new TreeSet<>();
    cells = new ArrayList<>();
    timeBucketToDimensionValuesMap = new HashMap<>();
  }

  public void addEntry(String dimensionValue, TimeBucket timeBucket, Double baselineValue,
      Double currentValue, Double cumulativeBaselineValue, Double cumulativeCurrentValue) {
    if (finished) {
      throw new RuntimeException("Cannot add more entries since the view is already created");
    }
    DescriptiveStatistics baselineStats;
    DescriptiveStatistics cumulativeCurrentStats;
    DescriptiveStatistics currentStats;
    DescriptiveStatistics cumulativeBaselineStats;

    timeBuckets.add(timeBucket);
    dimensionValueSet.add(dimensionValue);
    baselineStats = getStats(baselineStatsMap, timeBucket);
    currentStats = getStats(currentStatsMap, timeBucket);
    cumulativeBaselineStats = getStats(cumulativeBaselineStatsMap, timeBucket);
    cumulativeCurrentStats = getStats(cumulativeCurrentStatsMap, timeBucket);

    baselineStats.addValue(baselineValue);
    currentStats.addValue(currentValue);
    cumulativeBaselineStats.addValue(cumulativeBaselineValue);
    cumulativeCurrentStats.addValue(cumulativeCurrentValue);
    ContributionCell contributionCell = new ContributionCell(dimensionValue, timeBucket,
        baselineValue, currentValue, cumulativeBaselineValue, cumulativeCurrentValue);
    cells.add(contributionCell);

    Map<String, ContributionCell> map = timeBucketToDimensionValuesMap.get(timeBucket);
    if (map == null) {
      map = new HashMap<>();
      timeBucketToDimensionValuesMap.put(timeBucket, map);
    }
    map.put(dimensionValue, contributionCell);

  }

  private DescriptiveStatistics getStats(Map<TimeBucket, DescriptiveStatistics> statsMap,
      TimeBucket timeBucket) {
    DescriptiveStatistics stats = statsMap.get(timeBucket);
    if (stats == null) {
      stats = new DescriptiveStatistics();
      statsMap.put(timeBucket, stats);
    }
    return stats;
  }

  public ContributionViewTable build() {
    finished = true;
    for (String dimensionValue : dimensionValueSet) {
      ContributionCell prevCell = null;
      for (TimeBucket timeBucket : timeBuckets) {
        
        double baselineTotal = baselineStatsMap.get(timeBucket).getSum();
        double currentTotal = currentStatsMap.get(timeBucket).getSum();
        double cumulativeBaselineTotal = cumulativeBaselineStatsMap.get(timeBucket).getSum();
        double cumulativeCurrentTotal = cumulativeCurrentStatsMap.get(timeBucket).getSum();

        ContributionCell cell = timeBucketToDimensionValuesMap.get(timeBucket).get(dimensionValue);
        if (cell == null) {
          double cumulativeBaselineValue = 0;
          double cumulativeCurrentValue = 0;
          if (prevCell != null) {
            cumulativeBaselineValue = prevCell.getCumulativeBaselineValue();
            cumulativeCurrentValue = prevCell.getCumulativeCurrentValue();
          }
          cell = new ContributionCell(dimensionValue, timeBucket, 0, 0, cumulativeBaselineValue,
              cumulativeCurrentValue);
          cells.add(cell);
        }
        cell.updateContributionStats(baselineTotal, currentTotal, cumulativeBaselineTotal,
            cumulativeCurrentTotal);
      }
    }
    return new ContributionViewTable(metricName, dimensionName, cells);
  }

  public String getMetricName() {
    return metricName;
  }

  public String getDimensionName() {
    return dimensionName;
  }
}
