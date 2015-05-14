package com.linkedin.thirdeye.dashboard.util;

import com.google.common.base.Joiner;
import com.linkedin.thirdeye.dashboard.api.QueryResult;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.Array2DRowRealMatrix;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.*;

public class SnapshotUtils {
  public static final String REST = "REST";

  private static final String SEP = "\t";
  private static final Joiner TSV = Joiner.on(SEP);

  /**
   * Calculates the compression loss.
   *
   * <p>
   *   We expect currentValue to be expectedGrowthRatio * baselineValue.
   *   The difference between these two quantities is the compression loss.
   * </p>
   *
   * @param expectedGrowthRatio
   *  The ratio by which we expect baselineValue to grow
   * @param baselineValue
   *  Starting value
   * @param currentValue
   *  Ending value
   * @param standardDeviation
   *  The standard deviation of the distribution of baseline values
   */
  private static double compressionLoss(double expectedGrowthRatio,
                                        double baselineValue,
                                        double currentValue,
                                        double standardDeviation) {
    NormalDistribution dist = new NormalDistribution(baselineValue * expectedGrowthRatio, standardDeviation);
    return -Math.log(dist.density(currentValue));
  }

  /**
   * Returns descriptive statistics for each metric for a set of dimension combinations.
   *
   * @param numMetrics
   *  The number of metric values
   * @param metricsByDimension
   *  Metrics grouped by dimension combination
   */
  private static DescriptiveStatistics[] getStats(int numMetrics, Map<String, Number[]> metricsByDimension) {
    if (metricsByDimension.isEmpty()) {
      throw new IllegalArgumentException("Cannot calculate statistics for no metrics");
    }

    DescriptiveStatistics[] stats = new DescriptiveStatistics[numMetrics];
    for (int i = 0; i < numMetrics; i++) {
      stats[i] = new DescriptiveStatistics();
    }

    for (Map.Entry<String, Number[]> entry : metricsByDimension.entrySet()) {
      Number[] metrics = entry.getValue();
      for (int i = 0; i < metrics.length; i++) {
        if (metrics[i].doubleValue() != Double.NEGATIVE_INFINITY) {
          stats[i].addValue(metrics[i].doubleValue());
        }
      }
    }

    return stats;
  }

  /**
   * Compute the initial growth ratio at the highest aggregation granularity.
   *
   * @param numMetrics
   *  The number of metric values
   * @param baseline
   *  Baseline data grouped by dimension
   * @param current
   *  Current data grouped by dimension
   */
  private static double[] getInitialRatios(int numMetrics,
                                           Map<String, Number[]> baseline,
                                           Map<String, Number[]> current) {
    double[] ratios = new double[numMetrics];
    for (int i = 0; i < numMetrics; i++) {
      double valueT1 = 0.0;
      double valueT2 = 0.0;
      for (Map.Entry<String, Number[]> entry : baseline.entrySet()) {
        String combination = entry.getKey();
        Number[] baselineData = entry.getValue();
        Number[] currentData = current.get(combination);

        if (currentData == null) {
          continue;
        }

        valueT1 += baselineData[i].doubleValue();
        valueT2 += currentData[i].doubleValue();
      }
      ratios[i] = valueT2 / valueT1;
    }
    return ratios;
  }

  /**
   * Returns the records that were biggest movers.
   *
   * @param metricIdx
   *  Only compute snapshot on this particular metric
   * @param maxRecords
   *  Return this many records
   * @param growthRatio
   *  The expected growth ratio from baseline to current
   * @param baseline
   *  Baseline data
   * @param current
   *  Current data
   */
  private static SnapshotResult computeOneSnapshot(int metricIdx,
                                                   int maxRecords,
                                                   double growthRatio,
                                                   Map<String, Number[]> baseline,
                                                   Map<String, Number[]> current) {
    int rowNum = baseline.size();
    int colNum = maxRecords + 1;
    RealMatrix table = new Array2DRowRealMatrix(rowNum, colNum);
    String[][] recordTable = new String[rowNum][colNum];
    String[] combinationTable = new String[rowNum];

    // Fill in combination table
    int idx = 0;
    for (String combination : baseline.keySet()) {
      combinationTable[idx] = combination;
      idx++;
    }

    // Compute the variance / standard deviation
    double variance = 0.0;
    for (int i = 0; i < rowNum; i++) {
      String combination = combinationTable[i];
      Number[] baselineData = baseline.get(combination);
      Number[] currentData = current.get(combination);
      if (currentData == null) {
        continue;
      }

      double baselineValue = baselineData[metricIdx].doubleValue();
      double currentValue = currentData[metricIdx].doubleValue();
      double diff = currentValue - growthRatio * baselineValue;
      variance += diff * diff;
    }
    double stDev = Math.sqrt(variance);

    // Update first column
    for (int i = 0; i < rowNum; i++) {
      String combination = combinationTable[i];
      Number[] baselineData = baseline.get(combination);
      Number[] currentData = current.get(combination);
      if (currentData == null) {
        continue;
      }

      double baselineValue = baselineData[metricIdx].doubleValue();
      double currentValue = currentData[metricIdx].doubleValue();
      double loss = compressionLoss(growthRatio, baselineValue, currentValue, stDev);
      if (i == 0) {
        table.setEntry(i, 0, loss);
      } else {
        table.setEntry(i, 0, table.getEntry(i - 1, 0) + loss);
      }

      recordTable[i][0] = REST;
    }

    // Update first row
    for (int j = 1; j < colNum; j++) {
      table.setEntry(0, j, 0);
      recordTable[0][j] = combinationTable[0];
    }

    // Update rest of columns
    for (int i = 1; i < rowNum; i++) {
      String combination = combinationTable[i];
      Number[] baselineData = baseline.get(combination);
      Number[] currentData = current.get(combination);
      if (currentData == null) {
        continue;
      }

      double baselineValue = baselineData[metricIdx].doubleValue();
      double currentValue = currentData[metricIdx].doubleValue();

      for (int j = 1; j < colNum; j++) {
        // D(T_(i+1), n, r) = min (D(T_i, n-1, r) + D(t_(i+1), 1, r), D(T_i, n, r) +D(t_(i+1), 0, r))
        double t1 = table.getEntry(i - 1, j - 1);
        double t2 = table.getEntry(i - 1, j) + compressionLoss(growthRatio, baselineValue, currentValue, stDev);
        table.setEntry(i, j, Math.min(t1, t2));

        // Record combinations
        if (t1 < t2) {
          recordTable[i][j] = TSV.join(combination, recordTable[i - 1][j - 1]);
        } else if (recordTable[i - 1][j - 1].contains(REST)) {
          recordTable[i][j] = recordTable[i - 1][j];
        } else {
          recordTable[i][j] = TSV.join(recordTable[i - 1][j], REST);
        }
      }
    }

    double cost = table.getEntry(rowNum - 1, colNum - 1);
    String[] records = recordTable[rowNum - 1][colNum - 1].split(SEP);
    return new SnapshotResult(cost, records);
  }

  /**
   * Computes snapshot for each metric.
   */
  private static String[][] computeSnapshot(int maxRecords,
                                            int numMetrics,
                                            Map<String, Number[]> baseline,
                                            Map<String, Number[]> current) {
    double[] ratios = getInitialRatios(numMetrics, baseline, current);
    String[][] records = new String[numMetrics][];

    for (int i = 0; i < numMetrics; i++) {
      double ratio = ratios[i];
      double lo = ratio / 2;
      double hi = 2 * ratio;
      int steps = 50;  // TODO Configurable? (was 100)
      double stepSize = (hi - lo) / steps;
      double minCost = computeOneSnapshot(i, maxRecords, lo, baseline, current).getCost();
      double minRatio = lo;

      for (int j = 1; j < steps; j++) {
        double currentRatio = lo + i * stepSize;
        double currentCost = computeOneSnapshot(i, maxRecords, currentRatio, baseline, current).getCost();
        if (currentCost < minCost) {
          minCost = currentCost;
          minRatio = currentRatio;
        }
      }

      records[i] = computeOneSnapshot(i, maxRecords, minRatio, baseline, current).getRecords();
    }

    return records;
  }

  /**
   * Clusters the biggest movers among all dimension combinations.
   */
  public static String[][] snapshot(int maxRecords, QueryResult queryResult) {
    Map<String, Number[]> baseline = new HashMap<>(queryResult.getData().size());
    Map<String, Number[]> current = new HashMap<>(queryResult.getData().size());
    for (Map.Entry<String, Map<String, Number[]>> entry : queryResult.getData().entrySet()) {
      if (entry.getValue().isEmpty()) {
        continue; // No data
      }

      // Find current / baseline time
      List<Long> times = new ArrayList<>(entry.getValue().size());
      for (String timeString : entry.getValue().keySet()) {
        times.add(Long.valueOf(timeString));
      }
      Long baselineTime = Collections.min(times);
      Long currentTime = Collections.max(times);

      // Get current / baseline data
      Number[] baselineData = entry.getValue().get(Long.toString(baselineTime));
      Number[] currentData = entry.getValue().get(Long.toString(currentTime));
      baseline.put(entry.getKey(), baselineData);
      current.put(entry.getKey(), currentData);
    }

    // Compute snapshot
    return computeSnapshot(maxRecords, queryResult.getMetrics().size(), baseline, current);
  }

  private static class SnapshotResult {
    private final double cost;
    private final String[] records;

    SnapshotResult(double cost, String[] records) {
      this.cost = cost;
      this.records = records;
    }

    double getCost() {
      return cost;
    }

    String[] getRecords() {
      return records;
    }
  }
}
