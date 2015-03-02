package com.linkedin.thirdeye.heatmap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VolumeHeatMap extends SimpleHeatMap
{
  private static final RGBColor COLOR = new RGBColor(136, 138, 252);

  @Override
  protected List<HeatMapCell> generateHeatMap(Map<String, Number> baseline, Map<String, Number> current)
  {
    List<HeatMapCell> cells = new ArrayList<HeatMapCell>();

    // Get stats about baseline
    Stats baselineStats = getStats(baseline);
    Stats currentStats = getStats(current);

    // Compute log of baseline
    Map<String, Number> logBaseline = new HashMap<String, Number>(baseline.size());
    for (Map.Entry<String, Number> entry : baseline.entrySet())
    {
      double value = entry.getValue().doubleValue();
      logBaseline.put(entry.getKey(), Math.log(value));
    }
    Stats logBaselineStats = getStats(logBaseline);

    // Use this to compute alpha of cells
    Map<String, Double> alphas = new HashMap<String, Double>();
    for (Map.Entry<String, Number> entry : logBaseline.entrySet())
    {
      double score = (entry.getValue().doubleValue() - logBaselineStats.getAverage()) / Math.sqrt(logBaselineStats.getVariance());
      alphas.put(entry.getKey(), normalCdf(score));
    }

    for (Map.Entry<String, Number> entry : current.entrySet())
    {
      Number currentMetricValue = entry.getValue();
      Number baselineMetricValue = baseline.get(entry.getKey());

      if (baselineMetricValue == null || baselineMetricValue.intValue() == 0)
      {
        continue; // nothing to compare to
      }

      double ratio = (currentMetricValue.doubleValue() - baselineMetricValue.doubleValue()) / baselineStats.getSum();

      // Construct cell label
      StringBuilder label = new StringBuilder();
      label.append("(baseline=").append(baselineMetricValue)
           .append(", current=").append(currentMetricValue);
      if (baselineStats.getSum() > 0)
      {
        label.append(", baselineRatio=").append(baselineMetricValue.doubleValue() / baselineStats.getSum());
      }
      if (currentStats.getSum() > 0)
      {
        label.append(", currentRatio=").append(currentMetricValue.doubleValue() / currentStats.getSum());
      }
      label.append(")");

      HeatMapCell cell = new HeatMapCell(entry.getKey(),
                                         currentMetricValue,
                                         baselineMetricValue,
                                         label.toString(),
                                         ratio,
                                         alphas.get(entry.getKey()),
                                         COLOR);

      cells.add(cell);
    }

    // Order by descending current value
    Collections.sort(cells, new Comparator<HeatMapCell>()
    {
      @Override
      public int compare(HeatMapCell o1, HeatMapCell o2)
      {
        return (int) (o2.getCurrent().doubleValue() - o1.getCurrent().doubleValue());
      }
    });

    return cells;
  }

  private static double normalCdf(double x)
  {
    double t = 1 / (1 + 0.2316419 * Math.abs(x));
    double d = 0.3989423 * Math.exp(-1 * x * x / 2);
    double p = d * t * (.3193815 + t * (-.3565638 + t * (1.781478 + t * (-1.821256 + t * 1.330274))));
    if (x > 0)
    {
      p = 1 - p;
    }
    return p;
  }

  private static Stats getStats(Map<String, Number> metrics)
  {
    double sum = 0;
    double sumSquares = 0;

    for (Map.Entry<String, Number> entry : metrics.entrySet())
    {
      if (Double.NEGATIVE_INFINITY != entry.getValue().doubleValue())
      {
        sum += entry.getValue().doubleValue();
        sumSquares += entry.getValue().doubleValue() * entry.getValue().doubleValue();
      }
    }

    double average = sum / metrics.size();
    double variance = (sumSquares - (sum * sum) / metrics.size()) / (metrics.size() - 1);

    return new Stats(sum, average, variance);
  }

  private static class Stats
  {
    private final double sum;
    private final double average;
    private final double variance;

    Stats(double sum, double average, double variance)
    {
      this.sum = sum;
      this.average = average;
      this.variance = variance;
    }

    public double getSum()
    {
      return sum;
    }

    public double getAverage()
    {
      return average;
    }

    public double getVariance()
    {
      return variance;
    }
  }
}
