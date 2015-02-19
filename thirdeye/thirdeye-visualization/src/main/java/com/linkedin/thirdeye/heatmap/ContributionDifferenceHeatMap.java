package com.linkedin.thirdeye.heatmap;

import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.impl.NumberUtils;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ContributionDifferenceHeatMap extends SimpleHeatMap
{
  private static final RGBColor DOWN_COLOR = new RGBColor(252, 136, 138);
  private static final RGBColor UP_COLOR = new RGBColor(138, 252, 136);

  private final MetricType metricType;

  public ContributionDifferenceHeatMap(MetricType metricType)
  {
    this.metricType = metricType;
  }

  @Override
  protected List<HeatMapCell> generateHeatMap(Map<String, Number> baseline, Map<String, Number> current)
  {
    List<HeatMapCell> cells = new ArrayList<HeatMapCell>();

    DescriptiveStatistics positiveStats = new DescriptiveStatistics();
    DescriptiveStatistics negativeStats = new DescriptiveStatistics();

    Number baselineSum = 0;
    Number currentSum = 0;

    for (Number value : baseline.values())
    {
      baselineSum = NumberUtils.sum(baselineSum, value, metricType);
    }

    for (Number value : current.values())
    {
      currentSum = NumberUtils.sum(currentSum, value, metricType);
    }

    for (Map.Entry<String, Number> entry : current.entrySet())
    {
      Number currentValue = entry.getValue();
      Number baselineValue = baseline.get(entry.getKey());

      if (baselineValue == null || NumberUtils.isZero(baselineValue, metricType))
      {
        continue;
      }

      double ratio = currentValue.doubleValue() / currentSum.doubleValue() - baselineValue.doubleValue() / baselineSum.doubleValue();

      RGBColor color;
      if (ratio > 0)
      {
        positiveStats.addValue(ratio);
        color = UP_COLOR;
      }
      else
      {
        negativeStats.addValue(ratio);
        color = DOWN_COLOR;
      }

      HeatMapCell cell = new HeatMapCell();
      cell.setDimensionValue(entry.getKey());
      cell.setCurrent(currentValue);
      cell.setBaseline(baselineValue);
      cell.setRatio(ratio);
      cell.setColor(color);

      cells.add(cell);
    }

    NormalDistribution positiveDist = positiveStats.getStandardDeviation() > 0
            ? new NormalDistribution(positiveStats.getMean(), positiveStats.getStandardDeviation())
            : null;
    NormalDistribution negativeDist = negativeStats.getStandardDeviation() > 0
            ? new NormalDistribution(negativeStats.getMean(), negativeStats.getStandardDeviation())
            : null;

    for (HeatMapCell cell : cells)
    {
      if (cell.getRatio() > 0)
      {
        cell.setAlpha(positiveDist == null ? 0.5 : positiveDist.cumulativeProbability(cell.getRatio()));
      }
      else
      {
        cell.setAlpha(negativeDist == null ? 0.5 : 1 - negativeDist.cumulativeProbability(cell.getRatio()));
      }
    }

    Collections.sort(cells, new Comparator<HeatMapCell>()
    {
      @Override
      public int compare(HeatMapCell o1, HeatMapCell o2)
      {
        return (int) ((o2.getRatio() - o1.getRatio()) * 100000);
      }
    });

    return cells;
  }
}
