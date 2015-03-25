package com.linkedin.thirdeye.resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.heatmap.ContributionDifferenceHeatMap;
import com.linkedin.thirdeye.heatmap.HeatMap;
import com.linkedin.thirdeye.heatmap.HeatMapCell;
import com.linkedin.thirdeye.heatmap.RGBColor;
import com.linkedin.thirdeye.heatmap.SelfRatioHeatMap;
import com.linkedin.thirdeye.heatmap.SnapshotHeatMap;
import com.linkedin.thirdeye.heatmap.VolumeHeatMap;


public class TestHeatMapResource {

  private String metricName;
  private long startMillis;
  private long endMillis;

  HeatMap heatMap;
  MetricSpec metricSpecs1, metricSpecs2;
  MetricSchema metricSchema;
  MetricTimeSeries metricTimeSeries;
  Map<String, MetricTimeSeries> timeSeriesByDimensionValue;
  List<HeatMapCell> actualHeatMapCells;
  List<TimeSeriesByDimensionValue> values;

  private int expectedSummationOldValue = 0, expectedSummationNewValue = 0;
  ArrayList<HeatMapCell> expectedCells;

  final RGBColor DOWN_COLOR = new RGBColor(252, 136, 138);
  final RGBColor UP_COLOR = new RGBColor(138, 252, 136);
  RGBColor EXPECTED_COLOR ;


  @BeforeClass
  public void beforeClass() throws Exception
  {
    metricName = "totalSales";
    startMillis = 0;
    endMillis = 100000;

    //Create sample data
    values = new ArrayList<TimeSeriesByDimensionValue>();
    values.add(new TimeSeriesByDimensionValue("USA", 100, 10000, 200, 30000));
    values.add(new TimeSeriesByDimensionValue("IND", 50, 6000, 70, 10000));
    values.add(new TimeSeriesByDimensionValue("CAN", 60, 7000.78, 20, 2000));
    values.add(new TimeSeriesByDimensionValue("BRA", 70, 900, 140, 20000));
    values.add(new TimeSeriesByDimensionValue("CHI", 20, 40000, 100, 50000));

    metricSpecs1 = new MetricSpec("totalSales", MetricType.INT);
    metricSpecs2 = new MetricSpec("totalProfit", MetricType.FLOAT);
    metricSchema = MetricSchema.fromMetricSpecs(Arrays.asList(metricSpecs1, metricSpecs2));

    timeSeriesByDimensionValue = new HashMap<String, MetricTimeSeries>();

    int dummyMetricValue = 50;

    //Create dummy metric time series
    for (TimeSeriesByDimensionValue value : values)
    {
      metricTimeSeries = new MetricTimeSeries(metricSchema);

      metricTimeSeries.set(startMillis, metricSpecs1.getName(), value.metric1Start);
      metricTimeSeries.set(startMillis + (startMillis + endMillis)/2,  metricSpecs1.getName(), dummyMetricValue);
      metricTimeSeries.set(endMillis, metricSpecs1.getName(), value.metric1End);
      metricTimeSeries.set(startMillis, metricSpecs2.getName(), value.metric2Start);
      metricTimeSeries.set(startMillis + (startMillis + endMillis)/2,  metricSpecs2.getName(), dummyMetricValue);
      metricTimeSeries.set(endMillis, metricSpecs2.getName(), value.metric2End);

      timeSeriesByDimensionValue.put(value.dimensionValue, metricTimeSeries);

      expectedSummationOldValue += value.metric1Start;
      expectedSummationNewValue += value.metric1End;

    }
  }


  @BeforeMethod
  public void beforeMethod() throws Exception
  {
     expectedCells = new ArrayList<HeatMapCell>();
  }


  @Test
  public void testHeatMapVolume() throws Exception
  {
    //Get actual heat map cells
    heatMap = new VolumeHeatMap();
    actualHeatMapCells = heatMap.generateHeatMap(metricName, timeSeriesByDimensionValue, startMillis, endMillis);

    //Calculate expected heat map cells
    for (TimeSeriesByDimensionValue value : values)
    {
      double expectedRatio = new Double(value.metric1End - value.metric1Start)/expectedSummationOldValue;

      HeatMapCell expectedCell = new HeatMapCell(
          value.dimensionValue, value.metric1End, value.metric1Start, "dummy label", expectedRatio, 0, null);
      expectedCells.add(expectedCell);
    }

    // Tests sorting of values
    Collections.sort(expectedCells, new Comparator<HeatMapCell>() {
      @Override
      public int compare(HeatMapCell o1, HeatMapCell o2) {
        return (int) (o2.getCurrent().doubleValue() - o1.getCurrent()
            .doubleValue());
      }
    });

    // Tests each cell's value
    Assert.assertEquals(expectedCells.size(), actualHeatMapCells.size());

    for (int i = 0; i < expectedCells.size(); i++)
    {
      Assert.assertEquals(expectedCells.get(i).getRatio(), actualHeatMapCells.get(i).getRatio(), 0.0001);
    }

    //Tests sum of all values equal to total change
    double actualSumOfAllCells = 0;
    double expectedTotalChange = 0;
    for (HeatMapCell actualCell : actualHeatMapCells )
    {
      actualSumOfAllCells += actualCell.getRatio();
    }

    expectedTotalChange = new Double(expectedSummationNewValue - expectedSummationOldValue)/expectedSummationOldValue;

    Assert.assertEquals(expectedTotalChange, actualSumOfAllCells, 0.0001);

  }


  @Test
  public void testHeatMapContributionDifference() throws Exception
  {

    //Get actual heat map cells
    heatMap = new ContributionDifferenceHeatMap(metricSpecs1.getType());
    actualHeatMapCells = heatMap.generateHeatMap(metricName, timeSeriesByDimensionValue, startMillis, endMillis);


    //Calculate expected heat map cells
    for (TimeSeriesByDimensionValue value : values)
    {
      double expectedRatio = (new Double(value.metric1End)/expectedSummationNewValue )
          -(new Double(value.metric1Start)/expectedSummationOldValue);

      EXPECTED_COLOR = (expectedRatio > 0) ? (UP_COLOR) : (DOWN_COLOR) ;

      HeatMapCell expectedCell = new HeatMapCell(
          value.dimensionValue, value.metric1End, value.metric1Start, "dummy label", expectedRatio, 0, EXPECTED_COLOR);
      expectedCells.add(expectedCell);
    }

    //Tests sorting of values
    Collections.sort(expectedCells, new Comparator<HeatMapCell>()
    {
      @Override
      public int compare(HeatMapCell o1, HeatMapCell o2)
      {
        return (int) ((o2.getRatio() - o1.getRatio()) * 100000);
      }
    });

    //Tests each cell's value
    Assert.assertEquals(expectedCells.size(), actualHeatMapCells.size());

    for (int i = 0; i < expectedCells.size(); i++)
    {
      Assert.assertEquals(expectedCells.get(i).getRatio(), actualHeatMapCells.get(i).getRatio(), 0.0001);
    }

    //Tests sum of all values equals zero
    double actualSumOfAllCells = 0;
    double expectedSumOfAllCells = 0;

    for (HeatMapCell actualCell : actualHeatMapCells)
    {
      actualSumOfAllCells += actualCell.getRatio();
    }

    Assert.assertEquals(expectedSumOfAllCells, actualSumOfAllCells, 0.0001);

  }


  @Test
  public void testHeatMapSelfRatio() throws Exception
  {
    //Get actual heat map cells
    heatMap = new SelfRatioHeatMap(metricSpecs1.getType());
    actualHeatMapCells = heatMap.generateHeatMap(metricName, timeSeriesByDimensionValue, startMillis, endMillis);

    //Calculate expected heat map cells
    for (TimeSeriesByDimensionValue value : values)
    {
      double expectedRatio = new Double(value.metric1End - value.metric1Start) / value.metric1Start;

      EXPECTED_COLOR = (expectedRatio > 0) ? (UP_COLOR) : (DOWN_COLOR) ;

      HeatMapCell expectedCell = new HeatMapCell(
          value.dimensionValue, value.metric1End, value.metric1Start, "dummy label", expectedRatio, 0, EXPECTED_COLOR);
      expectedCells.add(expectedCell);
    }

    //Tests sorting of values
    Collections.sort(expectedCells, new Comparator<HeatMapCell>()
    {
      @Override
      public int compare(HeatMapCell o1, HeatMapCell o2)
      {
        return (int) ((o2.getRatio() - o1.getRatio()) * 100000);
      }
    });

    //Tests each cell's value
    Assert.assertEquals(expectedCells.size(), actualHeatMapCells.size());

    for (int i = 0; i < expectedCells.size(); i++)
    {
      Assert.assertEquals(expectedCells.get(i).getRatio(), actualHeatMapCells.get(i).getRatio(), 0.0001);
    }

  }

  @Test
  public void testHeatMapSnapshot() throws Exception
  {
    heatMap = new SnapshotHeatMap(2);
    heatMap.generateHeatMap(metricName, timeSeriesByDimensionValue, startMillis, endMillis);
  }

  private class TimeSeriesByDimensionValue
  {
    String dimensionValue;
    int metric1Start;
    double metric2Start;
    int metric1End;
    double metric2End;

    TimeSeriesByDimensionValue(String dimensionValue,
        int metric1Start, double metric2Start, int metric1End, double metric2End)
    {
      this.dimensionValue = dimensionValue;
      this.metric1Start = metric1Start;
      this.metric2Start = metric2Start;
      this.metric1End = metric1End;
      this.metric2End = metric2End;
    }

  }

}
