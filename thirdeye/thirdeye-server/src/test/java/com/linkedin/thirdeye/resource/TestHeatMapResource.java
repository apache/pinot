package com.linkedin.thirdeye.resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  Map<String, TimeSeriesByDimensionValue> data;

  private int expectedSummationOldValue1 = 0, expectedSummationNewValue1 = 0, expectedSummationOldValue2 = 0, expectedSummationNewValue2 = 0;
  ArrayList<HeatMapCell> expectedCells;

  final RGBColor DOWN_COLOR = new RGBColor(252, 136, 138);
  final RGBColor UP_COLOR = new RGBColor(138, 252, 136);
  RGBColor EXPECTED_COLOR ;


  @BeforeClass
  public void beforeClass() throws Exception
  {

    startMillis = 0;
    endMillis = 100000;

    //Create sample data
    data = new HashMap<>();
    data.put("USA",new TimeSeriesByDimensionValue("USA", 100, 0, 200, 30000));
    data.put("IND",new TimeSeriesByDimensionValue("IND", 50, 0, 70, 0));
    data.put("CAN",new TimeSeriesByDimensionValue("CAN", 60, 0, 20, 6));
    data.put("BRA",new TimeSeriesByDimensionValue("BRA", 70, 900, 140, 20000));
    data.put("CHI",new TimeSeriesByDimensionValue("CHI", 20, 0, 100, 0));

    metricSpecs1 = new MetricSpec("totalSales", MetricType.INT);
    metricSpecs2 = new MetricSpec("totalProfit", MetricType.FLOAT);
    metricSchema = MetricSchema.fromMetricSpecs(Arrays.asList(metricSpecs1, metricSpecs2));

    timeSeriesByDimensionValue = new HashMap<String, MetricTimeSeries>();

    int dummyMetricValue = 50;

    //Create dummy metric time series
    for (TimeSeriesByDimensionValue value : data.values())
    {
      metricTimeSeries = new MetricTimeSeries(metricSchema);

      metricTimeSeries.set(startMillis, metricSpecs1.getName(), value.metric1Start);
      metricTimeSeries.set(startMillis + (startMillis + endMillis)/2,  metricSpecs1.getName(), dummyMetricValue);
      metricTimeSeries.set(endMillis, metricSpecs1.getName(), value.metric1End);
      metricTimeSeries.set(startMillis, metricSpecs2.getName(), value.metric2Start);
      metricTimeSeries.set(startMillis + (startMillis + endMillis)/2,  metricSpecs2.getName(), dummyMetricValue);
      metricTimeSeries.set(endMillis, metricSpecs2.getName(), value.metric2End);

      timeSeriesByDimensionValue.put(value.dimensionValue, metricTimeSeries);

      expectedSummationOldValue1 += value.metric1Start;
      expectedSummationNewValue1 += value.metric1End;

      expectedSummationOldValue2 += value.metric2Start;
      expectedSummationNewValue2 += value.metric2End;

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
    metricName = "totalSales";
    //Get actual heat map cells
    heatMap = new VolumeHeatMap();
    actualHeatMapCells = heatMap.generateHeatMap(metricName, timeSeriesByDimensionValue, startMillis, endMillis);

    //Calculate expected heat map cells
    for (TimeSeriesByDimensionValue value : data.values())
    {
      double expectedRatio = new Double(value.metric1End - value.metric1Start)/expectedSummationOldValue1;

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

    expectedTotalChange = new Double(expectedSummationNewValue1 - expectedSummationOldValue1)/expectedSummationOldValue1;

    Assert.assertEquals(expectedTotalChange, actualSumOfAllCells, 0.0001);

  }


  @Test
  public void testHeatMapContributionDifference() throws Exception
  {

    metricName = "totalSales";
    //Get actual heat map cells
    heatMap = new ContributionDifferenceHeatMap(metricSpecs1.getType());
    actualHeatMapCells = heatMap.generateHeatMap(metricName, timeSeriesByDimensionValue, startMillis, endMillis);


    //Calculate expected heat map cells
    for (TimeSeriesByDimensionValue value : data.values())
    {
      double expectedRatio = (new Double(value.metric1End)/expectedSummationNewValue1 )
          -(new Double(value.metric1Start)/expectedSummationOldValue1);

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
    metricName = "totalSales";
    //Get actual heat map cells
    heatMap = new SelfRatioHeatMap(metricSpecs1.getType());
    actualHeatMapCells = heatMap.generateHeatMap(metricName, timeSeriesByDimensionValue, startMillis, endMillis);

    //Calculate expected heat map cells
    for (TimeSeriesByDimensionValue value : data.values())
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
    metricName = "totalSales";
    //Get actual heat map cells
    heatMap = new SnapshotHeatMap(2);
    actualHeatMapCells = heatMap.generateHeatMap(metricName, timeSeriesByDimensionValue, startMillis, endMillis);

    //Check value of each cell
    double baselineSum = expectedSummationOldValue1;
    double currentSum = expectedSummationNewValue1;
    for (HeatMapCell cell : actualHeatMapCells)
    {
      if (!cell.getDimensionValue().equals("Rest"))
      {
        TimeSeriesByDimensionValue actualData = data.get(cell.getDimensionValue());
        Assert.assertEquals(cell.getRatio(), new Double(actualData.metric1End)/new Double(actualData.metric1Start) - 1, 0.0001);
        baselineSum = baselineSum - actualData.metric1Start;
        currentSum = currentSum - actualData.metric1End;
      }
      else
      {
        Assert.assertEquals(cell.getRatio(), new Double(currentSum)/baselineSum - 1, 0.0001);
      }
    }

    // Check edge cases : 
    // Cases where baseline value is zero 
    // Cases where Rest category has zero change
    metricName = "totalProfit";
    actualHeatMapCells = heatMap.generateHeatMap(metricName, timeSeriesByDimensionValue, startMillis, endMillis);

    baselineSum = expectedSummationOldValue2;
    currentSum = expectedSummationNewValue2;

    for (HeatMapCell cell : actualHeatMapCells)
    {
      if (!cell.getDimensionValue().equals("Rest"))
      {
        TimeSeriesByDimensionValue actualData = data.get(cell.getDimensionValue());
        Assert.assertEquals(cell.getRatio(), new Double(actualData.metric2End)/new Double(actualData.metric2Start) - 1, 0.0001);
        baselineSum = baselineSum - actualData.metric2Start;
        currentSum = currentSum - actualData.metric2End;
      }
      else
      {
        Assert.assertEquals(cell.getRatio(), new Double(currentSum)/baselineSum - 1, 0.0001);
      }
    }
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


