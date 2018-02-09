package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponseRow;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class CSVThirdEyeResponseTest {
  CSVThirdEyeResponse response;
  @BeforeMethod
  public void beforeMethod(){
    ThirdEyeRequest request = ThirdEyeRequest.
        newBuilder()
        .setStartTimeInclusive(0)
        .setEndTimeExclusive(100)
        .addGroupBy("country")
        .addMetricFunction(new MetricFunction(MetricAggFunction.AVG, "views", 0l, "source", null, null))
        .build("");
    response = new CSVThirdEyeResponse(
        request,
        new TimeSpec("timestamp", new TimeGranularity(1, TimeUnit.HOURS), TimeSpec.SINCE_EPOCH_FORMAT),
        new DataFrame().addSeries("timestamp", 100).addSeries("country", "us")
        .addSeries("AVG_views", 1000)
    );
  }

  @Test
  public void testGetNumRows(){
    Assert.assertEquals(response.getNumRows(), 1);
  }


  @Test
  public void testGetRow(){
    ThirdEyeResponseRow responseRow = response.getRow(0);
    Assert.assertEquals(responseRow.getDimensions(), Arrays.asList("us"));
    Assert.assertEquals(responseRow.getMetrics(), Arrays.asList(1000.0));
    Assert.assertEquals(responseRow.getTimeBucketId(), 0);
  }

  @Test
  public void testGetNumRowsFor() {
    Assert.assertEquals(response.getNumRows(), 1);
  }

  @Test
  public void testGetRowMap(){
    Map<String, String> map = new HashMap<>();
    map.put("country", "us");
    map.put("AVG_views", "1000");
    Assert.assertEquals(
        response.getRow(
            new MetricFunction(MetricAggFunction.AVG, "views", 0l, "source", null, null),
        0), map);
  }

}
