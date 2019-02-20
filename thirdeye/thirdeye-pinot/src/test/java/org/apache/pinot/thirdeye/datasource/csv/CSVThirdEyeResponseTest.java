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

package org.apache.pinot.thirdeye.datasource.csv;

import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponseRow;
import java.util.Collections;
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
        .addMetricFunction(new MetricFunction(MetricAggFunction.AVG, "views", 0L, "source", null, null))
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
    Assert.assertEquals(responseRow.getDimensions(), Collections.singletonList("us"));
    Assert.assertEquals(responseRow.getMetrics(), Collections.singletonList(1000.0));
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
            new MetricFunction(MetricAggFunction.AVG, "views", 0L, "source", null, null),
        0), map);
  }

}
