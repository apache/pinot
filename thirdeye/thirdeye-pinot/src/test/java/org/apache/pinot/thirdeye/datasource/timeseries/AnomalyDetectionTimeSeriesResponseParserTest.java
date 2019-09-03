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

package org.apache.pinot.thirdeye.datasource.timeseries;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.thirdeye.datasource.timeseries.BaseTimeSeriesResponseParserTest.buildMockedThirdEyeResponse;

public class AnomalyDetectionTimeSeriesResponseParserTest {
  @Test
  public void testParseGroupByTimeDimensionResponse() throws Exception {
    TimeSeriesResponseParser parser = new AnomalyDetectionTimeSeriesResponseParser();
    List<TimeSeriesRow> timeSeriesRows = parser.parseResponse(buildMockedThirdEyeResponse());

    Assert.assertTrue(CollectionUtils.isNotEmpty(timeSeriesRows));
    Assert.assertEquals(timeSeriesRows.size(), 9);

    Set<TimeSeriesRow> actualTimeSeriesRows = new HashSet<>(timeSeriesRows);
    Assert.assertEquals(actualTimeSeriesRows, getExpectedTimeSeriesRow());
  }

  static Set<TimeSeriesRow> getExpectedTimeSeriesRow() {
    Set<TimeSeriesRow> expectedTimeSeriesRows = new HashSet<>();

    List<String> dimensionNames = Arrays.asList("d1", "d2");
    {
      List<String> dimensionValues1 = Arrays.asList("d1v1", "d2v1");
      {
        TimeSeriesRow.Builder builder11 = new TimeSeriesRow.Builder();
        builder11.setStart(new DateTime(1));
        builder11.setEnd(new DateTime(2));
        builder11.setDimensionNames(dimensionNames);
        builder11.setDimensionValues(dimensionValues1);
        builder11.addMetric("m1", 100.0);
        builder11.addMetric("m2", 101.0);
        builder11.addMetric("m3", 102.0);
        expectedTimeSeriesRows.add(builder11.build());
      }
      {
        TimeSeriesRow.Builder builder12 = new TimeSeriesRow.Builder();
        builder12.setStart(new DateTime(2));
        builder12.setEnd(new DateTime(3));
        builder12.setDimensionNames(dimensionNames);
        builder12.setDimensionValues(dimensionValues1);
        builder12.addMetric("m1", 100.1);
        builder12.addMetric("m2", 101.1);
        builder12.addMetric("m3", 102.1);
        expectedTimeSeriesRows.add(builder12.build());
      }
      {
        TimeSeriesRow.Builder builder13 = new TimeSeriesRow.Builder();
        builder13.setStart(new DateTime(3));
        builder13.setEnd(new DateTime(4));
        builder13.setDimensionNames(dimensionNames);
        builder13.setDimensionValues(dimensionValues1);
        builder13.addMetric("m1", 100.2);
        builder13.addMetric("m2", 101.2);
        builder13.addMetric("m3", 102.2);
        expectedTimeSeriesRows.add(builder13.build());
      }
    }

    {
      List<String> dimensionValues1 = Arrays.asList("d1v2", "d2v2");
      {
        TimeSeriesRow.Builder builder21 = new TimeSeriesRow.Builder();
        builder21.setStart(new DateTime(1));
        builder21.setEnd(new DateTime(2));
        builder21.setDimensionNames(dimensionNames);
        builder21.setDimensionValues(dimensionValues1);
        builder21.addMetric("m1", 2.0);
        builder21.addMetric("m2", 2.1);
        builder21.addMetric("m3", 2.2);
        expectedTimeSeriesRows.add(builder21.build());
      }
      {
        TimeSeriesRow.Builder builder22 = new TimeSeriesRow.Builder();
        builder22.setStart(new DateTime(2));
        builder22.setEnd(new DateTime(3));
        builder22.setDimensionNames(dimensionNames);
        builder22.setDimensionValues(dimensionValues1);
        builder22.addMetric("m1", 2.01);
        builder22.addMetric("m2", 2.02);
        builder22.addMetric("m3", 2.03);
        expectedTimeSeriesRows.add(builder22.build());
      }
      {
        TimeSeriesRow.Builder builder23 = new TimeSeriesRow.Builder();
        builder23.setStart(new DateTime(3));
        builder23.setEnd(new DateTime(4));
        builder23.setDimensionNames(dimensionNames);
        builder23.setDimensionValues(dimensionValues1);
        builder23.addMetric("m1", 2.001);
        builder23.addMetric("m2", 2.002);
        builder23.addMetric("m3", 2.003);
        expectedTimeSeriesRows.add(builder23.build());
      }
    }

    {
      List<String> dimensionValues1 = Arrays.asList("d1v3", "d2v3");
      {
        TimeSeriesRow.Builder builder31 = new TimeSeriesRow.Builder();
        builder31.setStart(new DateTime(1));
        builder31.setEnd(new DateTime(2));
        builder31.setDimensionNames(dimensionNames);
        builder31.setDimensionValues(dimensionValues1);
        builder31.addMetric("m1", 3.0);
        builder31.addMetric("m2", 3.1);
        builder31.addMetric("m3", 3.2);
        expectedTimeSeriesRows.add(builder31.build());
      }
      {
        TimeSeriesRow.Builder builder32 = new TimeSeriesRow.Builder();
        builder32.setStart(new DateTime(2));
        builder32.setEnd(new DateTime(3));
        builder32.setDimensionNames(dimensionNames);
        builder32.setDimensionValues(dimensionValues1);
        builder32.addMetric("m1", 3.01);
        builder32.addMetric("m2", 3.02);
        builder32.addMetric("m3", 3.03);
        expectedTimeSeriesRows.add(builder32.build());
      }
      {
        TimeSeriesRow.Builder builder33 = new TimeSeriesRow.Builder();
        builder33.setStart(new DateTime(3));
        builder33.setEnd(new DateTime(4));
        builder33.setDimensionNames(dimensionNames);
        builder33.setDimensionValues(dimensionValues1);
        builder33.addMetric("m1", 3.001);
        builder33.addMetric("m2", 3.002);
        builder33.addMetric("m3", 3.003);
        expectedTimeSeriesRows.add(builder33.build());
      }
    }

    return expectedTimeSeriesRows;
  }
}
