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

import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.constant.MetricAggFunction;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponseRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * A base test class for providing the mocked ThirdEye response which could be used to test Response Parsers.
 */
public class BaseTimeSeriesResponseParserTest {

  @Test
  public void testBuildMockedThirdEyeResponse() {
    MockedThirdEyeResponse mockedThirdEyeResponse = buildMockedThirdEyeResponse();
    Assert.assertNotNull(mockedThirdEyeResponse);
    Assert.assertEquals(mockedThirdEyeResponse.getNumRows(), 9);
  }

  /**
   * Returns a ThirdEye Response for testing purpose.
   *
   * @return a ThirdEye Response for testing purpose.
   */
  static MockedThirdEyeResponse buildMockedThirdEyeResponse() {
    MockedThirdEyeResponse thirdEyeResponse = new MockedThirdEyeResponse();

    {
      int timeBucketId = 0;
      ThirdEyeResponseRow row1 = new ThirdEyeResponseRow(timeBucketId++, getDimensionValueList1(), getMetricT1DV1());
      ThirdEyeResponseRow row2 = new ThirdEyeResponseRow(timeBucketId++, getDimensionValueList1(), getMetricT2DV1());
      ThirdEyeResponseRow row3 = new ThirdEyeResponseRow(timeBucketId++, getDimensionValueList1(), getMetricT3DV1());
      thirdEyeResponse.addRow(row1);
      thirdEyeResponse.addRow(row2);
      thirdEyeResponse.addRow(row3);
    }

    {
      int timeBucketId = 0;
      ThirdEyeResponseRow row1 = new ThirdEyeResponseRow(timeBucketId++, getDimensionValueList2(), getMetricT1DV2());
      ThirdEyeResponseRow row2 = new ThirdEyeResponseRow(timeBucketId++, getDimensionValueList2(), getMetricT2DV2());
      ThirdEyeResponseRow row3 = new ThirdEyeResponseRow(timeBucketId++, getDimensionValueList2(), getMetricT3DV2());
      thirdEyeResponse.addRow(row1);
      thirdEyeResponse.addRow(row2);
      thirdEyeResponse.addRow(row3);
    }

    {
      int timeBucketId = 0;
      ThirdEyeResponseRow row1 = new ThirdEyeResponseRow(timeBucketId++, getDimensionValueList3(), getMetricT1DV3());
      ThirdEyeResponseRow row2 = new ThirdEyeResponseRow(timeBucketId++, getDimensionValueList3(), getMetricT2DV3());
      ThirdEyeResponseRow row3 = new ThirdEyeResponseRow(timeBucketId++, getDimensionValueList3(), getMetricT3DV3());
      thirdEyeResponse.addRow(row1);
      thirdEyeResponse.addRow(row2);
      thirdEyeResponse.addRow(row3);
    }

    List<MetricFunction> metricFunctions = getMetricFunctions();
    thirdEyeResponse.setMetricFunctions(metricFunctions);

    List<String> dimensionNames = getDimensionNames();

    ThirdEyeRequest.ThirdEyeRequestBuilder requestBuilder = new ThirdEyeRequest.ThirdEyeRequestBuilder();
    requestBuilder.setStartTimeInclusive(1);
    requestBuilder.setEndTimeExclusive(4);
    requestBuilder.setMetricFunctions(metricFunctions);
    requestBuilder.setGroupBy(dimensionNames);
    requestBuilder.setGroupByTimeGranularity(new TimeGranularity(1, TimeUnit.MILLISECONDS));
    ThirdEyeRequest thirdEyeRequest = requestBuilder.build("test");
    thirdEyeResponse.setThirdEyeRequest(thirdEyeRequest);
    List<String> groupKeyColumns = new ArrayList<>();
    groupKeyColumns.add("time");
    groupKeyColumns.addAll(dimensionNames);
    thirdEyeResponse.setGroupKeyColumns(groupKeyColumns);

    return thirdEyeResponse;
  }

  private static List<Double> getMetricT1DV1() {
    return new ArrayList<Double>() {{
      add(100.0);
      add(101.0);
      add(102.0);
    }};
  }

  private static List<Double> getMetricT2DV1() {
    return new ArrayList<Double>() {{
      add(100.1);
      add(101.1);
      add(102.1);
    }};
  }

  private static List<Double> getMetricT3DV1() {
    return new ArrayList<Double>() {{
      add(100.2);
      add(101.2);
      add(102.2);
    }};
  }

  private static List<Double> getMetricT1DV2() {
    return new ArrayList<Double>() {{
      add(2.0);
      add(2.1);
      add(2.2);
    }};
  }

  private static List<Double> getMetricT2DV2() {
    return new ArrayList<Double>() {{
      add(2.01);
      add(2.02);
      add(2.03);
    }};
  }

  private static List<Double> getMetricT3DV2() {
    return new ArrayList<Double>() {{
      add(2.001);
      add(2.002);
      add(2.003);
    }};
  }

  private static List<Double> getMetricT1DV3() {
    return new ArrayList<Double>() {{
      add(3.0);
      add(3.1);
      add(3.2);
    }};
  }

  private static List<Double> getMetricT2DV3() {
    return new ArrayList<Double>() {{
      add(3.01);
      add(3.02);
      add(3.03);
    }};
  }

  private static List<Double> getMetricT3DV3() {
    return new ArrayList<Double>() {{
      add(3.001);
      add(3.002);
      add(3.003);
    }};
  }

  private static List<MetricFunction> getMetricFunctions() {
    final MetricFunction function1 = new MetricFunction();
    function1.setMetricName("m1");
    function1.setFunctionName(MetricAggFunction.SUM);

    final MetricFunction function2 = new MetricFunction();
    function2.setMetricName("m2");
    function2.setFunctionName(MetricAggFunction.SUM);

    final MetricFunction function3 = new MetricFunction();
    function3.setMetricName("m3");
    function3.setFunctionName(MetricAggFunction.SUM);

    return new ArrayList<MetricFunction>() {{
      add(function1);
      add(function2);
      add(function3);
    }};
  }

  private static List<String> getDimensionNames() {
    return new ArrayList<String>() {{
      add("d1");
      add("d2");
    }};
  }

  private static List<String> getDimensionValueList1() {
    return new ArrayList<String>() {{
      add("d1v1");
      add("d2v1");
    }};
  }

  private static List<String> getDimensionValueList2() {
    return new ArrayList<String>() {{
      add("d1v2");
      add("d2v2");
    }};
  }

  private static List<String> getDimensionValueList3() {
    return new ArrayList<String>() {{
      add("d1v3");
      add("d2v3");
    }};
  }

  static class MockedThirdEyeResponse implements ThirdEyeResponse {
    List<ThirdEyeResponseRow> responseRows = new ArrayList<>();
    List<MetricFunction> metricFunctions = new ArrayList<>();
    ThirdEyeRequest thirdEyeRequest;
    List<String> groupKeyColumns = new ArrayList<>();

    @Override
    public List<MetricFunction> getMetricFunctions() {
      return metricFunctions;
    }

    @Override
    public int getNumRows() {
      return responseRows.size();
    }

    @Override
    public ThirdEyeResponseRow getRow(int rowId) {
      return responseRows.get(rowId);
    }

    @Override
    public int getNumRowsFor(MetricFunction metricFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getRow(MetricFunction metricFunction, int rowId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ThirdEyeRequest getRequest() {
      return thirdEyeRequest;
    }

    @Override
    public TimeSpec getDataTimeSpec() {
      return null;
    }

    @Override
    public List<String> getGroupKeyColumns() {
      return groupKeyColumns;
    }

    public void addRow(ThirdEyeResponseRow row) {
      responseRows.add(row);
    }

    public void setMetricFunctions(List<MetricFunction> metricFunctions) {
      this.metricFunctions = metricFunctions;
    }

    public void setThirdEyeRequest(ThirdEyeRequest thirdEyeRequest) {
      this.thirdEyeRequest = thirdEyeRequest;
    }

    public void setGroupKeyColumns(List<String> groupKeyColumns) {
      this.groupKeyColumns = groupKeyColumns;
    }
  }
}
