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

package org.apache.pinot.thirdeye.datasource.pinot.resultset;

import com.google.common.base.Preconditions;
import org.apache.pinot.client.PinotClientException;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.ObjectSeries;
import org.apache.pinot.thirdeye.dataframe.StringSeries;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ThirdEyeDataFrameResultSetTest {

  @Test
  public void testFromPinotSelectResultSet() throws Exception {
    List<String> columnArray = new ArrayList<>();
    columnArray.add("col1");
    columnArray.add("col2");

    List<String[]> resultArray = new ArrayList<>();
    String[] row1 = new String[columnArray.size()];
    String[] row2 = new String[columnArray.size()];
    String[] row3 = new String[columnArray.size()];
    resultArray.add(row1);
    resultArray.add(row2);
    resultArray.add(row3);

    for (int rowIdx = 0; rowIdx < resultArray.size(); rowIdx++) {
      for (int columnIdx = 0; columnIdx < columnArray.size(); columnIdx++) {
        resultArray.get(rowIdx)[columnIdx] = Integer.toString(rowIdx) + Integer.toString(columnIdx);
      }
    }

    ResultSet selectResultSet = new MockedSelectResultSet(columnArray, resultArray);

    ThirdEyeDataFrameResultSet actualDataFrameResultSet =
        ThirdEyeDataFrameResultSet.fromPinotResultSet(selectResultSet);

    DataFrame dataFrame = new DataFrame();
    dataFrame.addSeries("col1", 0, 10, 20);
    dataFrame.addSeries("col2", 1, 11, 21);
    ThirdEyeResultSetMetaData metaData = new ThirdEyeResultSetMetaData(Collections.<String>emptyList(), columnArray);
    ThirdEyeDataFrameResultSet expectedDataFrameResultSet = new ThirdEyeDataFrameResultSet(metaData, dataFrame);

    Assert.assertEquals(actualDataFrameResultSet, expectedDataFrameResultSet);
    Assert.assertEquals(actualDataFrameResultSet.getGroupKeyLength(), 0);
    Assert.assertEquals(actualDataFrameResultSet.getColumnCount(), 2);
  }

  @Test
  public void testFromPinotSingleAggregationResultSet() {
    final String functionName = "sum_metric_name";

    ResultSet singleAggregationResultSet = new MockedSingleAggregationResultSet(functionName, "150.33576");
    ThirdEyeDataFrameResultSet actualDataFrameResultSet =
        ThirdEyeDataFrameResultSet.fromPinotResultSet(singleAggregationResultSet);

    ThirdEyeResultSetMetaData metaData =
        new ThirdEyeResultSetMetaData(Collections.<String>emptyList(), Collections.singletonList(functionName));
    DataFrame dataFrame = new DataFrame();
    dataFrame.addSeries(functionName, 150.33576);
    ThirdEyeDataFrameResultSet expectedDataFrameResultSet = new ThirdEyeDataFrameResultSet(metaData, dataFrame);

    Assert.assertEquals(actualDataFrameResultSet, expectedDataFrameResultSet);
    Assert.assertEquals(actualDataFrameResultSet.getGroupKeyLength(), 0);
    Assert.assertEquals(actualDataFrameResultSet.getColumnCount(), 1);
  }

  @Test
  public void testFromPinotSingleGroupByResultSet() {
    final String functionName = "sum_metric_name";

    List<String> groupByColumnNames = new ArrayList<>();
    groupByColumnNames.add("country");
    groupByColumnNames.add("pageName");

    List<MockedSingleGroupByResultSet.GroupByRowResult> resultArray = new ArrayList<>();
    resultArray.add(new MockedSingleGroupByResultSet.GroupByRowResult(Arrays.asList("US", "page1"), "1111"));
    resultArray.add(new MockedSingleGroupByResultSet.GroupByRowResult(Arrays.asList("US", "page2"), "2222.2"));
    resultArray.add(new MockedSingleGroupByResultSet.GroupByRowResult(Arrays.asList("IN", "page3"), "333.3"));
    resultArray.add(new MockedSingleGroupByResultSet.GroupByRowResult(Arrays.asList("JP", "page2"), "44444.4"));

    ResultSet singleGroupByResultSet = new MockedSingleGroupByResultSet(groupByColumnNames, functionName, resultArray);
    ThirdEyeDataFrameResultSet actualDataFrameResultSet =
        ThirdEyeDataFrameResultSet.fromPinotResultSet(singleGroupByResultSet);

    ThirdEyeResultSetMetaData metaData =
        new ThirdEyeResultSetMetaData(groupByColumnNames, Collections.singletonList(functionName));
    DataFrame dataFrame = new DataFrame();
    dataFrame.addSeries("country", "US", "US", "IN", "JP");
    dataFrame.addSeries("pageName", "page1", "page2", "page3", "page2");
    dataFrame.addSeries(functionName, 1111, 2222.2, 333.3, 44444.4);
    ThirdEyeDataFrameResultSet expectedDataFrameResultSet = new ThirdEyeDataFrameResultSet(metaData, dataFrame);

    Assert.assertEquals(actualDataFrameResultSet, expectedDataFrameResultSet);
    Assert.assertEquals(actualDataFrameResultSet.getGroupKeyLength(), 2);
    Assert.assertEquals(actualDataFrameResultSet.getColumnCount(), 1);
  }

  @Test
  public void testFromEmptyPinotSingleGroupByResultSet() {
    final String functionName = "sum_metric_name";

    List<String> groupByColumnNames = new ArrayList<>();
    groupByColumnNames.add("country");
    groupByColumnNames.add("pageName");

    List<MockedSingleGroupByResultSet.GroupByRowResult> resultArray = new ArrayList<>();

    ResultSet singleGroupByResultSet = new MockedSingleGroupByResultSet(groupByColumnNames, functionName, resultArray);
    ThirdEyeDataFrameResultSet actualDataFrameResultSet =
        ThirdEyeDataFrameResultSet.fromPinotResultSet(singleGroupByResultSet);

    ThirdEyeResultSetMetaData metaData =
        new ThirdEyeResultSetMetaData(groupByColumnNames, Collections.singletonList(functionName));
    DataFrame dataFrame = new DataFrame();
    dataFrame.addSeries("country", StringSeries.builder().build());
    dataFrame.addSeries("pageName", StringSeries.builder().build());
    dataFrame.addSeries(functionName, ObjectSeries.builder().build());
    ThirdEyeDataFrameResultSet expectedDataFrameResultSet = new ThirdEyeDataFrameResultSet(metaData, dataFrame);

    Assert.assertEquals(actualDataFrameResultSet, expectedDataFrameResultSet);
  }

  private static class MockedSingleGroupByResultSet extends MockedAbstractResultSet {
    List<String> groupByColumnNames = Collections.emptyList();
    String functionName;
    List<GroupByRowResult> resultArray = Collections.emptyList();

    MockedSingleGroupByResultSet(List<String> groupByColumnNames, String functionName,
        List<GroupByRowResult> resultArray) {
      Preconditions.checkNotNull(groupByColumnNames);
      Preconditions.checkNotNull(functionName);
      Preconditions.checkNotNull(resultArray);

      this.groupByColumnNames = groupByColumnNames;
      this.functionName = functionName;
      this.resultArray = resultArray;
    }

    @Override
    public int getRowCount() {
      return resultArray.size();
    }

    @Override
    public int getColumnCount() {
      return 1;
    }

    @Override
    public String getColumnName(int i) {
      return functionName;
    }

    @Override
    public String getString(int rowIdx, int columnIdx) {
      if (columnIdx != 0) {
        throw new IllegalArgumentException("Column index has to be 0 for single group by result.");
      }
      return resultArray.get(rowIdx).value;
    }

    @Override
    public int getGroupKeyLength() {
      // This method mimics the behavior of GroupByResultSet in Pinot, which throw exceptions when the result array
      // is empty.
      try {
        return resultArray.get(0).groupByColumnValues.size();
      } catch (Exception e) {
        throw new PinotClientException(
            "For some reason, Pinot decides to throw exception when the result is empty.");
      }
    }

    @Override
    public String getGroupKeyColumnName(int i) {
      return groupByColumnNames.get(i);
    }

    @Override
    public String getGroupKeyString(int rowIdx, int columnIdx) {
      return resultArray.get(rowIdx).groupByColumnValues.get(columnIdx);
    }

    static class GroupByRowResult {
      List<String> groupByColumnValues;
      String value;

      GroupByRowResult(List<String> groupByColumnValues, String value) {
        Preconditions.checkNotNull(groupByColumnValues);
        Preconditions.checkNotNull(value);

        this.groupByColumnValues = groupByColumnValues;
        this.value = value;
      }
    }
  }


  private static class MockedSingleAggregationResultSet extends MockedAbstractResultSet {
    String functionName;
    String result;

    MockedSingleAggregationResultSet(String functionName, String result) {
      Preconditions.checkNotNull(functionName);
      Preconditions.checkNotNull(result);

      this.functionName = functionName;
      this.result = result;
    }

    @Override
    public int getRowCount() {
      return 1;
    }

    @Override
    public int getColumnCount() {
      return 1;
    }

    @Override
    public String getColumnName(int i) {
      return functionName;
    }

    @Override
    public String getString(int rowIdx, int columnIdx) {
      if (rowIdx != 0 || columnIdx != 0) {
        throw new IllegalArgumentException("Row or column index has to be 0 for single aggregation result.");
      }
      return result;
    }

    @Override
    public int getGroupKeyLength() {
      return 0;
    }

    @Override
    public String getGroupKeyColumnName(int i) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getGroupKeyString(int rowIdx, int columnIdx) {
      throw new UnsupportedOperationException();
    }
  }

  private static class MockedSelectResultSet extends MockedAbstractResultSet {
    List<String> columnArray = Collections.emptyList();
    List<String[]> resultArray = Collections.emptyList();

    MockedSelectResultSet(List<String> columnArray, List<String[]> resultArray) {
      if (columnArray != null) {
        this.columnArray = columnArray;
      }
      if (resultArray != null) {
        if (resultArray.size() != 0) {
          int columnCount = resultArray.get(0).length;
          for (String[] aResultArray : resultArray) {
            int rowColumnCount = aResultArray.length;
            if (rowColumnCount != columnCount) {
              throw new IllegalArgumentException("Result array needs to have rows in the same length.");
            }
          }
        }
        this.resultArray = resultArray;
      }
    }

    @Override
    public int getRowCount() {
      return resultArray.size();
    }

    @Override
    public int getColumnCount() {
      return columnArray.size();
    }

    @Override
    public String getColumnName(int columnIdx) {
      return columnArray.get(columnIdx);
    }

    @Override
    public String getString(int rowIdx, int columnIdx) {
      return resultArray.get(rowIdx)[columnIdx];
    }

    @Override
    public int getGroupKeyLength() {
      return 0;
    }

    @Override
    public String getGroupKeyColumnName(int i) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getGroupKeyString(int rowIdx, int columnIdx) {
      throw new UnsupportedOperationException();
    }
  }

  private static abstract class MockedAbstractResultSet implements ResultSet {
    @Override
    public int getInt(int rowIdx) {
      return getInt(rowIdx, 0);
    }

    @Override
    public long getLong(int rowIdx) {
      return getLong(rowIdx, 0);
    }

    @Override
    public float getFloat(int rowIdx) {
      return getFloat(rowIdx, 0);
    }

    @Override
    public double getDouble(int rowIdx) {
      return getDouble(rowIdx, 0);
    }

    @Override
    public String getString(int rowIdx) {
      return getString(rowIdx, 0);
    }

    @Override
    public int getInt(int rowIndex, int columnIndex) {
      return Integer.parseInt(this.getString(rowIndex, columnIndex));
    }

    @Override
    public long getLong(int rowIndex, int columnIndex) {
      return Long.parseLong(this.getString(rowIndex, columnIndex));
    }

    @Override
    public float getFloat(int rowIndex, int columnIndex) {
      return Float.parseFloat(this.getString(rowIndex, columnIndex));
    }

    @Override
    public double getDouble(int rowIndex, int columnIndex) {
      return Double.parseDouble(this.getString(rowIndex, columnIndex));
    }

    @Override
    public int getGroupKeyInt(int rowIndex, int groupKeyColumnIndex) {
      return Integer.parseInt(this.getGroupKeyString(rowIndex, groupKeyColumnIndex));
    }

    @Override
    public long getGroupKeyLong(int rowIndex, int groupKeyColumnIndex) {
      return Long.parseLong(this.getGroupKeyString(rowIndex, groupKeyColumnIndex));
    }

    @Override
    public float getGroupKeyFloat(int rowIndex, int groupKeyColumnIndex) {
      return Float.parseFloat(this.getGroupKeyString(rowIndex, groupKeyColumnIndex));
    }

    @Override
    public double getGroupKeyDouble(int rowIndex, int groupKeyColumnIndex) {
      return Double.parseDouble(this.getGroupKeyString(rowIndex, groupKeyColumnIndex));
    }
  }
}
