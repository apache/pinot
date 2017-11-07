package com.linkedin.thirdeye.datasource.pinot.resultset;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.thirdeye.dataframe.DataFrame;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ThirdEyeDataFrameResultSet extends AbstractThirdEyePinotResultSet {
  private ResultSetMetaData resultSetMetaData;
  private DataFrame dataFrame;

  public ThirdEyeDataFrameResultSet(ResultSetMetaData resultSetMetaData, DataFrame dataFrame) {
    Preconditions.checkState(isMetaDataAndDataHaveSameColumns(resultSetMetaData, dataFrame),
        "Meta data and data's columns do not match.");

    this.resultSetMetaData = resultSetMetaData;
    this.dataFrame = dataFrame;
  }

  private boolean isMetaDataAndDataHaveSameColumns(ResultSetMetaData resultSetMetaData, DataFrame dataFrame) {
    Set<String> metaDataAllColumns = new HashSet<>(resultSetMetaData.getAllColumnNames());
    return metaDataAllColumns.equals(dataFrame.getSeries().keySet());
  }

  @Override
  public int getRowCount() {
    return dataFrame.size();
  }

  @Override
  public int getColumnCount() {
    return dataFrame.getSeries().size();
  }

  @Override
  public String getColumnName(int columnIdx) {
    Preconditions.checkPositionIndexes(0, columnIdx, resultSetMetaData.getMetricColumnNames().size() - 1);
    return resultSetMetaData.getMetricColumnNames().get(columnIdx);
  }

  @Override
  public String getString(int rowIdx, int columnIdx) {
    Preconditions.checkPositionIndexes(0, columnIdx, resultSetMetaData.getMetricColumnNames().size() - 1);
    return dataFrame.get(resultSetMetaData.getMetricColumnNames().get(columnIdx)).getString(rowIdx);
  }

  @Override
  public int getGroupKeyLength() {
    return resultSetMetaData.getGroupKeyColumnNames().size();
  }

  @Override
  public String getGroupKeyColumnName(int columnIdx) {
    Preconditions.checkPositionIndexes(0, columnIdx, getGroupKeyLength() - 1);
    return resultSetMetaData.getGroupKeyColumnNames().get(columnIdx);
  }

  @Override
  public String getGroupKeyColumnValue(int rowIdx, int columnIdx) {
    Preconditions.checkPositionIndexes(0, columnIdx, getGroupKeyLength() - 1);
    return dataFrame.get(resultSetMetaData.getGroupKeyColumnNames().get(columnIdx)).getString(rowIdx);
  }

  public static ThirdEyeDataFrameResultSet fromPinotResultSet(ResultSet resultSet) {
    // Build the meta data of this result set
    List<String> groupKeyColumnNames = new ArrayList<>();
    for (int groupKeyColumnIdx = 0; groupKeyColumnIdx < resultSet.getGroupKeyLength(); groupKeyColumnIdx++) {
      groupKeyColumnNames.add(resultSet.getGroupKeyColumnName(groupKeyColumnIdx));
    }
    List<String> metricColumnNames = new ArrayList<>();
    for (int columnIdx = 0; columnIdx < resultSet.getColumnCount(); columnIdx++) {
      metricColumnNames.add(resultSet.getColumnName(columnIdx));
    }
    ResultSetMetaData resultSetMetaData = new ResultSetMetaData(groupKeyColumnNames, metricColumnNames);

    // Build the DataFrame
    DataFrame.Builder dfBuilder = DataFrame.builder(resultSetMetaData.getAllColumnNames());
    int rowCount = resultSet.getRowCount();
    int groupByColumnCount = resultSet.getGroupKeyLength();
    int metricColumnCount = resultSet.getColumnCount();
    int totalColumnCount = groupByColumnCount + metricColumnCount;
    // Dump the values in ResultSet to the DataFrame
    for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
      String[] columnsOfTheRow = new String[totalColumnCount];
      // GroupBy column value(i.e., dimension values)
      for (int groupByColumnIdx = 0; groupByColumnIdx < groupByColumnCount; groupByColumnIdx++) {
        String valueString = null;
        try {
          valueString = resultSet.getGroupKeyString(rowIdx, groupByColumnIdx);
        } catch (Exception e) {
          // Do nothing and subsequently insert a null value to the current series.
        }
        columnsOfTheRow[groupByColumnIdx] = valueString;
      }
      // Metric column's value
      for (int metricColumnIdx = 0; metricColumnIdx < metricColumnCount; metricColumnIdx++) {
        String valueString = null;
        try {
          valueString = resultSet.getString(rowIdx, metricColumnIdx);
        } catch (Exception e) {
          // Do nothing and subsequently insert a null value to the current series.
        }
        columnsOfTheRow[metricColumnIdx + groupByColumnCount] = valueString;
      }
      dfBuilder.append(columnsOfTheRow);
    }
    DataFrame dataFrame = dfBuilder.build();
    // Build ThirdEye's result set
    ThirdEyeDataFrameResultSet thirdEyeDataFrameResultSet =
        new ThirdEyeDataFrameResultSet(resultSetMetaData, dataFrame);

    return thirdEyeDataFrameResultSet;
  }
}
