/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.datasource.csv;

import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.datasource.BaseThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.MetricFunction;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponseRow;
import org.apache.pinot.thirdeye.datasource.TimeRangeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.joda.time.DateTime;


/**
 * The response of ThirdEye if the data source is a CSV file.
 * Used by {@link CSVThirdEyeDataSource}
 */
public class CSVThirdEyeResponse extends BaseThirdEyeResponse {
  private static final String COL_TIMESTAMP = CSVThirdEyeDataSource.COL_TIMESTAMP;
  /**
   * The Dataframe.
   */
  DataFrame dataframe;

  /**
   * Instantiates a new Csv third eye response.
   *
   * @param request the ThirdEye request
   * @param dataTimeSpec the data time spec
   * @param df the data frame
   */
  public CSVThirdEyeResponse(ThirdEyeRequest request, TimeSpec dataTimeSpec, DataFrame df) {
    super(request, dataTimeSpec);
    this.dataframe = df;
  }

  /**
   * Get the number of rows in the data frame.
   *
   * @return the number of rows in the data frame
   */
  @Override
  public int getNumRows() {
    return dataframe.size();
  }

  /**
   * Get a row from the data frame.
   *
   * @param rowId row number
   * @return a ThirdEyeResponseRow
   */
  @Override
  public ThirdEyeResponseRow getRow(int rowId)  {
    if(rowId >= dataframe.size()){
      throw new IllegalArgumentException();
    }
    int timeBucketId = -1;

    if (dataframe.contains(COL_TIMESTAMP)) {
      long time = dataframe.getLong(COL_TIMESTAMP, rowId);
      timeBucketId = TimeRangeUtils.computeBucketIndex(
              dataTimeSpec.getDataGranularity(),
              request.getStartTimeInclusive(),
              new DateTime(time));
    }

    List<String> dimensions = new ArrayList<>();
    for (String dimension : request.getGroupBy()){
      dimensions.add(dataframe.getString(dimension, rowId));
    }

    List<Double> metrics = new ArrayList<>();
    for(MetricFunction function : request.getMetricFunctions()){
      metrics.add(dataframe.getDouble(function.toString(), rowId));
    }
    return new ThirdEyeResponseRow(timeBucketId, dimensions, metrics);
  }

  /**
   * Get the number of rows for a metric function.
   *
   * @param metricFunction a MetricFunction
   * @return the number of rows for this metric function
   */
  @Override
  public int getNumRowsFor(MetricFunction metricFunction) {
    return dataframe.size();
  }


  /**
   * Get the row that corresponds to a metric function.
   *
   * @param metricFunction a MetricFunction
   * @return the row that corresponds to a metric function
   */
  @Override
  public Map<String, String> getRow(MetricFunction metricFunction, int rowId) {
    Map<String, String> rowMap = new HashMap<>();
    for (int i = 0; i < groupKeyColumns.size(); i++) {
      String dimension = groupKeyColumns.get(i);
      rowMap.put(dimension, dataframe.getString(dimension, rowId));
    }
    rowMap.put(metricFunction.toString(), dataframe.getString(metricFunction.toString(), rowId));
    return rowMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CSVThirdEyeResponse response = (CSVThirdEyeResponse) o;
    return Objects.equals(dataframe, response.dataframe);
  }

  @Override
  public int hashCode() {

    return Objects.hash(dataframe);
  }
}
