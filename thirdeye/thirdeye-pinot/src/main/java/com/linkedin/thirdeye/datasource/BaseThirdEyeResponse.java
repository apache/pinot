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

package com.linkedin.thirdeye.datasource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.api.TimeSpec;

public abstract class BaseThirdEyeResponse implements ThirdEyeResponse {
  protected final List<MetricFunction> metricFunctions;
  protected final ThirdEyeRequest request;
  protected final TimeSpec dataTimeSpec;
  protected final List<String> groupKeyColumns;
  protected final String[] allColumnNames;
  public BaseThirdEyeResponse(ThirdEyeRequest request, TimeSpec dataTimeSpec) {
    this.request = request;
    this.dataTimeSpec = dataTimeSpec;
    this.metricFunctions = request.getMetricFunctions();
    this.groupKeyColumns = new ArrayList<>();
    if (request.getGroupByTimeGranularity() != null) {
      groupKeyColumns.add(dataTimeSpec.getColumnName());
    }
    groupKeyColumns.addAll(request.getGroupBy());
    ArrayList<String> allColumnNameList = new ArrayList<>();
    allColumnNameList.addAll(request.getGroupBy());
    for(MetricFunction function:request.getMetricFunctions()){
      allColumnNameList.add(function.toString());
    }
    allColumnNames = new String[allColumnNameList.size()];
    allColumnNameList.toArray(allColumnNames);
  }

  @Override
  public List<MetricFunction> getMetricFunctions() {
    return metricFunctions;
  }

  @Override
  public abstract int getNumRows();

  @Override
  public abstract ThirdEyeResponseRow getRow(int rowId);

  @Override
  public abstract int getNumRowsFor(MetricFunction metricFunction);

  @Override
  public abstract Map<String, String> getRow(MetricFunction metricFunction, int rowId);

  @Override
  public ThirdEyeRequest getRequest() {
    return request;
  }

  @Override
  public TimeSpec getDataTimeSpec() {
    return dataTimeSpec;
  }

  @Override
  public List<String> getGroupKeyColumns() {
    return groupKeyColumns;
  }

  public String[] getAllColumnNames() {
    return allColumnNames;
  }
  
  @Override
  public String toString() {
    return super.toString();
  }

}
