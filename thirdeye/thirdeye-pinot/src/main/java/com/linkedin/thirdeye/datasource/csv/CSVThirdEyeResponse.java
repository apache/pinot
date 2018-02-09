package com.linkedin.thirdeye.datasource.csv;

import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.datasource.BaseThirdEyeResponse;
import com.linkedin.thirdeye.datasource.MetricFunction;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponseRow;
import com.linkedin.thirdeye.datasource.TimeRangeUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;


public class CSVThirdEyeResponse extends BaseThirdEyeResponse {
  private static final String COL_TIMESTAMP = CSVThirdEyeDataSource.COL_TIMESTAMP;
  DataFrame dataframe;

  public CSVThirdEyeResponse(ThirdEyeRequest request, TimeSpec dataTimeSpec, DataFrame df) {
    super(request, dataTimeSpec);
    this.dataframe = df;
  }

  @Override
  public int getNumRows() {
    return dataframe.size();
  }

  @Override
  public ThirdEyeResponseRow getRow(int rowId)  {
    if(rowId >= dataframe.size()){
      throw new IllegalArgumentException();
    }
    long time = dataframe.getLong(COL_TIMESTAMP, rowId);
    int timeBucketId = TimeRangeUtils.computeBucketIndex(
        dataTimeSpec.getDataGranularity(),
        request.getStartTimeInclusive(),
        new DateTime(time)
    );
    List<String> dimensions = new ArrayList<>();
    for (String dimension : request.getGroupBy()){
      dimensions.add(dataframe.getString(dimension, rowId));
    }

    List<Double> metrics = new ArrayList<>();
    for (String metric : request.getMetricNames()){
      metrics.add(dataframe.getDouble(metric, rowId));
    }
    return new ThirdEyeResponseRow(timeBucketId, dimensions, metrics);
  }

  @Override
  public int getNumRowsFor(MetricFunction metricFunction) {
    return dataframe.size();
  }

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
}
