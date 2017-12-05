package com.linkedin.thirdeye.datasource.pinot;

import com.google.common.base.Preconditions;
import com.linkedin.thirdeye.api.TimeSpec;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.datasource.BaseThirdEyeResponse;
import com.linkedin.thirdeye.datasource.ThirdEyeRequest;
import com.linkedin.thirdeye.datasource.ThirdEyeResponseRow;
import com.linkedin.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;

public class PinotDataFrameThirdEyeResponse extends BaseThirdEyeResponse {
  private ThirdEyeResultSetMetaData metaData;
  private DataFrame dataFrame;

  public PinotDataFrameThirdEyeResponse(ThirdEyeRequest request, TimeSpec dataTimeSpec,
      ThirdEyeResultSetMetaData metaData, DataFrame dataFrame) {
    super(request, dataTimeSpec);

    Preconditions.checkNotNull(metaData);
    Preconditions.checkNotNull(dataFrame);

    this.metaData = metaData;
    this.dataFrame = dataFrame;
  }

  @Override
  public int getNumRows() {
    return dataFrame.size();
  }

  @Override
  public ThirdEyeResponseRow getRow(int rowId) {
    long timestamp = -1;
    List<String> dimensionValues = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(metaData.getGroupKeyColumnNames())) {
      if (request.getGroupByTimeGranularity() != null) { // One of the dimension values is timestamp
        // Set up timestamp
        timestamp = dataFrame.getLong(dataTimeSpec.getColumnName(), rowId);
        // Dimension values such as "US", "Windows10", etc. excluding time column
        for (String dimensionName : metaData.getGroupKeyColumnNames()) {
          if (!Objects.equals(dataTimeSpec.getColumnName(), dimensionName)) {
            dimensionValues.add(dataFrame.getString(dimensionName, rowId));
          }
        }
      } else { // Either no timestamp or timestamp is used as data
        for (String dimensionName : metaData.getGroupKeyColumnNames()) {
          dimensionValues.add(dataFrame.getString(dimensionName, rowId));
        }
      }
    }
    // metric values
    List<Double> metrics = new ArrayList<>();
    for (String metricNames : metaData.getMetricColumnNames()) {
      metrics.add(dataFrame.getDouble(metricNames, rowId));
    }

    return new ThirdEyeResponseRow(timestamp, dimensionValues, metrics);
  }

  @Override
  public List<String> getGroupKeyColumns() {
    return metaData.getGroupKeyColumnNames();
  }

}
