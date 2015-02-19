package com.linkedin.pinot.core.query.aggregation;

import java.util.List;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * @author xiafu
 *
 */
public class AggregationFunctionUtils {
  public static DataSchema getAggregationResultsDataSchema(List<AggregationFunction> aggregationFunctionList)
      throws Exception {
    final String[] columnNames = new String[aggregationFunctionList.size()];
    final DataType[] columnTypes = new DataType[aggregationFunctionList.size()];
    for (int i = 0; i < aggregationFunctionList.size(); ++i) {
      columnNames[i] = aggregationFunctionList.get(i).getFunctionName();
      columnTypes[i] = aggregationFunctionList.get(i).aggregateResultDataType();
    }
    return new DataSchema(columnNames, columnTypes);
  }

  public static boolean isAggregationFunctionWithDictionary(AggregationInfo aggregationInfo, IndexSegment indexSegment) {
    boolean hasDictionary = true;
    if (!aggregationInfo.getAggregationType().equalsIgnoreCase("count")) {
      String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");

      for (String column : columns) {
        if (!indexSegment.getSegmentMetadata().hasDictionary(column)) {
          hasDictionary = false;
        }
      }
    }
    return hasDictionary;
  }
}
