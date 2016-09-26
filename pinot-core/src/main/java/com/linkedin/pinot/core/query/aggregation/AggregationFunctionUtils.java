/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.query.aggregation;

import java.util.List;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.utils.DataTableBuilder.DataSchema;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.realtime.impl.RealtimeSegmentImpl;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;


/**
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
        if (indexSegment instanceof IndexSegmentImpl) {
          if (!indexSegment.getSegmentMetadata().hasDictionary(column)) {
            hasDictionary = false;
          }
        } else if (indexSegment instanceof RealtimeSegmentImpl) {
          if (!((RealtimeSegmentImpl) indexSegment).hasDictionary(column)) {
            hasDictionary = false;
          }
        }
      }
    }
    return hasDictionary;
  }

  public static void ensureAggregationColumnsAreSingleValued(AggregationInfo aggregationInfo, IndexSegment indexSegment) {
    // Only check fasthll for single valued column.
    if (aggregationInfo.getAggregationType().equalsIgnoreCase("fasthll")) {
      String[] columns = aggregationInfo.getAggregationParams().get("column").trim().split(",");
      for (String column : columns) {
        if (!indexSegment.getDataSource(column).getDataSourceMetadata().isSingleValue()) {
          throw new RuntimeException("Unsupported aggregation on multi value column: " + column);
        }
      }
    }
  }
}
