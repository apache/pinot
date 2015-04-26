/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.query.pruner;

import org.apache.commons.configuration.Configuration;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.SelectionSort;
import com.linkedin.pinot.core.indexsegment.IndexSegment;


/**
 * An implementation of SegmentPruner.
 * Querying columns not appearing in the given segment will be pruned.
 *
 * @author xiafu
 *
 */
public class DataSchemaSegmentPruner implements SegmentPruner {

  private static final String COLUMN_KEY = "column";

  @Override
  public boolean prune(IndexSegment segment, BrokerRequest brokerRequest) {
    Schema schema = segment.getSegmentMetadata().getSchema();
    if (brokerRequest.getSelections() != null) {
      // Check selection columns
      for (String columnName : brokerRequest.getSelections().getSelectionColumns()) {
        if ((!columnName.equalsIgnoreCase("*")) && (!schema.isExisted(columnName))) {
          return true;
        }
      }

      // Check columns to do sorting,
      for (SelectionSort selectionOrder : brokerRequest.getSelections().getSelectionSortSequence()) {
        if (!schema.isExisted(selectionOrder.getColumn())) {
          return true;
        }
      }
    }
    // Check groupBy columns.
    if (brokerRequest.getGroupBy() != null) {
      for (String columnName : brokerRequest.getGroupBy().getColumns()) {
        if (!schema.isExisted(columnName)) {
          return true;
        }
      }
    }
    // Check aggregation function columns.
    if (brokerRequest.getAggregationsInfo() != null) {
      for (AggregationInfo aggregationInfo : brokerRequest.getAggregationsInfo()) {
        if (aggregationInfo.getAggregationParams().containsKey(COLUMN_KEY)) {
          String columnName = aggregationInfo.getAggregationParams().get(COLUMN_KEY);
          if ((!columnName.equalsIgnoreCase("*")) && (!schema.isExisted(columnName))) {
            return true;
          }
        }
      }
    }

    return false;
  }

  @Override
  public void init(Configuration config) {

  }

  @Override
  public String toString() {
    return "DataSchemaSegmentPruner";
  }
}
