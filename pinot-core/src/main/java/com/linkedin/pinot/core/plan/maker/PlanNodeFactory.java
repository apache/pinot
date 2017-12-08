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
package com.linkedin.pinot.core.plan.maker;

import java.util.List;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.plan.AggregationGroupByPlanNode;
import com.linkedin.pinot.core.plan.AggregationPlanNode;
import com.linkedin.pinot.core.plan.DictionaryBasedAggregationPlanNode;
import com.linkedin.pinot.core.plan.MetadataBasedAggregationPlanNode;
import com.linkedin.pinot.core.plan.PlanNode;
import com.linkedin.pinot.core.plan.SelectionPlanNode;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;

public class PlanNodeFactory {

  /**
   * Helper method to select the right plan node based on the broker request and the segment
   * @param brokerRequest
   * @param indexSegment
   * @param maxInitialResultHolderCapacity
   * @param numAggrGroupsLimit
   * @return
   */
  public static PlanNode getPlanNode(BrokerRequest brokerRequest, IndexSegment indexSegment,
      int maxInitialResultHolderCapacity, int numAggrGroupsLimit) {

    if (brokerRequest.isSetAggregationsInfo()) {
      if (brokerRequest.isSetGroupBy()) {
        return new AggregationGroupByPlanNode(indexSegment, brokerRequest,
            maxInitialResultHolderCapacity, numAggrGroupsLimit);
      } else {
        if (isFitForMetadataBasedPlan(brokerRequest, indexSegment)) {
          return new MetadataBasedAggregationPlanNode(indexSegment, brokerRequest.getAggregationsInfo());
        } else if (isFitForDictionaryBasedPlan(brokerRequest, indexSegment)) {
          return new DictionaryBasedAggregationPlanNode(indexSegment, brokerRequest.getAggregationsInfo());
        } else {
          return new AggregationPlanNode(indexSegment, brokerRequest);
        }
      }
    }
    if (brokerRequest.isSetSelections()) {
      return new SelectionPlanNode(indexSegment, brokerRequest);
    }
    throw new UnsupportedOperationException("The query contains no aggregation or selection.");
  }

  /**
   * Helper method to identify if query is fit to be be served purely based on metadata.
   * Currently count queries without any filters, and min max queries if the segment has min max in
   * metadata, are supported.
   * @param brokerRequest Broker request
   * @param indexSegment
   * @return True if query can be served using metadata, false otherwise.
   */
  public static boolean isFitForMetadataBasedPlan(BrokerRequest brokerRequest, IndexSegment indexSegment) {
    if (brokerRequest.getFilterQuery() != null || brokerRequest.isSetGroupBy()) {
      return false;
    }

    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo == null) {
      return false;
    }
    for (AggregationInfo aggInfo : aggregationsInfo) {
      if (!isMetadataBasedAggregationFunction(aggInfo, indexSegment)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isMetadataBasedAggregationFunction(AggregationInfo aggregationInfo,
      IndexSegment indexSegment) {
    String aggregationType = aggregationInfo.getAggregationType();
    if (aggregationType.equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.COUNT.getName())) {
      return true;
    }

    // Use minValue and maxValue from metadata, in non star tree cases
    // minValue and maxValue is generated from dictionary on segment load, so it won't be correct in case of star tree
    if ((aggregationType.equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.MAX.getName())
        || aggregationType.equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.MIN.getName())
        || aggregationType.equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.MINMAXRANGE.getName()))
        && !indexSegment.getSegmentMetadata().hasStarTree()) {
      String column = aggregationInfo.getAggregationParams().get("column");
      SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) indexSegment.getSegmentMetadata();
      if (segmentMetadata.getColumnMetadataMap() == null) {
        return false;
      }
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata.getMaxValue() != null && columnMetadata.getMinValue() != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Helper method to identify if query is fit to be be served purely based on dictionary.
   * It can be served through dictionary only for min, max, minmaxrange queries as of now,
   * and if a dictionary is present for the column
   * @param brokerRequest Broker request
   * @param indexSegment
   * @return True if query can be served using dictionary, false otherwise.
   */
  public static boolean isFitForDictionaryBasedPlan(BrokerRequest brokerRequest,
      IndexSegment indexSegment) {
    // Skipping dictionary in case of star tree. Results from dictionary won't be correct
    // because of aggregated values in metrics, and ALL value in dimension
    if ((brokerRequest.getFilterQuery() != null) || brokerRequest.isSetGroupBy()
        || indexSegment.getSegmentMetadata().hasStarTree()) {
      return false;
    }
    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    if (aggregationsInfo == null) {
      return false;
    }
    for (AggregationInfo aggregationInfo : aggregationsInfo) {
      if (!isDictionaryBasedAggregationFunction(aggregationInfo, indexSegment)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isDictionaryBasedAggregationFunction(AggregationInfo aggregationInfo,
      IndexSegment indexSegment) {
    String aggregationType = aggregationInfo.getAggregationType();
    if (aggregationType.equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.MAX.getName())
        || aggregationType.equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.MIN.getName())
        || aggregationType.equalsIgnoreCase(AggregationFunctionFactory.AggregationFunctionType.MINMAXRANGE.getName())) {
      String column = aggregationInfo.getAggregationParams().get("column");
      SegmentMetadataImpl segmentMetadata = (SegmentMetadataImpl) indexSegment.getSegmentMetadata();
      if (segmentMetadata.getColumnMetadataMap() != null && segmentMetadata.hasDictionary(column)) {
        return true;
      }
    }
    return false;
  }

}
