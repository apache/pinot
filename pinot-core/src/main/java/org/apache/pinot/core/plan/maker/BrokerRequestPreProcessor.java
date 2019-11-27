/**
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
package org.apache.pinot.core.plan.maker;

import java.util.List;
import java.util.Objects;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.segment.SegmentMetadata;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


/**
 * The <code>BrokerRequestPreProcessor</code> class provides the utility to pre-process the {@link BrokerRequest}.
 * <p>After the pre-process, the {@link BrokerRequest} should not be further changed.
 */
public class BrokerRequestPreProcessor {
  private BrokerRequestPreProcessor() {
  }

  /**
   * Pre-process the {@link BrokerRequest}.
   * <p>Will apply the changes directly to the passed in object.
   * <p>The following steps are performed:
   * <ul>
   *   <li>Rewrite 'fasthll' column name.</li>
   * </ul>
   *
   * @param indexSegments list of index segments.
   * @param brokerRequest broker request.
   */
  public static void preProcess(List<IndexSegment> indexSegments, BrokerRequest brokerRequest) {
    if (brokerRequest.isSetAggregationsInfo()) {
      List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
      rewriteFastHllColumnName(indexSegments, aggregationsInfo);
    }
  }

  /**
   * Rewrite 'fasthll' column name.
   *
   * @param indexSegments list of index segments.
   * @param aggregationsInfo list of aggregation info.
   */
  private static void rewriteFastHllColumnName(List<IndexSegment> indexSegments,
      List<AggregationInfo> aggregationsInfo) {
    // Consistent check.
    for (AggregationInfo aggregationInfo : aggregationsInfo) {
      if (aggregationInfo.getAggregationType().equalsIgnoreCase("fasthll")) {
        String column = AggregationFunctionUtils.getColumn(aggregationInfo);
        boolean isFirstSegment = true;
        String firstSegmentName = null;
        String hllDerivedColumn = null;
        for (IndexSegment indexSegment : indexSegments) {
          SegmentMetadata segmentMetadata = indexSegment.getSegmentMetadata();
          if (isFirstSegment) {
            // Use metadata from first index segment to perform rewrite.
            isFirstSegment = false;
            firstSegmentName = segmentMetadata.getName();
            hllDerivedColumn = segmentMetadata.getDerivedColumn(column, MetricFieldSpec.DerivedMetricType.HLL);
            if (hllDerivedColumn != null) {
              aggregationInfo.putToAggregationParams(AggregationFunctionUtils.COLUMN_KEY, hllDerivedColumn);
            }
          } else {
            // Perform consistency check on other index segments.
            String hllDerivedColumnToCheck =
                segmentMetadata.getDerivedColumn(column, MetricFieldSpec.DerivedMetricType.HLL);
            if (!Objects.equals(hllDerivedColumn, hllDerivedColumnToCheck)) {
              throw new RuntimeException(
                  "Found inconsistency HLL derived column name. In segment " + firstSegmentName + ": "
                      + hllDerivedColumn + "; In segment " + segmentMetadata.getName() + ": "
                      + hllDerivedColumnToCheck);
            }
          }
        }
      }
    }
  }
}
