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
package org.apache.pinot.core.query.aggregation.utils;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;


/**
 * Util class for statistical aggregation functions
 *
 * e.g. Variance, Covariance, Standard Deviation...
 */
public class StatisticalAggregationFunctionUtils {
  private StatisticalAggregationFunctionUtils() {
  }

  public static double[] getValSet(Map<ExpressionContext, BlockValSet> blockValSetMap, ExpressionContext expression) {
    BlockValSet blockValSet = blockValSetMap.get(expression);
    //TODO: Add MV support for covariance
    Preconditions.checkState(blockValSet.isSingleValue(),
        "Variance, Covariance, Standard Deviation function currently only supports single-valued column");
    switch (blockValSet.getValueType().getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return blockValSet.getDoubleValuesSV();
      default:
        throw new IllegalStateException(
            "Cannot compute variance, covariance, or standard deviation for non-numeric type: "
                + blockValSet.getValueType());
    }
  }
}
