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
package org.apache.pinot.core.util;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.SelectionSort;

import static org.apache.pinot.common.utils.CommonConstants.Broker.Request.*;


public final class GroupByUtils {

  private static final int NUM_RESULTS_LOWER_LIMIT = 5000;

  private GroupByUtils() {

  }

  /**
   * Returns the higher of topN * 5 or 5k. This is to ensure better precision in results
   */
  public static int getTableCapacity(int topN) {
    return Math.max(topN * 5, NUM_RESULTS_LOWER_LIMIT);
  }

  /**
   * For group by + order by queries: returns the higher of (topN * 5) or (5k), to ensure better precision in results
   * For group by with no order by queries: returns the topN
   */
  public static int getTableCapacity(GroupBy groupBy, List<SelectionSort> orderBy) {
    int topN = (int) groupBy.getTopN();
    if (orderBy != null && !orderBy.isEmpty()) {
      return getTableCapacity(topN);
    } else {
      return topN;
    }
  }
}
