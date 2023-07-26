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
package org.apache.pinot.core.query.utils;

import org.apache.pinot.spi.config.table.TableType;


/**
 * Utils to generate and manage the unique query id within a cluster.
 * Request id might not be unique across brokers or for request hitting a hybrid table. To generate a unique query id
 * within a cluster, we want to combine the broker id, request id and table type.
 */
public class QueryIdUtils {
  private QueryIdUtils() {
  }

  public static final String OFFLINE_SUFFIX = "_O";
  public static final String REALTIME_SUFFIX = "_R";

  public static String getQueryId(String brokerId, long requestId, TableType tableType) {
    return brokerId + "_" + requestId + (tableType == TableType.OFFLINE ? OFFLINE_SUFFIX : REALTIME_SUFFIX);
  }

  public static boolean hasTypeSuffix(String queryId) {
    return queryId.endsWith(OFFLINE_SUFFIX) || queryId.endsWith(REALTIME_SUFFIX);
  }

  public static String withOfflineSuffix(String queryId) {
    return queryId + OFFLINE_SUFFIX;
  }

  public static String withRealtimeSuffix(String queryId) {
    return queryId + REALTIME_SUFFIX;
  }
}
