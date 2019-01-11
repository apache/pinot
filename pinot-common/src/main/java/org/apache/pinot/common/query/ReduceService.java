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
package org.apache.pinot.common.query;

import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.ServerInstance;
import org.apache.pinot.common.utils.DataTable;


/**
 * Interface for merging/reducing responses from a set of servers.
 * @param <T> type of broker response.
 */
@ThreadSafe
public interface ReduceService<T extends BrokerResponse> {

  /**
   * Reduce data tables gathered from server instances to one brokerResponse.
   * <p>Server instance information here is useful for debugging purpose.
   *
   * @param brokerRequest broker request.
   * @param instanceResponseMap map from server instance to data table.
   * @param brokerMetrics broker metrics to track execution statistics.
   * @return broker response.
   */
  @Nonnull
  T reduceOnDataTable(@Nonnull BrokerRequest brokerRequest, @Nonnull Map<ServerInstance, DataTable> instanceResponseMap,
      @Nullable BrokerMetrics brokerMetrics);
}
