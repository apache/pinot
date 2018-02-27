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
package com.linkedin.pinot.common.query;

import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;


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
