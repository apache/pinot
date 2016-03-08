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
package com.linkedin.pinot.common.query;

import java.util.Map;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;


/**
 * Interface for merging/reducing responses from a set of servers.
 * @param <T>
 */
public interface ReduceService<T extends BrokerResponse> {
  /**
   * Reduce instanceResponses gathered from server instances to one brokerResponse.
   * ServerInstance would be helpful in debug mode
   * All the implementations should be thread safe.
   *
   *
   * @param brokerRequest
   * @param instanceResponseMap
   * @return T extends BrokerResponse
   */
  public T reduce(BrokerRequest brokerRequest, Map<ServerInstance, InstanceResponse> instanceResponseMap);

  /**
   * Reduce instanceResponses gathered from server instances to one brokerResponse.
   * ServerInstance would be helpful in debug mode
   * All the implementations should be thread safe.
   *
   *
   * @param brokerRequest
   * @param instanceResponseMap
   * @return T extends BrokerResponse
   */
  public T reduceOnDataTable(BrokerRequest brokerRequest,
      Map<ServerInstance, DataTable> instanceResponseMap);

}
