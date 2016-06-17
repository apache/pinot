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

import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.BrokerResponseFactory;
import java.util.HashMap;
import java.util.Map;


/**
 * Registry for ReduceService based on BrokerResponse
 */
public class ReduceServiceRegistry {
  ReduceService<? extends BrokerResponse> _defaultReduceService;
  Map<BrokerResponseFactory.ResponseType, ReduceService<? extends BrokerResponse>> _registry;

  public ReduceServiceRegistry() {
    _defaultReduceService = null;
    _registry = new HashMap<BrokerResponseFactory.ResponseType, ReduceService<? extends BrokerResponse>>();
  }

  /**
   * Register the ReduceService for the given broker response type.
   *
   * @param responseType BrokerResponseType for which to register the reduce service.
   * @param reduceService Reduce service to register
   */
  public synchronized void register(BrokerResponseFactory.ResponseType responseType,
      ReduceService<? extends BrokerResponse> reduceService) {
    _registry.put(responseType, reduceService);
  }

  /**
   * Register a default ReduceService, to be returned when asked for an un-registered service.
   * @param reduceService
   */
  public void registerDefault(ReduceService<? extends BrokerResponse> reduceService) {
    _defaultReduceService = reduceService;
  }

  /**
   * Return the ReduceService registered for the given BrokerResponseType.
   * If specified response type was not registered, the default registry is returned.
   * Returns null if a default was not registered.
   *
   * @param responseType
   * @return
   */
  public ReduceService<? extends BrokerResponse> get(BrokerResponseFactory.ResponseType responseType) {
    ReduceService<? extends BrokerResponse> reduceService = _registry.get(responseType);
    return (reduceService == null) ? _defaultReduceService : reduceService;
  }
}
