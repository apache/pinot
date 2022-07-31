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
package org.apache.pinot.broker.api.resources;

import javax.inject.Inject;
import javax.ws.rs.Path;
import org.apache.pinot.broker.api.services.PinotBrokerRoutingService;
import org.apache.pinot.broker.routing.BrokerRoutingManager;


@Path("/")
public class PinotBrokerRouting implements PinotBrokerRoutingService {

  @Inject
  BrokerRoutingManager _routingManager;

  @Override
  public String buildRouting(String tableNameWithType) {
    _routingManager.buildRouting(tableNameWithType);
    return "Success";
  }

  @Override
  public String refreshRouting(String tableNameWithType, String segmentName) {
    _routingManager.refreshSegment(tableNameWithType, segmentName);
    return "Success";
  }

  @Override
  public String removeRouting(String tableNameWithType) {
    _routingManager.removeRouting(tableNameWithType);
    return "Success";
  }
}
