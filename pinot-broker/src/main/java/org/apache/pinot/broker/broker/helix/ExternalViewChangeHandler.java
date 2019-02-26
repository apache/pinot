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
package org.apache.pinot.broker.broker.helix;

import org.apache.pinot.broker.queryquota.TableQueryQuotaManager;
import org.apache.pinot.broker.routing.HelixExternalViewBasedRouting;


/**
 * Cluster change handler for external view changes.
 */
public class ExternalViewChangeHandler implements ClusterChangeHandler {
  private final HelixExternalViewBasedRouting _helixExternalViewBasedRouting;
  private final TableQueryQuotaManager _tableQueryQuotaManager;

  public ExternalViewChangeHandler(HelixExternalViewBasedRouting helixExternalViewBasedRouting,
      TableQueryQuotaManager tableQueryQuotaManager) {
    _helixExternalViewBasedRouting = helixExternalViewBasedRouting;
    _tableQueryQuotaManager = tableQueryQuotaManager;
  }

  @Override
  public void processClusterChange() {
    _helixExternalViewBasedRouting.processExternalViewChange();
    _tableQueryQuotaManager.processQueryQuotaChange();
  }
}
