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
package org.apache.pinot.query.timeboundary;

import java.util.List;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.spi.data.LogicalTableConfig;


public interface TimeBoundaryStrategy {

  /**
   * Returns the time boundary strategy name.
   *
   * @return The time boundary strategy name.
   */
  String getName();

  /**
   * Initializes the time boundary strategy with the given logical table configuration and table cache.
   * @param logicalTableConfig The logical table configuration to use for initialization.
   * @param tableCache The table cache to use for initialization.
   */
  void init(LogicalTableConfig logicalTableConfig, TableCache tableCache);

  /**
   * Computes the time boundary for the given physical table names.
   *
   * @param routingManager The routing manager to use for computing the time boundary.
   * @return The computed time boundary information.
   */
  TimeBoundaryInfo computeTimeBoundary(RoutingManager routingManager);


  /**
   * Returns the list of physical table names that are part of the time boundary.
   * @param logicalTableConfig The logical table configuration
   * @return The list of physical table names that are part of the time boundary.
   */
  List<String> getTimeBoundaryTableNames(LogicalTableConfig logicalTableConfig);
}
