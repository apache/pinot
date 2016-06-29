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
package com.linkedin.pinot.routing.builder;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;

import com.linkedin.pinot.routing.ServerToSegmentSetMap;


/**
 * Interface for creating a list of ServerToSegmentSetMap based on ExternalView from Helix.
 */
public interface RoutingTableBuilder {

  /**
   * Inits the routing table builder.
   *
   * @param configuration The configuration to use
   */
  void init(Configuration configuration);

  /**
   * Builds one or more routing tables (maps of servers to segment lists) that are used for query routing. The union of
   * the segment lists that are in each routing table should contain all data in Pinot.
   *
   * @param tableName The table name for which to build the routing table
   * @param externalView The external view for the table
   * @param instanceConfigList The instance configurations for the instances serving this particular table (used for
   *                           pruning)
   * @return List of routing tables used to route queries
   */
  List<ServerToSegmentSetMap> computeRoutingTableFromExternalView(String tableName, ExternalView externalView,
      List<InstanceConfig> instanceConfigList);
}
