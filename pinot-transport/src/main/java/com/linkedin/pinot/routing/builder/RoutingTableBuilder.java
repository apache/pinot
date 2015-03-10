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
package com.linkedin.pinot.routing.builder;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.helix.model.ExternalView;

import com.linkedin.pinot.routing.ServerToSegmentSetMap;


/**
 * Interface for creating a list of ServerToSegmentSetMap based on ExternalView from helix.
 *
 * @author xiafu
 *
 */
public interface RoutingTableBuilder {

  /**
   * @param configuration
   */
  void init(Configuration configuration);

  /**
   * @param resourceName
   * @param externalView
   * @return List of routing table used to
   */
  List<ServerToSegmentSetMap> computeRoutingTableFromExternalView(String resourceName, ExternalView externalView);
}
