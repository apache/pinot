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
package com.linkedin.pinot.controller.helix.core.sharding;

import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.helix.model.IdealState;


/*
This interface can be implemented by all approaches that assigns a numeric load metric to a server.
Example load metrics are number of segments, storage size of segments, STeP paper cost model.
STeP paper: http://people.csail.mit.edu/rytaft/step.pdf
*/
public interface ServerLoadMetric {
  double computeInstanceMetric(PinotHelixResourceManager helixResourceManager, IdealState idealState, String instance,String tableName, SegmentMetadata segmentMetadata);
  void updateServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance, Double currentLoadMetric, String tableName, SegmentMetadata segmentMetadata);
  void resetServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance);
}
