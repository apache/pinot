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
import java.util.Map;
import org.apache.helix.model.IdealState;


public class SegmentCountMetric implements ServerLoadMetric {
  @Override
  public double computeInstanceMetric(PinotHelixResourceManager helixResourceManager, IdealState idealState,
                                    String instance, String tableName) {
    /*
    //We use idealState to count number of segments for instances
    if (idealState != null) {
      long numOfSegments = 0;
      for (String partitionName : idealState.getPartitionSet()) {
        Map<String, String> instanceToStateMap = idealState.getInstanceStateMap(partitionName);
        if (instanceToStateMap != null) {
          for (String instanceName : instanceToStateMap.keySet()) {
            if (instance.equalsIgnoreCase(instanceName)) {
              numOfSegments++;
            }
          }
        }
      }
      return numOfSegments;
    } else {
      return 0;
    }

    Map<String,Double> serverLoadMap = helixResourceManager.getServerLoadMap();
    if(serverLoadMap.containsKey(instance))
    {
      return serverLoadMap.get(instance);
    }
    else
    {
      return 0;
    }
    */
    return  0;

  }

  @Override
  public void updateServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance, Double currentLoadMetric, String tableName, SegmentMetadata segmentMetadata) {
    //helixResourceManager.updateServerLoadMap(instance,currentLoadMetric+1);
  }

   @Override
   public void resetServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance) {
    //helixResourceManager.updateServerLoadMap(instance, 0D);
   }

}
