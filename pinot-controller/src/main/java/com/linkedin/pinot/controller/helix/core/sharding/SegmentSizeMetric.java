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

import com.linkedin.pinot.common.restlet.resources.ServerSegmentInfo;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.util.ServerPerfMetricsReader;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.helix.model.IdealState;


public class SegmentSizeMetric implements ServerLoadMetric {
  private static final HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
  private static final Executor executor = Executors.newFixedThreadPool(1);

  @Override
  public double computeInstanceMetric(PinotHelixResourceManager helixResourceManager, IdealState idealState,
      String instance, String tableName) {
    ServerPerfMetricsReader serverPerfMetricsReader =
        new ServerPerfMetricsReader(executor, connectionManager, helixResourceManager);
    ServerSegmentInfo serverSegmentInfo = serverPerfMetricsReader.getServerPerfMetrics(instance, true, 300);

    return serverSegmentInfo.getSegmentSizeInBytes();

    //return  serverSegmentInfo.getSegmentCPULoad();

    /*
    double timeInterval = 0;
    List<SegmentMetadata> segmentMetadataList = serverSegmentInfo.getSegmentList();
    for(int i=0; i<segmentMetadataList.size();i++)
    {
      timeInterval += (segmentMetadataList.get(i).getStartTime() - segmentMetadataList.get(i).getEndTime())/(24*3600);
    }

    return  timeInterval;*/

  }

  @Override
  public void updateServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance, Double currentLoadMetric, String tableName, SegmentMetadata segmentMetadata) {
    return;
  }

  @Override
  public void resetServerLoadMetric(PinotHelixResourceManager helixResourceManager, String instance) {
    return;
  }
}
