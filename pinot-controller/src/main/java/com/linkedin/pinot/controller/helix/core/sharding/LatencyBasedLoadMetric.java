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

import com.linkedin.pinot.common.restlet.resources.ServerLatencyMetric;
import com.linkedin.pinot.common.restlet.resources.ServerLoadMetrics;
import com.linkedin.pinot.controller.helix.core.PinotHelixResourceManager;
import com.linkedin.pinot.controller.helix.core.sharding.ServerLoadMetric;
import com.linkedin.pinot.controller.util.ServerLatencyMetricReader;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class LatencyBasedLoadMetric implements ServerLoadMetric {
    private static final HttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
    private static final Executor executor = Executors.newFixedThreadPool(1);

   /* public static ServerLoadMetrics  computeInstanceLatencyMetric(HelixAdmin helixAdmin, IdealState idealState, String instance, String tableName) {

    }*/

    @Override
    public long computeInstanceMetric(PinotHelixResourceManager helixResourceManager, IdealState idealState, String instance) {
        ServerLatencyMetricReader serverlatencyMetricsReader =
                new ServerLatencyMetricReader(executor, connectionManager, helixResourceManager);
        //ServerLoadMetrics serverLatencyInfo = serverlatencyMetricsReader.getServerLatencyMetrics(instance, tableName,true, 300);
        // Will Add logic to read from the model file.
        return 0;
    }
}
