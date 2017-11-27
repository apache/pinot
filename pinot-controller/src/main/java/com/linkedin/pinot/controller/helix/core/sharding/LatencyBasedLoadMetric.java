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
