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
package com.linkedin.pinot.server.api.resources;

import com.linkedin.pinot.common.restlet.resources.ServerLatencyMetric;
import com.linkedin.pinot.common.restlet.resources.ServerLoadMetrics;
import com.linkedin.pinot.common.utils.CommonConstants;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;


public class ServerMetricsInfoTest {
    /*
    public static final Logger LOGGER = LoggerFactory.getLogger(ServerMetricsInfoTest.class);
    ResourceTestHelper testHelper = new ResourceTestHelper();
    WebTarget target;

    @BeforeClass
    public void setupTest() throws Exception {
        testHelper.setup();
        target = testHelper.target;
    }

    @AfterTest
    public void tearDownTest() throws Exception {
        testHelper.tearDown();
    }

    @Test
    public void testServerPerfMetricNotFound() {
        //We did mot implement QueryInfo yet, so it should return error
        Response response = target.path("/ServerPerfMetrics/table_0/notDOne").request().get(Response.class);
        Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    }

    @Test
    public void testServerPerfResource() {
        ServerLoadMetrics serverLatencyInfo = target.path(CommonConstants.Helix.ServerMetricUris.SERVER_METRICS_INFO_URI+"table_0/LatencyInfo").request().get(ServerLoadMetrics.class);
        ServerLatencyMetric metric = new ServerLatencyMetric();
        metric.setSegmentSize((long)100);
        metric.setLatency((long)203);
        metric.setTimestamp(100);
        List<ServerLatencyMetric> server1latencies = new ArrayList<>();
        server1latencies.add(metric);
        Assert.assertNotNull(server1latencies);
    }
    */
}
