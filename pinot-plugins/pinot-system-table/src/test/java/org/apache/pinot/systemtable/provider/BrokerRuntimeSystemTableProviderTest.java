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
package org.apache.pinot.systemtable.provider;

import java.util.Map;
import org.apache.pinot.common.systemtable.SystemTableProviderContext;
import org.apache.pinot.common.version.PinotVersion;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class BrokerRuntimeSystemTableProviderTest {

  @Test
  public void testBrokerRuntimeTableProducesOneRow() {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CommonConstants.Broker.CONFIG_OF_BROKER_ID, "Broker_testHost_1234",
        CommonConstants.Broker.CONFIG_OF_BROKER_HOSTNAME, "testHost",
        CommonConstants.Helix.KEY_OF_BROKER_QUERY_PORT, "1234"));

    BrokerRuntimeSystemTableProvider provider = new BrokerRuntimeSystemTableProvider();
    provider.init(new SystemTableProviderContext(null, null, null, config));
    assertEquals(provider.getExecutionMode(), BrokerRuntimeSystemTableProvider.ExecutionMode.BROKER_SCATTER_GATHER);

    IndexSegment segment = provider.getDataSource();
    try {
      assertEquals(segment.getSegmentMetadata().getTotalDocs(), 1);
      assertEquals(segment.getValue(0, "brokerId"), "Broker_testHost_1234");
      assertEquals(segment.getValue(0, "host"), "testHost");
      assertEquals(segment.getValue(0, "port"), 1234);
      assertTrue((long) segment.getValue(0, "startTimeMs") > 0);
      assertTrue((long) segment.getValue(0, "uptimeMs") >= 0);
      assertTrue((long) segment.getValue(0, "timestampMs") > 0);
      assertEquals(segment.getValue(0, "pinotVersion"), PinotVersion.VERSION);
    } finally {
      segment.destroy();
    }
  }
}
