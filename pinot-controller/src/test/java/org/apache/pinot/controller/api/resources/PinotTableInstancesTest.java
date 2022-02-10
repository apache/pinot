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
package org.apache.pinot.controller.api.resources;

import com.google.common.collect.ImmutableList;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.ws.rs.WebApplicationException;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class PinotTableInstancesTest {

    @Test
    public void testGetLiveBrokersForTableByType() {
        String tableName = "testTable";
        String offlineTableName = "testTable_OFFLINE";
        String realtimeTableName = "testTable_REALTIME";
        List<String> expectedBrokers = ImmutableList.of("pinot_broker_0");
        // Setup mock helix resource manager.
        PinotHelixResourceManager mockedHelixResourceManager = mock(PinotHelixResourceManager.class);
        doReturn(expectedBrokers)
                .when(mockedHelixResourceManager)
                .getLiveBrokersForTable(anyString());
        doAnswer((invocationOnMock) -> {
            String inputTableName = invocationOnMock.getArgument(0);
            return inputTableName.equals(tableName) || inputTableName.equals(offlineTableName);
        })
                .when(mockedHelixResourceManager)
                .hasOfflineTable(anyString());
        doAnswer((invocationOnMock) -> {
            String inputTableName = invocationOnMock.getArgument(0);
            return inputTableName.equals(tableName) || inputTableName.equals(realtimeTableName);
        })
                .when(mockedHelixResourceManager)
                .hasRealtimeTable(anyString());

        // Create PinotTableInstances and begin testing.
        PinotTableInstances pinotTableInstances = new PinotTableInstances();
        pinotTableInstances.setPinotHelixResourceManager(mockedHelixResourceManager);

        // Test with table name without type suffix.
        Map<String, List<String>> result = pinotTableInstances.getLiveBrokersForTableByType(tableName);
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(expectedBrokers, result.get("OFFLINE"));
        Assert.assertEquals(expectedBrokers, result.get("REALTIME"));

        // Test with table name with a type suffix.
        result = pinotTableInstances.getLiveBrokersForTableByType(offlineTableName);
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(expectedBrokers, result.get("OFFLINE"));

        // Test with non-existent table
        try {
            pinotTableInstances.getLiveBrokersForTableByType("non_existent_table");
            Assert.fail("API call should have failed with 404 for non-existent table");
        } catch (WebApplicationException e) {
            Assert.assertEquals(404, e.getResponse().getStatus());
        }
    }
}
