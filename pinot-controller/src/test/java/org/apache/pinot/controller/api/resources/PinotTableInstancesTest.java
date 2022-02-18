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
import java.util.List;
import javax.ws.rs.WebApplicationException;
import org.apache.pinot.common.exception.TableNotFoundException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;


public class PinotTableInstancesTest {

  @Test
  public void testGetLiveBrokersForTable()
      throws TableNotFoundException {
    String nonExistentTableName = "testTableForNotFoundExceptionTesting";
    String tableName = "testTable";
    String offlineTableName = "testTable_OFFLINE";
    String realtimeTableName = "testTable_REALTIME";
    String realtimeBroker = "pinot_realtime_broker_0";
    String offlineBroker = "pinot_offline_broker_0";
    String commonBroker = "pinot_common_broker";
    // Setup mock helix resource manager.
    PinotHelixResourceManager mockedHelixResourceManager = mock(PinotHelixResourceManager.class);
    doReturn(ImmutableList.of(commonBroker, offlineBroker)).when(mockedHelixResourceManager)
        .getLiveBrokersForTable(offlineTableName);
    doReturn(ImmutableList.of(commonBroker, realtimeBroker)).when(mockedHelixResourceManager)
        .getLiveBrokersForTable(realtimeTableName);
    doReturn(ImmutableList.of(commonBroker)).when(mockedHelixResourceManager)
        .getLiveBrokersForTable(tableName);
    doThrow(TableNotFoundException.class).when(mockedHelixResourceManager)
        .getLiveBrokersForTable(nonExistentTableName);

    // Create PinotTableInstances and begin testing.
    PinotTableInstances pinotTableInstances = new PinotTableInstances();
    pinotTableInstances.setPinotHelixResourceManager(mockedHelixResourceManager);

    // Test with table name without type suffix.
    List<String> result = pinotTableInstances.getLiveBrokersForTable(tableName);
    Assert.assertEquals(result, ImmutableList.of(commonBroker));

    // Test with table name with a type suffix.
    result = pinotTableInstances.getLiveBrokersForTable(offlineTableName);
    Assert.assertEquals(result, ImmutableList.of(commonBroker, offlineBroker));

    result = pinotTableInstances.getLiveBrokersForTable(realtimeTableName);
    Assert.assertEquals(result, ImmutableList.of(commonBroker, realtimeBroker));

    // Test with non-existent table.
    try {
      pinotTableInstances.getLiveBrokersForTable(nonExistentTableName);
      Assert.fail("Method call above should have failed");
    } catch (WebApplicationException webApplicationException) {
      Assert.assertEquals(404, webApplicationException.getResponse().getStatus());
      Assert.assertTrue(webApplicationException.getMessage().contains(nonExistentTableName));
    }
  }
}
