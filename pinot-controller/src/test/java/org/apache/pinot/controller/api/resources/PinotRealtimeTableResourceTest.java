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

import java.lang.reflect.Field;
import java.util.Collections;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.helix.model.IdealState;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;


public class PinotRealtimeTableResourceTest {

  @Test
  public void testPauseConsumptionWithValidBatchParameters()
      throws Exception {
    PinotRealtimeTableResource resource = new PinotRealtimeTableResource();
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    PinotLLCRealtimeSegmentManager segmentManager = mock(PinotLLCRealtimeSegmentManager.class);

    setField(resource, "_pinotHelixResourceManager", helixResourceManager);
    setField(resource, "_pinotLLCRealtimeSegmentManager", segmentManager);

    String tableName = "testTable";
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    IdealState idealState = new IdealState(tableNameWithType);
    idealState.enable(true);
    when(helixResourceManager.getTableIdealState(tableNameWithType)).thenReturn(idealState);

    PauseStatusDetails expectedDetails =
        new PauseStatusDetails(true, Collections.singleton("segment"), PauseState.ReasonCode.ADMINISTRATIVE, "comment",
            "ts");
    when(segmentManager.pauseConsumption(eq(tableNameWithType), eq(PauseState.ReasonCode.ADMINISTRATIVE),
        eq("comment"), any())).thenReturn(expectedDetails);

    HttpHeaders headers = mock(HttpHeaders.class);
    Response response = resource.pauseConsumption(tableName, "comment", 4, 7, 30, headers);

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    PauseStatusDetails actual = (PauseStatusDetails) response.getEntity();
    assertNotNull(actual);
    assertEquals(actual.getConsumingSegments(), expectedDetails.getConsumingSegments());

    ArgumentCaptor<ForceCommitBatchConfig> configCaptor = ArgumentCaptor.forClass(ForceCommitBatchConfig.class);
    verify(segmentManager).pauseConsumption(eq(tableNameWithType), eq(PauseState.ReasonCode.ADMINISTRATIVE),
        eq("comment"), configCaptor.capture());
    ForceCommitBatchConfig capturedConfig = configCaptor.getValue();
    assertNotNull(capturedConfig);
    assertEquals(capturedConfig.getBatchSize(), 4);
    assertEquals(capturedConfig.getBatchStatusCheckIntervalMs(), 7000);
    assertEquals(capturedConfig.getBatchStatusCheckTimeoutMs(), 30000);
  }

  @Test
  public void testPauseConsumptionWithInvalidBatchParametersThrowsBadRequest()
      throws Exception {
    PinotRealtimeTableResource resource = new PinotRealtimeTableResource();
    PinotHelixResourceManager helixResourceManager = mock(PinotHelixResourceManager.class);
    PinotLLCRealtimeSegmentManager segmentManager = mock(PinotLLCRealtimeSegmentManager.class);

    setField(resource, "_pinotHelixResourceManager", helixResourceManager);
    setField(resource, "_pinotLLCRealtimeSegmentManager", segmentManager);

    String tableName = "badTable";
    String tableNameWithType = TableNameBuilder.REALTIME.tableNameWithType(tableName);
    IdealState idealState = new IdealState(tableNameWithType);
    idealState.enable(true);
    when(helixResourceManager.getTableIdealState(tableNameWithType)).thenReturn(idealState);

    HttpHeaders headers = mock(HttpHeaders.class);
    ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
        () -> resource.pauseConsumption(tableName, "comment", 0, 5, 10, headers));

    assertEquals(exception.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    verify(segmentManager, never()).pauseConsumption(any(), any(), any(), any());
  }

  private static void setField(Object target, String fieldName, Object value)
      throws Exception {
    Field field = target.getClass().getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
