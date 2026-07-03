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
package org.apache.pinot.server.starter.helix;

import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class SegmentOnlineOfflineStateModelFactoryTest {
  private static final String TABLE_NAME = "testTable_REALTIME";
  private static final String SEGMENT_NAME = "testTable__0__0__20260702T0000Z";

  @Test
  public void testOfflineFromErrorOffloadsSegment()
      throws Exception {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getInstanceId()).thenReturn("server-1");
    SegmentOnlineOfflineStateModelFactory.SegmentOnlineOfflineStateModel stateModel =
        (SegmentOnlineOfflineStateModelFactory.SegmentOnlineOfflineStateModel)
            new SegmentOnlineOfflineStateModelFactory(instanceDataManager, null)
                .createNewStateModel(TABLE_NAME, SEGMENT_NAME);

    stateModel.onBecomeOfflineFromError(createStateTransitionMessage(), mock(NotificationContext.class));

    verify(instanceDataManager).offloadSegment(TABLE_NAME, SEGMENT_NAME);
  }

  @Test
  public void testOfflineFromErrorDoesNotRethrowOffloadFailure()
      throws Exception {
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getInstanceId()).thenReturn("server-1");
    doThrow(new RuntimeException("offload failed")).when(instanceDataManager).offloadSegment(TABLE_NAME, SEGMENT_NAME);
    SegmentOnlineOfflineStateModelFactory.SegmentOnlineOfflineStateModel stateModel =
        (SegmentOnlineOfflineStateModelFactory.SegmentOnlineOfflineStateModel)
            new SegmentOnlineOfflineStateModelFactory(instanceDataManager, null)
                .createNewStateModel(TABLE_NAME, SEGMENT_NAME);

    stateModel.onBecomeOfflineFromError(createStateTransitionMessage(), mock(NotificationContext.class));

    verify(instanceDataManager).offloadSegment(TABLE_NAME, SEGMENT_NAME);
  }

  private static Message createStateTransitionMessage() {
    Message message = new Message(Message.MessageType.STATE_TRANSITION, "message-id");
    message.setResourceName(TABLE_NAME);
    message.setPartitionName(SEGMENT_NAME);
    return message;
  }
}
