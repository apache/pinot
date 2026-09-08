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

import org.apache.helix.model.Message;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.upsert.RetryableMetadataRemovalException;
import org.apache.pinot.server.starter.helix.SegmentOnlineOfflineStateModelFactory.SegmentOnlineOfflineStateModel;
import org.mockito.InOrder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;


/// Verifies that the reset used by realtime validation cannot acknowledge recovery before metadata cleanup succeeds.
public class MetadataRemovalResetTest {
  private static final String TABLE = "testTable_REALTIME";
  private static final String SEGMENT = "testTable__0__1__20260908T0000Z";

  @Test
  public void testErrorResetPropagatesRepairFailureAndRetries()
      throws Exception {
    InstanceDataManager instance = mock(InstanceDataManager.class);
    RealtimeTableDataManager table = mock(RealtimeTableDataManager.class);
    when(instance.getTableDataManager(TABLE)).thenReturn(table);
    SegmentOnlineOfflineStateModel stateModel = (SegmentOnlineOfflineStateModel)
        new SegmentOnlineOfflineStateModelFactory(instance, null).createNewStateModel(TABLE, SEGMENT);
    Message message = message();
    doThrow(new RetryableMetadataRemovalException("storage unavailable", new IllegalStateException()))
        .doNothing().when(table).retryFailedMetadataRemoval(SEGMENT);

    expectThrows(RetryableMetadataRemovalException.class, () -> stateModel.onBecomeOfflineFromError(message, null));
    verify(instance, never()).addConsumingSegment(TABLE, SEGMENT);
    stateModel.onBecomeOfflineFromError(message, null);
    stateModel.onBecomeConsumingFromOffline(message, null);
    InOrder order = inOrder(table, instance);
    order.verify(table).retryFailedMetadataRemoval(SEGMENT);
    order.verify(table).retryFailedMetadataRemoval(SEGMENT);
    order.verify(instance).addConsumingSegment(TABLE, SEGMENT);
  }

  @Test
  public void testErrorDropDoesNotDeleteUnfinishedMetadata()
      throws Exception {
    InstanceDataManager instance = mock(InstanceDataManager.class);
    RealtimeTableDataManager table = mock(RealtimeTableDataManager.class);
    when(instance.getTableDataManager(TABLE)).thenReturn(table);
    SegmentOnlineOfflineStateModel stateModel = (SegmentOnlineOfflineStateModel)
        new SegmentOnlineOfflineStateModelFactory(instance, null).createNewStateModel(TABLE, SEGMENT);
    doThrow(new RetryableMetadataRemovalException("storage unavailable", new IllegalStateException()))
        .doNothing().when(table).retryFailedMetadataRemoval(SEGMENT);
    expectThrows(RetryableMetadataRemovalException.class, () -> stateModel.onBecomeDroppedFromError(message(), null));
    verify(instance, never()).deleteSegment(TABLE, SEGMENT);
    stateModel.onBecomeDroppedFromError(message(), null);
    verify(instance).deleteSegment(TABLE, SEGMENT);
  }

  private static Message message() {
    Message message = new Message(Message.MessageType.STATE_TRANSITION, "metadata-recovery");
    message.setResourceName(TABLE);
    message.setPartitionName(SEGMENT);
    return message;
  }
}
