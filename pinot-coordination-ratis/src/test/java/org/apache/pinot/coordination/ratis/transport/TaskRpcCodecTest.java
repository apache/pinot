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
package org.apache.pinot.coordination.ratis.transport;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.TableShard;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptStatus;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Input;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueKey;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Task read/feed values preserve their fences without exposing task commands or enabling MINION transport access.
public class TaskRpcCodecTest {
  @Test
  public void testTypedTaskFeedRoundTrip() throws Exception {
    UUID history = UUID.randomUUID();
    UUID table = UUID.randomUUID();
    UUID taskId = UUID.randomUUID();
    ParticipantToken controller = new ParticipantToken(history, "controller", Role.CONTROLLER, UUID.randomUUID(), 1);
    OwnerToken owner = new OwnerToken(history, 1, new TableShard(0), 1, controller);
    QueueKey key = new QueueKey("orders_OFFLINE", table, "TestTask");
    ParticipantToken minion = new ParticipantToken(history, "minion", Role.MINION, UUID.randomUUID(), 1);
    ParticipantState participant = new ParticipantState(minion, Map.of(), Set.of(), 3, true, false, false,
        new Revision(history, 2), 0, 3);
    List<CatalogRecord> records = List.of(
        record(RecordKind.TASK_QUEUE, "queue", new QueueState(history, key, false, owner)),
        record(RecordKind.TASK, "task", new TaskState(history, taskId, new Input(key, "minion", Map.of("key", "v"), 2),
            List.of(new ObjectVersion(history, table, 1), new ObjectVersion(history, UUID.randomUUID(), 1)), owner,
            TaskStatus.RUNNING, 1)),
        record(RecordKind.TASK_ATTEMPT, "attempt", new AttemptState(history, taskId, 1, participant,
            new ObjectVersion(history, UUID.randomUUID(), 2), 4, AttemptStatus.RUNNING, new Progress(0, ""))));
    MetadataRpc.ChangeTransaction.Builder transaction = MetadataRpc.ChangeTransaction.newBuilder()
        .setPreviousIndex(3).setIndex(4);
    for (CatalogRecord record : records) {
      MetadataRpc.CatalogRecord encoded = MetadataRpcCodec.record(record);
      assertEquals(MetadataRpcCodec.record(encoded), record);
      transaction.addRecords(encoded);
    }
    MetadataRpc.Cursor from = MetadataRpc.Cursor.newBuilder().setHistoryId(history.toString()).setIndex(3)
        .setScopeId("a".repeat(64)).build();
    var feed = MetadataRpc.ChangePage.newBuilder().setStatus(MetadataRpc.ResultStatus.OK).setFrom(from)
        .setNext(from.toBuilder().setIndex(4)).setRetentionFloor(0).addTransactions(transaction).build();
    assertEquals(MetadataRpcCodec.changes(feed).transactions().getFirst().records(), records);
    MetadataRpc.CatalogRecord mismatched = MetadataRpcCodec.record(records.getFirst()).toBuilder()
        .setKind(MetadataRpc.RecordKind.TASK).build();
    assertThrows(IllegalArgumentException.class, () -> MetadataRpcCodec.record(mismatched));
  }

  @Test
  public void testReadTypesDoNotAuthorizeTaskWritesOrMinionMembership() {
    assertThrows(IllegalArgumentException.class, () -> new ParticipantIdentity("minion", Role.MINION, Set.of()));
    for (MetadataRpc.RecordKind kind : List.of(MetadataRpc.RecordKind.TASK_QUEUE, MetadataRpc.RecordKind.TASK,
        MetadataRpc.RecordKind.TASK_ATTEMPT)) {
      assertThrows(IllegalArgumentException.class, () -> MetadataRpcCodec.mutation(
          MetadataRpc.Mutation.newBuilder().setKind(kind).setAction(MetadataRpc.Mutation.Action.CREATE).build()));
    }
    assertThrows(IllegalArgumentException.class,
        () -> TaskRpcCodec.task(MetadataRpc.TaskExecutionState.getDefaultInstance()));
    assertThrows(IllegalArgumentException.class,
        () -> TaskRpcCodec.attempt(MetadataRpc.TaskAttemptState.getDefaultInstance()));
  }

  private static CatalogRecord record(RecordKind kind, String name, Object value) throws Exception {
    return new CatalogRecord(kind, name, UUID.randomUUID(), 1, 1, 4, false,
        JsonUtils.stringToJsonNode(JsonUtils.objectToString(value)));
  }
}
