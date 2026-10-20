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
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.TableShard;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptStatus;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Input;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.OutputPolicy;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.PublicationState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueKey;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.ScheduleState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.transport.ParticipantAccessPolicy.ParticipantIdentity;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Typed task values preserve their fences; worker commands use only authenticated MINION session identities.
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
    Input input = new Input(key, "minion", Map.of("key", "v"), 2, new OutputPolicy("file:/tmp/task-outputs/", 1));
    List<ObjectVersion> dependencies =
        List.of(new ObjectVersion(history, table, 1), new ObjectVersion(history, UUID.randomUUID(), 1));
    Content output = new Content("orders_OFFLINE", table, "segment", UUID.randomUUID(), "a".repeat(64),
        "file:/tmp/task-outputs/segment.tar.gz", new LoaderDescriptor(1, -1, false, null, null, -1, -1, Map.of()));
    List<CatalogRecord> records = List.of(
        record(RecordKind.TASK_QUEUE, "queue", new QueueState(history, key, false, owner)),
        record(RecordKind.TASK, "task",
            new TaskState(history, taskId, input, dependencies, owner, TaskStatus.SUCCEEDED, 1)),
        record(RecordKind.TASK_ATTEMPT, "attempt", new AttemptState(history, taskId, 1, participant,
            new ObjectVersion(history, UUID.randomUUID(), 2), 4, AttemptStatus.SUCCEEDED, new Progress(1, ""))),
        record(RecordKind.TASK_SCHEDULE, "schedule", new ScheduleState(history, UUID.randomUUID(), input, dependencies,
            owner, 0, 1, 2, 1, 1)),
        record(RecordKind.TASK_PUBLICATION, "publication",
            new PublicationState(history, taskId, 1, minion, List.of(output), 4)));
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
  public void testReadTypesDoNotAuthorizeGenericTaskWritesOrQueryCapabilities() {
    assertThrows(IllegalArgumentException.class,
        () -> new ParticipantIdentity("minion", Role.MINION, Set.of(Capability.SERVER_QUERY)));
    assertThrows(IllegalArgumentException.class,
        () -> new ParticipantIdentity("server", Role.SERVER, Set.of(Capability.MINION_TASK)));
    for (MetadataRpc.RecordKind kind : List.of(MetadataRpc.RecordKind.TASK_QUEUE, MetadataRpc.RecordKind.TASK,
        MetadataRpc.RecordKind.TASK_ATTEMPT, MetadataRpc.RecordKind.TASK_SCHEDULE,
        MetadataRpc.RecordKind.TASK_PUBLICATION)) {
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
