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
package org.apache.pinot.coordination.ratis.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ActivateControlEpoch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.BeginDrain;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ConfigurePolicy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Register;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.SetReady;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Tick;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Unregister;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Acknowledge;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Action;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.NativeTaskCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OfflineSegmentCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.OwnershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.TaskCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.TaskSessionCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.PublishContent;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.AssignOwner;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.ConfigureLayout;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.AttemptStatus;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Cancel;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Checkpoint;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Claim;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Complete;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.CreateSchedule;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.FireSchedule;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Input;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.OutputPolicy;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Progress;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.PublishOfflineOutputs;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.QueueKey;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Retry;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.SetQueuePaused;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Submit;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskStatus;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies durable task execution and recovery fences without a scheduler, native transport or plugin side effects.
public class TaskCatalogTest {
  private static final String TABLE = "orders_OFFLINE";
  private static final FeedScope TASKS =
      new FeedScope(Set.of(RecordKind.TASK, RecordKind.TASK_QUEUE, RecordKind.TASK_ATTEMPT), "task-test");

  @Test
  public void testAtomicClaimAndExecutionOutcomeSurviveRestoreAndLostReply() throws Exception {
    Fixture f = new Fixture();
    assertEquals(f.submit().status(), Status.OK);
    assertEquals(f.state().dependencies().size(), 2);
    assertTrue(f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TABLE_CONFIG, RecordKind.SCHEMA), "config"))
        .records().stream().noneMatch(record -> TaskCatalog.taskKind(record.kind())));
    var before = f._catalog.captureImage(TASKS);
    TaskSessionCommand claim = f.workerCommand(f._minion, new Claim(f._task, f.version()));
    Result accepted = f.apply(claim);
    assertEquals(accepted.status(), Status.OK);
    assertEquals(f.apply(claim), accepted);
    var changes = f._catalog.changes(TASKS, before.cursor(), 128, 128, MetadataProtocol.MAX_CHANGE_BYTES);
    assertEquals(changes.transactions().stream().filter(t -> !t.records().isEmpty()).count(), 1L);
    assertEquals(changes.transactions().getFirst().records().stream().map(CatalogRecord::kind).collect(
        Collectors.toSet()), Set.of(RecordKind.TASK, RecordKind.TASK_ATTEMPT));
    assertEquals(f.worker(new Checkpoint(f._task, f.version(), 1, new Progress(3, "processed"))).status(), Status.OK);
    assertEquals(f.worker(new Checkpoint(f._task, f.version(), 1, new Progress(2, "backwards"))).status(),
        Status.CONFLICT);
    assertEquals(f.worker(new Complete(f._task, f.version(), 1, AttemptStatus.EXECUTION_FINISHED,
        new Progress(4, "execution only"))).status(), Status.OK);
    assertEquals(f.state().status(), TaskStatus.EXECUTION_FINISHED);
    TaskSessionCommand last = f._lastWorker;
    Result completed = f._lastWorkerResult;
    f.restore();
    assertEquals(f.apply(last), completed);
    assertEquals(f.state().status(), TaskStatus.EXECUTION_FINISHED);
    assertEquals(f.task(new Retry(f._owner, f._task, f.version())).status(), Status.CONFLICT);
    assertEquals(f.submit().status(), Status.CONFLICT, "A fresh request cannot reuse a durable task identity");
    assertEquals(f.record(RecordKind.TABLE_CONFIG, TABLE).version(), 1L, "Execution must not publish table effects");
  }

  @Test
  public void testPauseResumeCancellationAndLateReports() throws Exception {
    Fixture f = new Fixture();
    assertEquals(f.submit().status(), Status.OK);
    String queueName = TaskProtocol.queueName(f.input().queue());
    assertEquals(f.task(new SetQueuePaused(f._owner, f.input().queue(), 1, true)).status(), Status.OK);
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.NOT_READY);
    assertEquals(f.task(new SetQueuePaused(f._owner, f.input().queue(), 1, false)).status(), Status.CONFLICT);
    assertTrue(TaskProtocol.decodeQueue(f.record(RecordKind.TASK_QUEUE, queueName).value()).paused());
    assertEquals(f.task(new SetQueuePaused(f._owner, f.input().queue(), 2, false)).status(), Status.OK);
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
    assertEquals(f.task(new Cancel(f._owner, f._task, f.version(), "cancel requested")).status(), Status.OK);
    assertEquals(f.state().status(), TaskStatus.CANCELLED);
    assertEquals(TaskProtocol.decodeAttempt(f.record(RecordKind.TASK_ATTEMPT,
        TaskProtocol.attemptName(f._task, 1)).value()).status(), AttemptStatus.FENCED);
    assertEquals(f.worker(new Complete(f._task, f.version(), 1, AttemptStatus.EXECUTION_FINISHED,
        new Progress(1, "late"))).status(), Status.FENCED);
    assertEquals(f.task(new Retry(f._owner, f._task, f.version())).status(), Status.CONFLICT);
    f.restore();
    assertEquals(f.state().status(), TaskStatus.CANCELLED);
  }

  @Test
  public void testReadinessAndDrainCannotStealAnAttemptButReplacementFencesIt() throws Exception {
    Fixture f = new Fixture();
    assertEquals(f.submit().status(), Status.OK);
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
    assertEquals(f.member(new SetReady(f._minion, new Revision(f._history, f._index), false)).status(), Status.OK);
    assertEquals(f.task(new Retry(f._owner, f._task, f.version())).status(), Status.CONFLICT);
    assertEquals(f.member(new BeginDrain(f._minion)).status(), Status.OK);
    assertEquals(f.task(new Retry(f._owner, f._task, f.version())).status(), Status.CONFLICT);
    assertEquals(f.worker(new Checkpoint(f._task, f.version(), 1, new Progress(1, "draining"))).status(), Status.OK);
    ParticipantToken old = f._minion;
    assertEquals(f.member(new Unregister(old)).status(), Status.OK);
    assertEquals(f.task(new Retry(f._owner, f._task, f.version())).status(), Status.OK);
    assertEquals(f.apply(new TaskSessionCommand(old, new NativeTaskCommand(f._history, TaskProtocol.clientId(old),
        1, "task", 99, old, new Checkpoint(f._task, f.version(), 1, new Progress(2, "stale"))))).status(),
        Status.FENCED);
    f._minion = f.registerMinion("minion", 1);
    assertNotEquals(TaskProtocol.clientId(f._minion), TaskProtocol.clientId(old));
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
    assertEquals(f.state().attempt(), 2L);
    assertEquals(f.worker(new Checkpoint(f._task, f.version(), 1, new Progress(2, "old attempt"))).status(),
        Status.FENCED);
    assertEquals(f.worker(new Complete(f._task, f.version(), 2, AttemptStatus.FATAL_FAILED,
        new Progress(0, "fatal"))).status(), Status.OK);
    assertEquals(f.task(new Retry(f._owner, f._task, f.version())).status(), Status.CONFLICT);
    f.restore();
    assertEquals(f.state().status(), TaskStatus.FATAL_FAILED);
  }

  @Test
  public void testRetryBudgetAndOldResultsAreRetained() throws Exception {
    Fixture f = new Fixture();
    assertEquals(f.submit().status(), Status.OK);
    for (int attempt = 1; attempt <= 2; attempt++) {
      assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
      assertEquals(f.worker(new Complete(f._task, f.version(), attempt, AttemptStatus.RETRYABLE_FAILED,
          new Progress(attempt, "transient"))).status(), Status.OK);
      f.restore();
      assertEquals(f.task(new Retry(f._owner, f._task, f.version())).status(),
          attempt == 1 ? Status.OK : Status.CONFLICT);
    }
    assertEquals(f.state().attempt(), 2L);
    assertEquals(TaskProtocol.decodeAttempt(f.record(RecordKind.TASK_ATTEMPT,
        TaskProtocol.attemptName(f._task, 1)).value()).status(), AttemptStatus.RETRYABLE_FAILED);
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.CONFLICT);
  }

  @Test
  public void testOwnerFailoverAndInputMutationFenceWorkWithoutPreventingCancellation() throws Exception {
    Fixture f = new Fixture();
    assertEquals(f.submit().status(), Status.OK);
    OwnerToken previous = f._owner;
    ParticipantToken successor = f.register("controller-b", Role.CONTROLLER, 0);
    assertEquals(f.owner(new AssignOwner(f._catalog.membershipControl().controlEpoch(), 1, previous.scope(), 1,
        successor)).status(), Status.OK);
    f._owner = new OwnerToken(f._history, 1, previous.scope(), 2, successor);
    assertEquals(f.task(new Cancel(previous, f._task, f.version(), "stale owner")).status(), Status.FENCED);
    Schema changed = new Schema.SchemaBuilder().setSchemaName("orders")
        .addSingleValueDimension("id", DataType.STRING).build();
    CatalogRecord schema = f.record(RecordKind.SCHEMA, "orders");
    assertEquals(f.mutate(Mutation.updateSchema(f.objectVersion(schema), changed)).status(), Status.OK);
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.CONFLICT);
    assertEquals(f.task(new Cancel(f._owner, f._task, f.version(), "input changed")).status(), Status.OK);
    f.restore();
    assertEquals(f.state().status(), TaskStatus.CANCELLED);
    CatalogRecord table = f.record(RecordKind.TABLE_CONFIG, TABLE);
    assertEquals(f.mutate(Mutation.delete(RecordKind.TABLE_CONFIG, TABLE, f.objectVersion(table))).status(), Status.OK);
    assertEquals(f.mutate(Mutation.createTableConfig(f._history, UUID.randomUUID(), f.table())).status(), Status.OK);
    assertEquals(f.task(new Submit(f._owner, UUID.randomUUID(), f.input(), List.of())).status(), Status.CONFLICT);
    f.restore();
  }

  @Test
  public void testWorkerLaneAndProtectedRecordFamilies() throws Exception {
    Fixture f = new Fixture();
    assertEquals(f.submit().status(), Status.OK);
    NativeTaskCommand unwrapped = new NativeTaskCommand(f._history, TaskProtocol.clientId(f._minion), 1, "task", 1,
        f._minion, new Claim(f._task, f.version()));
    assertEquals(f.apply(unwrapped).status(), Status.INVALID_REQUEST);
    ParticipantToken other = f.registerMinion("other", 0);
    assertEquals(f.apply(new TaskSessionCommand(other, unwrapped)).status(), Status.FENCED);
    assertEquals(f.worker(other, new Claim(f._task, f.version())).status(), Status.FENCED);
    assertEquals(f.apply(new TaskSessionCommand(f._minion,
        new RegisterClient(f._history, "another-client", f._minion.incarnation(), 0))).status(), Status.FENCED);
    assertEquals(f.apply(new TaskSessionCommand(f._minion, new Transaction(f._history,
        TaskProtocol.clientId(f._minion), 1, "config", 1, List.of()))).status(), Status.INVALID_REQUEST);
    for (RecordKind kind : List.of(RecordKind.TASK_QUEUE, RecordKind.TASK, RecordKind.TASK_ATTEMPT)) {
      assertEquals(f.mutate(new Mutation(Action.CREATE, kind, "forged",
          new ObjectVersion(f._history, UUID.randomUUID(), 0), MetadataProtocol.toTree(Map.of()))).status(),
          Status.INVALID_REQUEST);
    }
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
    assertEquals(f.worker(new Complete(f._task, f.version(), 1, AttemptStatus.FENCED, new Progress(0, "fake")))
        .status(), Status.INVALID_REQUEST);
  }

  @Test
  public void testSnapshotCompatibilityAndCorruptAttemptReferencesFailClosed() throws Exception {
    Fixture f = new Fixture();
    ObjectNode old = f._catalog.captureSnapshot().deepCopy();
    old.put("formatVersion", 8);
    MetadataCatalog upgraded = new MetadataCatalog(f._cluster, f._history);
    upgraded.restoreSnapshot(old);
    assertEquals(upgraded.captureSnapshot().path("formatVersion").asInt(), 10);
    assertEquals(f.submit().status(), Status.OK);
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
    long claimIndex = f._index;
    assertEquals(f.member(new Unregister(f._minion)).status(), Status.OK);
    f._minion = f.registerMinion("minion", 1);
    ObjectNode current = f._catalog.captureSnapshot().deepCopy();
    ObjectNode incorrectlyOld = current.deepCopy();
    incorrectlyOld.put("formatVersion", 8);
    assertThrows(IOException.class, () -> upgraded.restoreSnapshot(incorrectlyOld));
    for (String corruptField : List.of("registrationEpoch", "attempt", "incarnation", "claimedIndex")) {
      ObjectNode corrupt = current.deepCopy();
      String corruptIncarnation = UUID.randomUUID().toString();
      for (JsonNode records : corrupt.findValues("records")) {
        for (JsonNode record : records) {
          if (record.path("kind").asText().equals(RecordKind.TASK_ATTEMPT.name())) {
            if (corruptField.equals("registrationEpoch")) {
              ((ObjectNode) record.at("/value/participantAtClaim/participant")).put("registrationEpoch", 99);
            } else if (corruptField.equals("incarnation")) {
              ((ObjectNode) record.at("/value/participantAtClaim/participant")).put("incarnation",
                  corruptIncarnation);
            } else if (corruptField.equals("claimedIndex")) {
              ((ObjectNode) record.path("value")).put("claimedIndex", claimIndex - 1);
            } else {
              ((ObjectNode) record.path("value")).put("attempt", 2);
            }
          }
        }
      }
      assertThrows(IOException.class, () -> upgraded.restoreSnapshot(corrupt));
    }
    upgraded.restoreSnapshot(current);
    assertEquals(upgraded.captureSnapshot(), current);
  }

  @Test
  public void testOccurrenceWatermarkAndPublicationCommitAtomicallyAcrossRecovery() throws Exception {
    Fixture f = new Fixture(true);
    UUID scheduleId = UUID.randomUUID();
    CreateSchedule create = new CreateSchedule(f._owner, scheduleId, f.input(), List.of(), 1, 1, 2);
    assertEquals(f.task(create).status(), Status.OK);
    assertEquals(f.task(create).status(), Status.OK);
    assertEquals(f.task(new CreateSchedule(f._owner, scheduleId, f.input(), List.of(), 2, 1, 2)).status(),
        Status.CONFLICT);
    assertEquals(f.task(new FireSchedule(f._owner, scheduleId, 1)).status(), Status.NOT_READY);
    var control = f._catalog.membershipControl();
    assertEquals(f.member(new Tick(control.controlEpoch(), 0, control.policy().version())).status(), Status.OK);
    assertEquals(f.task(new FireSchedule(f._owner, scheduleId, 1)).status(), Status.OK);
    UUID taskId = TaskProtocol.occurrenceTaskId(f._history, scheduleId, 1);
    assertEquals(f.worker(new Claim(taskId, 1)).status(), Status.OK);
    Content output = f.output(taskId, 1);
    long before = f._index;
    assertEquals(f.worker(new PublishOfflineOutputs(taskId, 2, 1, List.of(output), new Progress(1, "published")))
        .status(), Status.OK);
    long published = f._lastWorkerResult.revision().index();
    assertTrue(published > before);
    for (CatalogRecord record : List.of(f.record(RecordKind.TASK, taskId.toString()),
        f.record(RecordKind.TASK_ATTEMPT, TaskProtocol.attemptName(taskId, 1)),
        f.record(RecordKind.TASK_PUBLICATION, taskId.toString()),
        f.record(RecordKind.OFFLINE_SEGMENT_CONTENT, OfflineSegmentProtocol.contentName(f._table,
            output.segmentName())))) {
      assertEquals(record.lastModifiedIndex(), published);
    }
    assertEquals(TaskProtocol.decodeTask(f.record(RecordKind.TASK, taskId.toString()).value()).status(),
        TaskStatus.SUCCEEDED);
    f.restore();
    assertEquals(f.apply(f._lastWorker), f._lastWorkerResult);
    assertTrue(f._catalog.taskSnapshot(f._minion, 128, 100_000).records().isEmpty());
    assertEquals(f.task(new Cancel(f._owner, taskId, 3, "too late")).status(), Status.CONFLICT);
    assertEquals(f.task(new FireSchedule(f._owner, scheduleId, 1)).status(), Status.CONFLICT);
    assertEquals(f.task(new FireSchedule(f._owner, scheduleId, 2)).status(), Status.NOT_READY);
    assertEquals(f.member(new Tick(control.controlEpoch(), 1, control.policy().version())).status(), Status.OK);
    assertEquals(f.task(new FireSchedule(f._owner, scheduleId, 2)).status(), Status.OK);
    assertEquals(f.task(new FireSchedule(f._owner, scheduleId, 3)).status(), Status.CONFLICT);
    f.restore();
    ObjectNode corrupt = f._catalog.captureSnapshot().deepCopy();
    for (JsonNode records : corrupt.findValues("records")) {
      for (JsonNode record : records) {
        if (record.path("kind").asText().equals(RecordKind.TASK_PUBLICATION.name())) {
          ((ObjectNode) record.at("/value/outputs/0")).put("contentSha256", "b".repeat(64));
        }
      }
    }
    assertThrows(IOException.class, () -> new MetadataCatalog(f._cluster, f._history).restoreSnapshot(corrupt));
  }

  @Test
  public void testPublicationRejectsUnownedArtifactsAndFencedAttemptsWithoutPartialEffects() throws Exception {
    Fixture f = new Fixture(true);
    assertEquals(f.submit().status(), Status.OK);
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
    Content output = f.output(f._task, 1);
    Content wrongPath = new Content(output.tableName(), output.tableObjectId(), output.segmentName(),
        output.segmentObjectId(), output.contentSha256(), "file:///outside/result.tar.gz", output.loader());
    assertEquals(f.worker(new PublishOfflineOutputs(f._task, f.version(), 1, List.of(wrongPath),
        new Progress(1, "bad path"))).status(), Status.INVALID_REQUEST);
    assertEquals(f.state().status(), TaskStatus.RUNNING);
    assertEquals(f.worker(new Complete(f._task, f.version(), 1, AttemptStatus.EXECUTION_FINISHED,
        new Progress(1, "no publication"))).status(), Status.INVALID_REQUEST);
    UUID second = UUID.randomUUID();
    assertEquals(f.task(new Submit(f._owner, second, f.input(), List.of())).status(), Status.OK);
    assertEquals(f.worker(new Claim(second, 1)).status(), Status.NOT_READY);
    ParticipantToken old = f._minion;
    assertEquals(f.member(new Unregister(old)).status(), Status.OK);
    f._minion = f.registerMinion("minion", 1);
    assertEquals(f.worker(new Claim(second, 1)).status(), Status.NOT_READY);
    assertEquals(f.task(new Retry(f._owner, f._task, f.version())).status(), Status.OK);
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
    assertEquals(f.worker(new PublishOfflineOutputs(f._task, f.version(), 1, List.of(output),
        new Progress(1, "stale attempt"))).status(), Status.FENCED);
    assertEquals(f.task(new Cancel(f._owner, f._task, f.version(), "cancel")).status(), Status.OK);
    assertEquals(f.worker(new PublishOfflineOutputs(f._task, f.version(), 2, List.of(f.output(f._task, 2)),
        new Progress(1, "late"))).status(), Status.FENCED);
    assertFalse(f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK_PUBLICATION,
        RecordKind.OFFLINE_SEGMENT_CONTENT), "outputs"), 10, 100_000).records().iterator().hasNext());
    f.restore();
  }

  @Test
  public void testPublicationCollisionAndInputChangeCannotCertifySuccess() throws Exception {
    Fixture f = new Fixture(true);
    assertEquals(f.submit().status(), Status.OK);
    assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
    Content output = f.output(f._task, 1);
    ObjectVersion table = f.objectVersion(f.record(RecordKind.TABLE_CONFIG, TABLE));
    assertEquals(f.controller(sequence -> new OfflineSegmentCommand(f._history, "controller", 1, "controller",
        sequence, new PublishContent(f._owner, table, output))).status(), Status.OK);
    assertEquals(f.worker(new PublishOfflineOutputs(f._task, f.version(), 1, List.of(output),
        new Progress(1, "collision"))).status(), Status.CONFLICT);
    assertEquals(f.state().status(), TaskStatus.RUNNING);
    assertEquals(f.mutate(new Mutation(Action.UPDATE, RecordKind.TABLE_CONFIG, TABLE, table,
        MetadataProtocol.toTree(f.table()))).status(), Status.OK);
    assertEquals(f.worker(new PublishOfflineOutputs(f._task, f.version(), 1, List.of(output),
        new Progress(1, "changed input"))).status(), Status.CONFLICT);
    assertEquals(f._catalog.taskSnapshot(f._minion, 1, 100_000).status(), Status.RESPONSE_TOO_LARGE);
    assertEquals(f._catalog.taskSnapshot(f._minion, 128, 1024).status(), Status.RESPONSE_TOO_LARGE);
    var image = f._catalog.taskSnapshot(f._minion, 128, 100_000);
    assertEquals(image.status(), Status.OK);
    assertTrue(image.records().stream().noneMatch(record -> record.kind() == RecordKind.PARTICIPANT));
    f.restore();
  }

  @Test
  public void testLegacyInputEncodingAndSnapshotRemainReplayable() throws Exception {
    Fixture f = new Fixture();
    TaskCommand command = new TaskCommand(f._history, "controller", 1, "controller", f._sequence + 1,
        new Submit(f._owner, f._task, f.input(), List.of()));
    byte[] legacy = MetadataProtocol.encode(command);
    assertFalse(new String(legacy, StandardCharsets.UTF_8).contains("outputPolicy"));
    Command decoded = MetadataProtocol.decodeCommand(legacy);
    assertEquals(MetadataProtocol.encode(decoded), legacy);
    assertEquals(MetadataProtocol.digest(decoded), MetadataProtocol.digest(command));
    for (Input invalid : List.of(new Input(null, "minion", Map.of(), 2),
        new Input(f.input().queue(), null, Map.of(), 2), new Input(f.input().queue(), "minion", null, 2))) {
      TaskCommand rejected = new TaskCommand(f._history, "controller", 1, "controller", f._sequence + 1,
          new Submit(f._owner, f._task, invalid, List.of()));
      byte[] rejectedBytes = MetadataProtocol.encode(rejected);
      assertEquals(MetadataProtocol.encode(MetadataProtocol.decodeCommand(rejectedBytes)), rejectedBytes);
      assertEquals(f.task(new Submit(f._owner, f._task, invalid, List.of())).status(), Status.INVALID_REQUEST);
    }
    assertEquals(f.submit().status(), Status.OK);
    ObjectNode old = f._catalog.captureSnapshot().deepCopy();
    old.put("formatVersion", 9);
    MetadataCatalog restored = new MetadataCatalog(f._cluster, f._history);
    restored.restoreSnapshot(old);
    assertEquals(restored.captureSnapshot().path("formatVersion").asInt(), 10);
  }

  @Test
  public void testActionableAdmissionPreservesWatermarkAndReservesClaimCapacity() throws Exception {
    for (boolean largeInput : List.of(false, true)) {
      Fixture f = new Fixture(true);
      Input input = f.input();
      if (largeInput) {
        input = new Input(input.queue(), input.minionId(),
            Map.of("sourceUri", "file:///" + "a".repeat(16_000), "sourceSha256", "a".repeat(64)),
            input.maxAttempts(), input.outputPolicy());
      }
      UUID schedule = UUID.randomUUID();
      assertEquals(f.task(new CreateSchedule(f._owner, schedule, input, List.of(), 0, 0, 1)).status(), Status.OK);
      int admitted = 0;
      while (admitted < TaskProtocol.MAX_SNAPSHOT_RECORDS) {
        UUID id = admitted == 0 ? f._task : UUID.randomUUID();
        Result result = f.task(new Submit(f._owner, id, input, List.of()));
        if (result.status() == Status.NOT_READY) {
          break;
        }
        assertEquals(result.status(), Status.OK);
        admitted++;
      }
      assertTrue(admitted > 0 && admitted < TaskProtocol.MAX_SNAPSHOT_RECORDS);
      var image = f._catalog.taskSnapshot(f._minion, TaskProtocol.MAX_SNAPSHOT_RECORDS,
          TaskProtocol.MAX_SNAPSHOT_BYTES);
      assertEquals(image.status(), Status.OK);
      if (largeInput) {
        assertTrue(image.records().size() < TaskProtocol.MAX_SNAPSHOT_RECORDS - 1,
            "Byte admission must reject before the record bound is exhausted");
      }
      // Configuration creation adds no actionable work, even while this minion is full.
      assertEquals(f.task(new CreateSchedule(f._owner, UUID.randomUUID(), input, List.of(), 0, 0, 1)).status(),
          Status.OK);
      assertEquals(f.task(new FireSchedule(f._owner, schedule, 1)).status(), Status.NOT_READY);
      CatalogRecord retained = f.record(RecordKind.TASK_SCHEDULE, schedule.toString());
      assertEquals(retained.version(), 1L);
      assertEquals(TaskProtocol.decodeSchedule(retained.value()).firedOccurrences(), 0L);
      assertEquals(f.worker(new Claim(f._task, 1)).status(), Status.OK);
      assertEquals(f.worker(new Checkpoint(f._task, f.version(), 1,
          new Progress(1, "\u0000".repeat(TaskProtocol.MAX_DETAIL_LENGTH)))).status(), Status.OK);
      assertEquals(f._catalog.taskSnapshot(f._minion, TaskProtocol.MAX_SNAPSHOT_RECORDS,
          TaskProtocol.MAX_SNAPSHOT_BYTES).status(), Status.OK);
      // A failed first attempt can return to READY at the full queue; historical attempt records do not consume
      // actionable capacity, and the second claim still has its reserved slot.
      assertEquals(f.worker(new Complete(f._task, f.version(), 1, AttemptStatus.RETRYABLE_FAILED,
          new Progress(1, "retry"))).status(), Status.OK);
      assertEquals(f.task(new Retry(f._owner, f._task, f.version())).status(), Status.OK);
      assertEquals(f.worker(new Claim(f._task, f.version())).status(), Status.OK);
      assertEquals(f._catalog.taskSnapshot(f._minion, TaskProtocol.MAX_SNAPSHOT_RECORDS,
          TaskProtocol.MAX_SNAPSHOT_BYTES).status(), Status.OK);
      assertEquals(f.worker(new Complete(f._task, f.version(), 2, AttemptStatus.FATAL_FAILED,
          new Progress(0, "execution failed"))).status(), Status.OK);
      assertEquals(f.task(new FireSchedule(f._owner, schedule, 1)).status(), Status.OK);
      assertEquals(TaskProtocol.decodeSchedule(f.record(RecordKind.TASK_SCHEDULE, schedule.toString()).value())
          .firedOccurrences(), 1L);
      f.restore();
    }
  }

  private static final class Fixture {
    private final boolean _nativeTask;
    private final UUID _cluster = UUID.randomUUID();
    private final UUID _history = UUID.randomUUID();
    private final UUID _table = UUID.randomUUID();
    private final UUID _task = UUID.randomUUID();
    private MetadataCatalog _catalog = new MetadataCatalog(_cluster, _history);
    private final Map<ParticipantToken, Long> _workerSequences = new HashMap<>();
    private ParticipantToken _minion;
    private OwnerToken _owner;
    private long _index;
    private long _sequence;
    private TaskSessionCommand _lastWorker;
    private Result _lastWorkerResult;

    private Fixture() throws Exception {
      this(false);
    }

    private Fixture(boolean nativeTask) throws Exception {
      _nativeTask = nativeTask;
      assertEquals(apply(new RegisterClient(_history, "controller", UUID.randomUUID(), 0)).status(), Status.OK);
      assertEquals(member(new ActivateControlEpoch(1, "controller", UUID.randomUUID())).status(), Status.OK);
      assertEquals(member(new ConfigurePolicy(_catalog.membershipControl().controlEpoch(), 0,
          new Policy(1, 100, 3, 200, 300, 1000))).status(), Status.OK);
      ParticipantToken controller = register("controller", Role.CONTROLLER, 0);
      _minion = registerMinion("minion", 0);
      var layout = new OwnershipProtocol.Layout(1, 24, OwnershipProtocol.HashVersion.MURMUR2_UTF8_MASK_POSITIVE_V1);
      assertEquals(owner(new ConfigureLayout(_catalog.membershipControl().controlEpoch(), layout)).status(), Status.OK);
      var scope = OwnershipProtocol.tableScope(layout, TABLE);
      assertEquals(owner(new AssignOwner(_catalog.membershipControl().controlEpoch(), 1, scope, 0, controller))
          .status(),
          Status.OK);
      _owner = new OwnerToken(_history, 1, scope, 1, controller);
      assertEquals(mutate(Mutation.createTableConfig(_history, _table, table()),
          Mutation.createSchema(_history, UUID.randomUUID(), new Schema.SchemaBuilder().setSchemaName("orders")
              .addSingleValueDimension("id", DataType.INT).build())).status(), Status.OK);
    }

    private TableConfig table() {
      return new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").build();
    }

    private Input input() {
      if (_nativeTask) {
        return new Input(new QueueKey(TABLE, _table, TaskProtocol.NATIVE_SEGMENT_GENERATION_TASK), "minion",
            Map.of("sourceUri", "file:///inputs/source.json", "sourceSha256", "a".repeat(64)), 2,
            new OutputPolicy("file:///outputs", 1));
      }
      return new Input(new QueueKey(TABLE, _table, "TestTask"), "minion", Map.of("tableName", TABLE), 2);
    }

    private Content output(UUID taskId, long attempt) {
      String digest = "a".repeat(64);
      return new Content(TABLE, _table, TaskProtocol.outputSegmentName(taskId, attempt, 0),
          TaskProtocol.outputObjectId(_history, taskId, attempt, 0), digest,
          TaskProtocol.outputUri(input().outputPolicy(), _history, taskId, attempt, _minion.incarnation(), 0, digest),
          new LoaderDescriptor(1, 0, false, null, null, 1, 1, Map.of()));
    }

    private Result submit() throws IOException {
      return task(new Submit(_owner, _task, input(), List.of()));
    }

    private ParticipantToken register(String id, Role role, long epoch) throws IOException {
      var response = member(new Register(id, role, UUID.randomUUID(), epoch, Map.of(),
          _nativeTask && role == Role.MINION ? Set.of(Capability.MINION_TASK) : Set.of()));
      assertEquals(response.status(), Status.OK);
      ParticipantToken token = response.participant().participant();
      assertEquals(member(new SetReady(token, new Revision(_history, _index), true)).status(), Status.OK);
      return token;
    }

    private ParticipantToken registerMinion(String id, long epoch) throws IOException {
      ParticipantToken token = register(id, Role.MINION, epoch);
      assertEquals(apply(new TaskSessionCommand(token,
          new RegisterClient(_history, TaskProtocol.clientId(token), token.incarnation(), 0))).status(), Status.OK);
      return token;
    }

    private MembershipProtocol.Response member(MembershipProtocol.Operation operation) throws IOException {
      return MembershipProtocol.decodeResponse(_catalog.execute(MetadataProtocol.encode(
          new MembershipCommand(_history, "controller", operation)), 1, ++_index));
    }

    private Result controller(LongFunction<Command> command) throws IOException {
      if (_sequence > 0) {
        assertEquals(apply(new Acknowledge(_history, "controller", 1, "controller", _sequence)).status(), Status.OK);
      }
      return apply(command.apply(++_sequence));
    }

    private Result task(TaskProtocol.ControllerOperation operation) throws IOException {
      return controller(sequence -> new TaskCommand(_history, "controller", 1, "controller", sequence, operation));
    }

    private Result owner(OwnershipProtocol.Operation operation) throws IOException {
      return controller(sequence -> new OwnershipCommand(_history, "controller", 1, "controller", sequence, operation));
    }

    private Result mutate(Mutation... mutations) throws IOException {
      return controller(sequence -> new Transaction(_history, "controller", 1, "controller", sequence,
          List.of(mutations)));
    }

    private TaskSessionCommand workerCommand(ParticipantToken minion, TaskProtocol.WorkerOperation operation)
        throws IOException {
      long previous = _workerSequences.getOrDefault(minion, 0L);
      if (previous > 0) {
        assertEquals(apply(new TaskSessionCommand(minion,
            new Acknowledge(_history, TaskProtocol.clientId(minion), 1, "task", previous))).status(), Status.OK);
      }
      _workerSequences.put(minion, previous + 1);
      return new TaskSessionCommand(minion, new NativeTaskCommand(_history, TaskProtocol.clientId(minion), 1,
          "task", previous + 1, minion, operation));
    }

    private Result worker(ParticipantToken minion, TaskProtocol.WorkerOperation operation) throws IOException {
      _lastWorker = workerCommand(minion, operation);
      _lastWorkerResult = apply(_lastWorker);
      return _lastWorkerResult;
    }

    private Result worker(TaskProtocol.WorkerOperation operation) throws IOException {
      return worker(_minion, operation);
    }

    private CatalogRecord record(RecordKind kind, String name) throws IOException {
      Result result = MetadataProtocol.decodeResult(_catalog.query(MetadataProtocol.encode(new GetRecord(_history,
          kind, name))));
      assertEquals(result.status(), Status.OK);
      return result.record();
    }

    private TaskProtocol.TaskState state() throws IOException {
      return TaskProtocol.decodeTask(record(RecordKind.TASK, TaskProtocol.taskName(_task)).value());
    }

    private long version() throws IOException {
      return record(RecordKind.TASK, TaskProtocol.taskName(_task)).version();
    }

    private ObjectVersion objectVersion(CatalogRecord record) {
      return new ObjectVersion(_history, record.objectId(), record.version());
    }

    private Result apply(Command command) throws IOException {
      return MetadataProtocol.decodeResult(_catalog.execute(MetadataProtocol.encode(command), 1, ++_index));
    }

    private void restore() throws IOException {
      JsonNode snapshot = _catalog.captureSnapshot();
      MetadataCatalog restored = new MetadataCatalog(_cluster, _history);
      restored.restoreSnapshot(snapshot);
      assertEquals(restored.captureSnapshot(), snapshot);
      _catalog = restored;
    }
  }
}
