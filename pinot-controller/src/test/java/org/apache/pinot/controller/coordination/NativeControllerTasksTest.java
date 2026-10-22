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
package org.apache.pinot.controller.coordination;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataCatalog;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetStreamTip;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipQuery;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Query;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ReadBarrier;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.StreamTipResult;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol;
import org.apache.pinot.coordination.ratis.transport.MembershipAccess;
import org.apache.pinot.coordination.ratis.transport.MetadataAccess;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Runs controller scheduling against the durable catalog to cover lost replies and owner recovery across restart.
public class NativeControllerTasksTest {
  @Test
  public void testScheduleLostReplyFailoverAndImmutableOccurrence() throws Exception {
    try (Fixture f = new Fixture();
        NativeControllerTasks first = f.controller("first");
        NativeControllerTasks competing = f.controller("second")) {
      UUID scheduleId = UUID.randomUUID();
      var request = new NativeControllerTasks.ScheduleRequest("minion", "file:///approved/rows.json", "1".repeat(64),
          0, 0, 1, 2);
      f._loseScheduleReply = true;
      expectThrows(IOException.class, () -> first.schedule("orders_OFFLINE", scheduleId, request));
      assertEquals(first.schedule("orders_OFFLINE", scheduleId, request).status(), Status.OK);
      expectThrows(IOException.class, () -> competing.schedule("orders_OFFLINE", UUID.randomUUID(), request));
      first.tick();
      first.tick();
      assertEquals(f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK), "tasks")).records().size(), 1);
      first.close();
      MetadataCatalog recovered = new MetadataCatalog(f._cluster, f._history);
      recovered.restoreSnapshot(f._catalog.captureSnapshot());
      f._catalog = recovered;
      try (NativeControllerTasks replacement = f.controller("first")) {
        assertEquals(replacement.schedule("orders_OFFLINE", scheduleId, request).status(), Status.OK);
        replacement.tick();
        assertEquals(f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK), "tasks")).records().size(), 1);
        var different = new NativeControllerTasks.ScheduleRequest("minion", "file:///approved/other.json",
            "1".repeat(64), 0, 0, 1, 2);
        assertEquals(replacement.schedule("orders_OFFLINE", scheduleId, different).status(), Status.CONFLICT);
        var schedules = f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK_SCHEDULE), "schedules")).records();
        assertEquals(TaskProtocol.decodeSchedule(schedules.getFirst().value()).firedOccurrences(), 1L);
      }
    }
  }

  @Test
  public void testUnknownMembershipRepliesAndExpiredControllerRecoverWithoutStealingLiveSession() throws Exception {
    try (Fixture f = new Fixture(); NativeControllerTasks controller = f.controller("first");
        NativeControllerTasks competing = f.controller("first")) {
      f._loseRegistrationReply = true;
      expectThrows(IOException.class, controller::tick);
      var first = f.participant("Controller_first").participant();
      controller.tick();
      assertEquals(f.participant("Controller_first").participant(), first,
          "An unknown registration outcome must retry the same incarnation");
      f._loseRenewalReply = true;
      expectThrows(IOException.class, controller::tick);
      long renewal = f.participant("Controller_first").lastRenewalSequence();
      controller.tick();
      assertEquals(f.participant("Controller_first").lastRenewalSequence(), renewal,
          "An unknown renewal outcome must not allocate another sequence");
      expectThrows(IOException.class, competing::tick);
      assertEquals(f.participant("Controller_first").participant(), first);
      f.expireLeases();
      controller.tick();
      var recovered = f.participant("Controller_first").participant();
      assertEquals(recovered.registrationEpoch(), first.registrationEpoch() + 1);
      assertNotEquals(recovered.incarnation(), first.incarnation());
      assertTrue(f.participant("Controller_first").ready());
      f.expireLeases();
      competing.tick();
      var replacement = f.participant("Controller_first").participant();
      expectThrows(IOException.class, controller::tick);
      assertEquals(f.participant("Controller_first").participant(), replacement,
          "A recovered process must not replace another live incarnation");
    }
  }

  @Test
  public void testLiveAttemptDoesNotWriteRetriesAndExpiredFinalAttemptIsFenced() throws Exception {
    try (Fixture f = new Fixture(); NativeControllerTasks controller = f.controller("first")) {
      var request = new NativeControllerTasks.ScheduleRequest("minion", "file:///approved/rows.json", "1".repeat(64),
          0, 0, 1, 1);
      assertEquals(controller.schedule("orders_OFFLINE", UUID.randomUUID(), request).status(), Status.OK);
      controller.tick();
      var taskRecord = f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK), "tasks")).records().getFirst();
      var task = TaskProtocol.decodeTask(taskRecord.value());
      var minionRead = f.query(new MembershipQuery(f._history, new MembershipProtocol.GetParticipant("minion")), 1000);
      var minion = minionRead.participant().participant();
      assertEquals(f.execute(new MembershipCommand(f._history, "minion", new MembershipProtocol.SetReady(minion,
          minionRead.revision(), true)), 1000).status(), Status.OK);
      assertEquals(f.execute(new MetadataProtocol.TaskSessionCommand(minion, new MetadataProtocol.RegisterClient(
          f._history, TaskProtocol.clientId(minion), minion.incarnation(), 0)), 1000).status(), Status.OK);
      assertEquals(f.execute(new MetadataProtocol.TaskSessionCommand(minion, new MetadataProtocol.NativeTaskCommand(
          f._history, TaskProtocol.clientId(minion), 1, "work", 1, minion,
          new TaskProtocol.Claim(task.taskId(), taskRecord.version()))), 1000).status(), Status.OK);
      int taskCommands = f._taskCommands;
      controller.tick();
      assertEquals(f._taskCommands, taskCommands, "A live exact attempt must not generate doomed retry commands");
      f.expireLeases();
      controller.tick();
      var current = f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK), "tasks")).records().getFirst();
      assertEquals(TaskProtocol.decodeTask(current.value()).status(), TaskProtocol.TaskStatus.CANCELLED);
      var attempt = f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK_ATTEMPT), "attempts"))
          .records().getFirst();
      assertEquals(TaskProtocol.decodeAttempt(attempt.value()).status(), TaskProtocol.AttemptStatus.FENCED);
    }
  }

  @Test
  public void testChangedInputsCancelReadyAndRunningTasks() throws Exception {
    for (boolean running : List.of(false, true)) {
      try (Fixture f = new Fixture(); NativeControllerTasks controller = f.controller("first")) {
        var request = new NativeControllerTasks.ScheduleRequest("minion", "file:///approved/rows.json", "1".repeat(64),
            0, 0, 1, 2);
        assertEquals(controller.schedule("orders_OFFLINE", UUID.randomUUID(), request).status(), Status.OK);
        controller.tick();
        var taskRecord = f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK), "tasks")).records().getFirst();
        var task = TaskProtocol.decodeTask(taskRecord.value());
        if (running) {
          var read = f.query(new MembershipQuery(f._history, new MembershipProtocol.GetParticipant("minion")), 1000);
          var minion = read.participant().participant();
          assertEquals(f.execute(new MembershipCommand(f._history, "minion", new MembershipProtocol.SetReady(minion,
              read.revision(), true)), 1000).status(), Status.OK);
          assertEquals(f.execute(new MetadataProtocol.TaskSessionCommand(minion, new MetadataProtocol.RegisterClient(
              f._history, TaskProtocol.clientId(minion), minion.incarnation(), 0)), 1000).status(), Status.OK);
          assertEquals(f.execute(new MetadataProtocol.TaskSessionCommand(minion, new MetadataProtocol.NativeTaskCommand(
              f._history, TaskProtocol.clientId(minion), 1, "work", 1, minion,
              new TaskProtocol.Claim(task.taskId(), taskRecord.version()))), 1000).status(), Status.OK);
        }
        var schema = f._catalog.captureImage(new FeedScope(Set.of(RecordKind.SCHEMA), "schema")).records().getFirst();
        assertEquals(f.execute(new MetadataProtocol.Acknowledge(f._history, "bootstrap", 1, "config", 1), 1000)
            .status(), Status.OK);
        assertEquals(f.execute(new MetadataProtocol.Transaction(f._history, "bootstrap", 1, "config", 2,
            List.of(Mutation.updateSchema(new ObjectVersion(f._history, schema.objectId(), schema.version()),
                new Schema.SchemaBuilder().setSchemaName("orders").addSingleValueDimension("id", DataType.STRING)
                    .build()))), 1000).status(), Status.OK);
        controller.tick();
        var cancelled = f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK), "tasks")).records().getFirst();
        assertEquals(TaskProtocol.decodeTask(cancelled.value()).status(), TaskProtocol.TaskStatus.CANCELLED);
        if (running) {
          var attempt = f._catalog.captureImage(new FeedScope(Set.of(RecordKind.TASK_ATTEMPT), "attempts"))
              .records().getFirst();
          assertEquals(TaskProtocol.decodeAttempt(attempt.value()).status(), TaskProtocol.AttemptStatus.FENCED);
        }
      }
    }
  }

  private static final class Fixture implements MetadataAccess, MembershipAccess, AutoCloseable {
    private final UUID _cluster = UUID.randomUUID();
    private final UUID _history = UUID.randomUUID();
    private final Path _output = Files.createTempDirectory("native-controller-tasks-");
    private MetadataCatalog _catalog = new MetadataCatalog(_cluster, _history);
    private long _index;
    private boolean _loseScheduleReply;
    private boolean _loseRegistrationReply;
    private boolean _loseRenewalReply;
    private int _taskCommands;

    private Fixture() throws Exception {
      var control = execute(new MembershipCommand(_history, "bootstrap",
          new MembershipProtocol.ActivateControlEpoch(1, "leader", UUID.randomUUID())), 1000);
      assertEquals(execute(new MembershipCommand(_history, "bootstrap",
          new MembershipProtocol.ConfigurePolicy(control.control().controlEpoch(), 0,
              new MembershipProtocol.Policy(1, 1000, 4, 1000, 500, 100))), 1000).status(), Status.OK);
      assertEquals(execute(new MembershipCommand(_history, "bootstrap", new MembershipProtocol.Register("minion",
          MembershipProtocol.Role.MINION, UUID.randomUUID(), 0, Map.of(),
          Set.of(MembershipProtocol.Capability.MINION_TASK))), 1000).status(), Status.OK);
      assertEquals(execute(new MetadataProtocol.RegisterClient(_history, "bootstrap", UUID.randomUUID(), 0),
          1000).status(), Status.OK);
      Schema schema = new Schema.SchemaBuilder().setSchemaName("orders")
          .addSingleValueDimension("id", DataType.LONG).build();
      var table = new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").setNumReplicas(1).build();
      assertEquals(execute(new MetadataProtocol.Transaction(_history, "bootstrap", 1, "config", 1, List.of(
          Mutation.createSchema(_history, UUID.randomUUID(), schema),
          Mutation.createTableConfig(_history, UUID.randomUUID(), table))), 1000).status(),
          Status.OK);
    }

    private NativeControllerTasks controller(String id) throws IOException {
      ControllerConf conf = new ControllerConf();
      conf.setProperty(NativeMetadataConfig.PREFIX + "id", id);
      conf.setProperty(NativeMetadataConfig.PREFIX + "controller.tasks.outputRoot", _output.toString());
      return new NativeControllerTasks(this, this, _history, "Controller_" + id, conf);
    }

    private MembershipProtocol.ParticipantState participant(String id) throws IOException {
      return query(new MembershipQuery(_history, new MembershipProtocol.GetParticipant(id)), 1000).participant();
    }

    private void expireLeases() throws IOException {
      for (int i = 0; i < 4; i++) {
        var control = _catalog.membershipControl();
        assertEquals(execute(new MembershipCommand(_history, "bootstrap", new MembershipProtocol.Tick(
            control.controlEpoch(), control.round(), control.policy().version())), 1000).status(), Status.OK);
      }
    }

    private byte[] apply(Command command) throws IOException {
      return _catalog.execute(MetadataProtocol.encode(command), 1, ++_index);
    }

    @Override
    public Result execute(Command command, long timeoutMillis) throws IOException {
      Result result = MetadataProtocol.decodeResult(apply(command));
      if (command instanceof MetadataProtocol.TaskCommand) {
        _taskCommands++;
      }
      if (_loseScheduleReply && command instanceof MetadataProtocol.TaskCommand task
          && task.request() instanceof TaskProtocol.CreateSchedule) {
        _loseScheduleReply = false;
        assertEquals(result.status(), Status.OK);
        throw new IOException("Lost response after committed schedule");
      }
      return result;
    }

    @Override
    public MembershipProtocol.Response execute(MembershipCommand command, long timeoutMillis) throws IOException {
      MembershipProtocol.Response result = MembershipProtocol.decodeResponse(apply(command));
      if (_loseRegistrationReply && command.request() instanceof MembershipProtocol.Register) {
        _loseRegistrationReply = false;
        throw new IOException("Lost committed registration response");
      }
      if (_loseRenewalReply && command.request() instanceof MembershipProtocol.RenewBatch) {
        _loseRenewalReply = false;
        throw new IOException("Lost committed renewal response");
      }
      return result;
    }

    @Override
    public MembershipProtocol.Response query(MembershipQuery query, long timeoutMillis) throws IOException {
      return MembershipProtocol.decodeResponse(apply(new ReadBarrier(query)));
    }

    @Override
    public Result query(Query query, long timeoutMillis) throws IOException {
      return MetadataProtocol.decodeResult(apply(new ReadBarrier(query)));
    }

    @Override
    public StreamTipResult query(GetStreamTip query, long timeoutMillis) throws IOException {
      return MetadataProtocol.decodeStreamTipResult(apply(new ReadBarrier(query)));
    }

    @Override
    public CatalogImage image(FeedScope scope, int maxRecords, int maxBytes, long timeoutMillis) throws IOException {
      return _catalog.captureImage(scope, maxRecords, maxBytes);
    }

    @Override
    public ChangePage changes(FeedScope scope, FeedCursor cursor, int maxTransactions, int maxRecords, int maxBytes,
        long timeoutMillis) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
      assertTrue(Files.isDirectory(_output));
      FileUtils.deleteDirectory(_output.toFile());
    }
  }
}
