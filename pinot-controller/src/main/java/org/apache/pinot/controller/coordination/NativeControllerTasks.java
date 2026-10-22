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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.coordination.NativeTaskOutputIdentity;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Response;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipQuery;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.Layout;
import org.apache.pinot.coordination.ratis.metadata.OwnershipProtocol.OwnerToken;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.ControllerOperation;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.Input;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.ScheduleState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskState;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.TaskStatus;
import org.apache.pinot.coordination.ratis.transport.MembershipAccess;
import org.apache.pinot.coordination.ratis.transport.MetadataAccess;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;


/// Bounded native task scheduling and OFFLINE content reconciliation using committed membership rounds. A controller
/// claims only unowned or expired table shards. Commands and heartbeats retain their identity across unknown replies.
/// Methods serialize the controller request stream; immutable schedule and content identities survive process restart.
public final class NativeControllerTasks implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeControllerTasks.class);
  private static final FeedScope SCOPE = new FeedScope(EnumSet.of(RecordKind.TABLE_CONFIG, RecordKind.SCHEMA,
      RecordKind.CLUSTER_CONFIG, RecordKind.OFFLINE_SEGMENT_CONTENT,
      RecordKind.MEMBERSHIP_CONTROL, RecordKind.PARTICIPANT, RecordKind.OWNER_LAYOUT, RecordKind.OWNER_ASSIGNMENT,
      RecordKind.TASK_SCHEDULE, RecordKind.TASK_QUEUE, RecordKind.TASK, RecordKind.TASK_ATTEMPT,
      RecordKind.TASK_PUBLICATION, RecordKind.OFFLINE_REPLICA_ASSIGNMENT, RecordKind.CONTROLLER_OPERATION,
      RecordKind.OFFLINE_INSTALLATION), "native-controller-tasks");
  private final MetadataAccess _access;
  private final MembershipAccess _membership;
  private final UUID _history;
  private final String _participantId;
  private final String _clientId;
  private final UUID _clientIncarnation;
  private final long _timeoutMillis;
  private final long _pollMillis;
  @Nullable
  private final String _outputRoot;
  @Nullable
  private ScheduledExecutorService _executor;
  @Nullable
  private ParticipantToken _participant;
  @Nullable
  private MembershipProtocol.Register _registration;
  @Nullable
  private MembershipProtocol.Renewal _renewal;
  @Nullable
  private Command _pending;
  private boolean _closed;
  private int _scanCursor;
  @Nullable
  private volatile String _failure;

  /// The public schedule ID is also its immutable configuration generation. Round zero is useful for an immediate
  /// one-shot job; recurring callers should select a first round from the current task image.
  public record ScheduleRequest(String minionId, String sourceUri, String sourceSha256, long firstRound,
                                long intervalRounds, int occurrences, int maxAttempts) {
  }

  public NativeControllerTasks(MetadataAccess access, MembershipAccess membership, UUID history, String participantId,
      ControllerConf config) throws IOException {
    _access = access;
    _membership = membership;
    _history = history;
    _participantId = participantId;
    _clientId = "controller-tasks/" + NativeMetadataConfig.required(config, "id");
    _clientIncarnation = UUID.nameUUIDFromBytes((_history + "/" + _clientId).getBytes(StandardCharsets.UTF_8));
    _timeoutMillis = config.getProperty(NativeMetadataConfig.PREFIX + "controller.tasks.timeoutMs", 5000L);
    _pollMillis = config.getProperty(NativeMetadataConfig.PREFIX + "controller.tasks.pollMillis", 1000L);
    checkArgument(_timeoutMillis > 0 && _timeoutMillis <= 30_000 && _pollMillis > 0 && _pollMillis <= 30_000,
        "Native task RPC and polling intervals must be between 1 and 30000 milliseconds");
    String root = config.getProperty(NativeMetadataConfig.PREFIX + "controller.tasks.outputRoot");
    if (root == null) {
      _outputRoot = null;
    } else {
      Path directory = Path.of(root).toRealPath();
      checkArgument(Files.isDirectory(directory), "Native task output root must be an existing shared directory");
      _outputRoot = directory.toUri().toString();
    }
  }

  public synchronized void start() {
    checkArgument(!_closed && _executor == null, "Native task scheduler is already started or closed");
    _executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
      Thread thread = new Thread(runnable, "native-controller-tasks");
      thread.setDaemon(true);
      return thread;
    });
    _executor.scheduleWithFixedDelay(this::tickSafely, 0, _pollMillis, TimeUnit.MILLISECONDS);
  }

  private void tickSafely() {
    try {
      tick();
      _failure = null;
    } catch (Exception e) {
      _failure = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    }
  }

  /// One cycle attempts at most one schedule, retry or output reconciliation. All three share a fair scan and one
  /// durable request stream. A quorum failure leaves the pending command identity intact.
  synchronized void tick() throws IOException {
    ensureOpen();
    resolvePending();
    heartbeat();
    CatalogImage image = image();
    Map<UUID, CatalogRecord> records = image.records().stream()
        .collect(Collectors.toMap(CatalogRecord::objectId, record -> record));
    NativeTaskPlacementStatus placement = new NativeTaskPlacementStatus(image);
    List<CatalogRecord> candidates = image.records().stream()
        .filter(record -> record.kind() == RecordKind.TASK_SCHEDULE || record.kind() == RecordKind.TASK
            || record.kind() == RecordKind.OFFLINE_SEGMENT_CONTENT)
        .sorted(Comparator.comparing(record -> record.objectId().toString())).toList();
    int start = candidates.isEmpty() ? 0 : _scanCursor % candidates.size();
    for (int offset = 0; offset < candidates.size(); offset++) {
      int position = (start + offset) % candidates.size();
      _scanCursor = (position + 1) % candidates.size();
      CatalogRecord record = candidates.get(position);
      if (record.kind() == RecordKind.OFFLINE_SEGMENT_CONTENT) {
        Content output = OfflineSegmentProtocol.decodeContent(record.value());
        NativeTaskPlacementStatus.Output status = placement.output(null, output);
        if (!status.needsReconciliation()) {
          continue;
        }
        OwnerToken owner = owner(image, output.tableName());
        if (owner != null) {
          Result reconciled = command(sequence -> new MetadataProtocol.OfflineSegmentCommand(
              _history, _clientId, 1, "tasks", sequence,
              new OfflineSegmentProtocol.ReconcileContent(owner, output.segmentObjectId())));
          require(reconciled.status() == Status.OK,
              "Native output reconciliation did not commit: " + reconciled.status());
          return;
        }
        continue;
      }
      if (record.kind() == RecordKind.TASK_SCHEDULE) {
        ScheduleState schedule = TaskProtocol.decodeSchedule(record.value());
        if (schedule.firedOccurrences() >= schedule.maxOccurrences()) {
          continue;
        }
        long due = Math.addExact(schedule.firstRound(),
            Math.multiplyExact(schedule.intervalRounds(), schedule.firedOccurrences()));
        ControlState control = control(image);
        if (control.round() < due || control.policy().version() != schedule.policyVersion()) {
          continue;
        }
        OwnerToken owner = owner(image, schedule.input().queue().tableName());
        if (owner != null) {
          task(new TaskProtocol.FireSchedule(owner, schedule.scheduleId(), record.version()));
          return;
        }
      }
      if (record.kind() == RecordKind.TASK) {
        TaskState task = TaskProtocol.decodeTask(record.value());
        if ((task.status() == TaskStatus.READY || task.status() == TaskStatus.RUNNING)
            && task.dependencies().stream().anyMatch(dependency -> {
              CatalogRecord input = records.get(dependency.objectId());
              return input == null || input.deleted() || input.version() != dependency.version();
            })) {
          OwnerToken owner = owner(image, task.input().queue().tableName());
          if (owner != null) {
            task(new TaskProtocol.Cancel(owner, task.taskId(), record.version(), "Captured task inputs changed"));
            return;
          }
          continue;
        }
        if (task.status() == TaskStatus.RUNNING
            || task.status() == TaskStatus.RETRYABLE_FAILED && task.attempt() < task.input().maxAttempts()) {
          if (task.status() == TaskStatus.RUNNING && attemptIsLive(image, task)) {
            continue;
          }
          OwnerToken owner = owner(image, task.input().queue().tableName());
          if (owner != null) {
            // The state machine alone decides whether the previous exact worker session is expired or fenced.
            task(task.attempt() < task.input().maxAttempts()
                ? new TaskProtocol.Retry(owner, task.taskId(), record.version())
                : new TaskProtocol.Cancel(owner, task.taskId(), record.version(),
                    "Worker expired after final attempt"));
            return;
          }
        }
      }
    }
  }

  /// Publishes a validated immutable archive using the same fenced owner and durable request stream as reconciliation.
  /// Archive validation and file I/O must finish outside this monitor; replicated apply checks both captured versions.
  public synchronized Result publish(Content content, ObjectVersion expectedTable, ObjectVersion expectedSchema)
      throws IOException {
    ensureOpen();
    checkArgument(content != null && expectedTable != null && expectedSchema != null,
        "Content and validated table/schema versions are required");
    resolvePending();
    heartbeat();
    CatalogImage image = image();
    Revision revision = new Revision(_history, image.cursor().index());
    CatalogRecord table = record(image, RecordKind.TABLE_CONFIG, content.tableName());
    CatalogRecord schema = record(image, RecordKind.SCHEMA,
        TableNameBuilder.extractRawTableName(content.tableName()));
    if (!matches(table, expectedTable) || !matches(schema, expectedSchema)
        || !content.tableObjectId().equals(expectedTable.objectId())) {
      return new Result(Status.CONFLICT, revision, "Upload validation dependencies changed", null, null, null);
    }
    CatalogRecord existing = record(image, RecordKind.OFFLINE_SEGMENT_CONTENT,
        OfflineSegmentProtocol.contentName(content.tableObjectId(), content.segmentName()));
    if (existing != null) {
      boolean identical = !existing.deleted() && existing.objectId().equals(content.segmentObjectId())
          && content.equals(OfflineSegmentProtocol.decodeContent(existing.value()));
      // Any controller can resolve an existing immutable publication through a committed read barrier. This
      // does not create new content or acquire another controller's live shard ownership.
      return new Result(identical ? Status.OK : Status.CONFLICT, revision,
          identical ? "Immutable content already published" : "Segment name identifies different immutable content",
          null, null, existing);
    }
    OwnerToken owner = owner(image, content.tableName());
    if (owner == null) {
      throw new IOException("Segment table is owned by another controller; submit to that controller");
    }
    return command(sequence -> new MetadataProtocol.OfflineSegmentCommand(_history, _clientId, 1, "tasks", sequence,
        new OfflineSegmentProtocol.PublishValidatedContent(owner, expectedTable, expectedSchema, content)));
  }

  private boolean matches(@Nullable CatalogRecord record, ObjectVersion version) {
    return record != null && !record.deleted() && _history.equals(version.historyId())
        && record.objectId().equals(version.objectId()) && record.version() == version.version();
  }

  public synchronized Result schedule(String tableName, UUID scheduleId, ScheduleRequest request) throws IOException {
    ensureOpen();
    checkArgument(_outputRoot != null, "Native controller task outputRoot is not configured");
    checkArgument(request != null && request.minionId() != null && request.sourceUri() != null
        && request.sourceSha256() != null, "Minion identity, source URI and digest are required");
    resolvePending();
    heartbeat();
    CatalogImage image = image();
    CatalogRecord table = record(image, RecordKind.TABLE_CONFIG, tableName);
    checkArgument(table != null, "Table does not exist");
    Input input = new Input(new TaskProtocol.QueueKey(tableName, table.objectId(), NativeTaskOutputIdentity.TASK_TYPE),
        request.minionId(), Map.of("sourceUri", request.sourceUri(), "sourceSha256", request.sourceSha256()),
        request.maxAttempts(), new TaskProtocol.OutputPolicy(_outputRoot, 1));
    OwnerToken owner = owner(image, tableName);
    if (owner == null) {
      throw new IOException("Task table is owned by another controller; submit to that controller");
    }
    return task(new TaskProtocol.CreateSchedule(owner, scheduleId, input, List.of(), request.firstRound(),
        request.intervalRounds(), request.occurrences()));
  }

  public synchronized Result cancel(String tableName, UUID taskId, long expectedVersion) throws IOException {
    ensureOpen();
    resolvePending();
    heartbeat();
    CatalogImage image = image();
    CatalogRecord record = record(image, RecordKind.TASK, taskId.toString());
    checkArgument(record != null
            && TaskProtocol.decodeTask(record.value()).input().queue().tableName().equals(tableName),
        "Task does not belong to this table");
    OwnerToken owner = owner(image, tableName);
    if (owner == null) {
      throw new IOException("Task table is owned by another controller");
    }
    return task(new TaskProtocol.Cancel(owner, taskId, expectedVersion, "Cancelled through controller API"));
  }

  public synchronized CatalogImage snapshot() throws IOException {
    ensureOpen();
    return image();
  }

  @Nullable
  public String failure() {
    return _failure;
  }

  private CatalogImage image() throws IOException {
    return _access.image(SCOPE, 4096, MetadataProtocol.MAX_IMAGE_BYTES, _timeoutMillis);
  }

  private void heartbeat() throws IOException {
    Response current = _membership.query(new MembershipQuery(_history,
        new MembershipProtocol.GetParticipant(_participantId)), _timeoutMillis);
    require(current.status() == Status.OK || current.status() == Status.NOT_FOUND, "Cannot read controller session");
    require(current.control() != null && current.control().controlEpoch() != null && current.control().policy() != null,
        "Native task membership control is unavailable");
    ParticipantState observed = current.participant();
    boolean live = observed != null && !observed.retired()
        && observed.expiryRound() > current.control().round();
    if (_participant != null && (observed == null || !observed.participant().equals(_participant) || !live)) {
      require(!live, "Native task controller session was replaced by a live incarnation");
      _participant = null;
      _registration = null;
      _renewal = null;
    }
    if (_participant == null) {
      // A lost registration reply retains its exact request. Resolve against the committed observation before
      // allocating another incarnation: a live matching registration is retried, a live different one is fenced.
      if (_registration != null && observed != null) {
        boolean matching = observed.participant().incarnation().equals(_registration.incarnation())
            && observed.participant().registrationEpoch() == _registration.expectedEpoch() + 1;
        require(!live || matching, "Another native task controller incarnation remains live");
        if (!live && (matching || observed.participant().registrationEpoch() != _registration.expectedEpoch())) {
          _registration = null;
        }
      }
      if (_registration == null) {
        require(!live, "Another native task controller incarnation remains live");
        long epoch = observed == null ? 0 : observed.participant().registrationEpoch();
        _registration = new MembershipProtocol.Register(_participantId, MembershipProtocol.Role.CONTROLLER,
            UUID.randomUUID(), epoch, Map.of(), Set.of());
      }
      Response registered = _membership.execute(new MembershipCommand(_history, _participantId, _registration),
          _timeoutMillis);
      require(registered.status() == Status.OK && registered.participant() != null,
          "Native task controller registration is not available");
      _participant = registered.participant().participant();
      current = registered;
    }
    require(current.participant() != null && current.participant().participant().equals(_participant),
        "Native task controller session was replaced");
    if (_renewal == null) {
      _renewal = new MembershipProtocol.Renewal(_participant,
          current.participant().lastRenewalSequence() + 1, current.revision(), Set.of());
    }
    Response renewed = _membership.execute(new MembershipCommand(_history, _participantId,
        new MembershipProtocol.RenewBatch(List.of(_renewal))), _timeoutMillis);
    require(renewed.status() == Status.OK && renewed.renewals().size() == 1
        && renewed.renewals().get(0).status() == Status.OK, "Native task controller renewal is unavailable");
    _renewal = null;
    Response ready = _membership.execute(new MembershipCommand(_history, _participantId,
        new MembershipProtocol.SetReady(_participant, renewed.revision(), true)), _timeoutMillis);
    require(ready.status() == Status.OK, "Native task controller is not ready");
  }

  private static boolean attemptIsLive(CatalogImage image, TaskState task) throws IOException {
    CatalogRecord attempt = record(image, RecordKind.TASK_ATTEMPT,
        TaskProtocol.attemptName(task.taskId(), task.attempt()));
    require(attempt != null, "Running task has no current attempt");
    ParticipantToken minion = TaskProtocol.decodeAttempt(attempt.value()).minion();
    CatalogRecord registration = record(image, RecordKind.PARTICIPANT, minion.participantId());
    if (registration == null) {
      return false;
    }
    ParticipantState participant = MembershipProtocol.decodeParticipant(registration.value());
    return participant.participant().equals(minion) && !participant.retired()
        && participant.expiryRound() > control(image).round();
  }

  @Nullable
  private OwnerToken owner(CatalogImage image, String tableName) throws IOException {
    CatalogRecord layoutRecord = record(image, RecordKind.OWNER_LAYOUT, OwnershipProtocol.LAYOUT_NAME);
    Layout layout;
    if (layoutRecord == null) {
      layout = new Layout(1, 24, OwnershipProtocol.HashVersion.MURMUR2_UTF8_MASK_POSITIVE_V1);
      Layout initial = layout;
      Result configured = command(sequence -> new MetadataProtocol.OwnershipCommand(_history, _clientId, 1,
          "tasks", sequence, new OwnershipProtocol.ConfigureLayout(controlUnchecked(image).controlEpoch(), initial)));
      require(configured.status() == Status.OK, "Ownership layout initialization changed");
    } else {
      layout = OwnershipProtocol.decodeLayout(layoutRecord.value());
    }
    var scope = OwnershipProtocol.tableScope(layout, tableName);
    CatalogRecord assignmentRecord = record(image, RecordKind.OWNER_ASSIGNMENT, OwnershipProtocol.name(scope));
    Assignment assignment =
        assignmentRecord == null ? null : OwnershipProtocol.decodeAssignment(assignmentRecord.value());
    long epoch = assignment == null ? 0 : assignment.ownerEpoch();
    if (assignment != null && _participant.equals(assignment.owner())) {
      return new OwnerToken(_history, layout.generation(), scope, epoch, _participant);
    }
    if (assignment != null && assignment.owner() != null) {
      CatalogRecord previous = record(image, RecordKind.PARTICIPANT, assignment.owner().participantId());
      ParticipantState participant = previous == null ? null : MembershipProtocol.decodeParticipant(previous.value());
      if (participant != null && participant.participant().equals(assignment.owner()) && !participant.retired()
          && participant.expiryRound() > control(image).round()) {
        return null;
      }
    }
    long generation = layout.generation();
    Result assigned = command(sequence -> new MetadataProtocol.OwnershipCommand(_history, _clientId, 1, "tasks",
        sequence, new OwnershipProtocol.AssignOwner(controlUnchecked(image).controlEpoch(), generation, scope, epoch,
        _participant)));
    require(assigned.status() == Status.OK, "Table ownership changed while scheduling");
    return new OwnerToken(_history, generation, scope, epoch + 1, _participant);
  }

  private Result task(ControllerOperation operation) throws IOException {
    return command(sequence -> new MetadataProtocol.TaskCommand(_history, _clientId, 1, "tasks", sequence, operation));
  }

  private Result command(LongFunction<Command> factory) throws IOException {
    resolvePending();
    Result registration = _access.execute(new MetadataProtocol.RegisterClient(_history, _clientId,
        _clientIncarnation, 0), _timeoutMillis);
    require(registration.status() == Status.OK && Long.valueOf(1).equals(registration.clientEpoch()),
        "Task controller request stream is fenced");
    var tip = _access.query(new MetadataProtocol.GetStreamTip(_history, _clientId, 1, "tasks"), _timeoutMillis);
    require(tip.status() == Status.OK && tip.sequence() >= 0 && tip.sequence() < Long.MAX_VALUE,
        "Task controller request stream is unavailable");
    if (tip.sequence() > tip.floor()) {
      require(tip.outcome() != null, "Task controller request outcome is unresolved");
      Result acknowledged = _access.execute(new MetadataProtocol.Acknowledge(_history, _clientId, 1, "tasks",
          tip.sequence()), _timeoutMillis);
      require(acknowledged.status() == Status.OK, "Cannot acknowledge resolved task controller outcome");
    }
    _pending = factory.apply(tip.sequence() + 1);
    Result result = _access.execute(_pending, _timeoutMillis);
    _pending = null;
    return result;
  }

  private void resolvePending() throws IOException {
    if (_pending != null) {
      _access.execute(_pending, _timeoutMillis);
      _pending = null;
    }
  }

  @Nullable
  private static CatalogRecord record(CatalogImage image, RecordKind kind, String name) {
    return image.records().stream().filter(value -> value.kind() == kind && value.name().equals(name)).findFirst()
        .orElse(null);
  }

  private static ControlState control(CatalogImage image) throws IOException {
    CatalogRecord record = record(image, RecordKind.MEMBERSHIP_CONTROL, MembershipProtocol.CONTROL_NAME);
    require(record != null, "Native membership control is unavailable");
    return MembershipProtocol.decodeControl(record.value());
  }

  private static ControlState controlUnchecked(CatalogImage image) {
    try {
      return control(image);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void require(boolean valid, String message) throws IOException {
    if (!valid) {
      throw new IOException(message);
    }
  }

  private void ensureOpen() throws IOException {
    require(!_closed, "Native task scheduler is closed");
  }

  @Override
  public synchronized void close() throws IOException {
    if (_closed) {
      return;
    }
    _closed = true;
    if (_executor != null) {
      _executor.shutdownNow();
    }
    if (_participant != null) {
      try {
        Response response = _membership.execute(new MembershipCommand(_history, _participantId,
            new MembershipProtocol.Unregister(_participant)), _timeoutMillis);
        if (response.status() != Status.OK) {
          LOGGER.warn("Native task scheduler stopped; controller retirement returned {}", response.status());
        }
      } catch (IOException e) {
        _failure = "Stopped; controller retirement outcome is unknown";
        LOGGER.warn("Native task scheduler stopped; controller retirement outcome is unknown", e);
      }
    }
  }
}
