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
package org.apache.pinot.minion.coordination;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.coordination.MetadataRpcConnection;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;


/// Native task participant behind the normal minion starter. The single worker consumes a complete own-session
/// projection, stages immutable attempt outputs, and publishes only through consensus fencing. Membership uses
/// independent channels/threads. No Helix connection, legacy plugin, singleton context, or query listener is created.
public final class NativeMinionRuntime implements AutoCloseable, ServiceStatus.ServiceStatusCallback {
  public static final String PREFIX = NativeMetadataConfig.PREFIX + "minion.";
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeMinionRuntime.class);
  private final NativeMetadataConfig _metadataConfig;
  private final Path _dataDirectory;
  private final Path _sourceRoot;
  private final Path _outputRoot;
  private final long _maxSourceBytes;
  private final int _maxRows;
  private final long _pollMillis;
  private final UUID _incarnation = UUID.randomUUID();
  private final Map<String, Long> _executedAttempts = new HashMap<>();
  private final ScheduledExecutorService _heartbeats = Executors.newSingleThreadScheduledExecutor(
      runnable -> new Thread(runnable, "native-minion-membership"));
  private final ScheduledExecutorService _worker = Executors.newSingleThreadScheduledExecutor(
      runnable -> new Thread(runnable, "native-minion-task"));
  @Nullable
  private Rpc _membership;
  @Nullable
  private Rpc _work;
  @Nullable
  private FileChannel _lockFile;
  @Nullable
  private FileLock _directoryLock;
  @Nullable
  private NativeSegmentGenerationTask _generator;
  @Nullable
  private NativeTaskOutbox _outbox;
  @Nullable
  private volatile MetadataRpc.ParticipantToken _participant;
  private volatile MetadataRpc.Revision _revision;
  private volatile boolean _healthy;
  private volatile boolean _hydrated;
  private volatile boolean _closed;
  private volatile boolean _fenced;
  @Nullable
  private volatile String _failure = "Not started";
  private boolean _started;
  private boolean _statusRegistered;
  private long _renewal = 1;
  @Nullable
  private MetadataRpc.MembershipCommandRequest _pendingRenewal;

  /// The transport must preserve payloads across retries and bound each invocation by the supplied timeout.
  interface Rpc extends AutoCloseable {
    MetadataRpc.MembershipResponse membershipRead(MetadataRpc.MembershipReadRequest request, long timeout);
    MetadataRpc.MembershipResponse membershipExecute(MetadataRpc.MembershipCommandRequest request, long timeout);
    MetadataRpc.TaskWorkResult taskWork(MetadataRpc.TaskWorkRequest request, long timeout);
    MetadataRpc.CommandResult taskExecute(MetadataRpc.TaskCommandRequest request, long timeout);
    @Override
    void close();
  }

  private static final class Connection extends MetadataRpcConnection implements Rpc {
    private Connection(NativeMetadataConfig config) throws IOException {
      super(config.endpoints(), config.certificate(), config.privateKey(), config.trustCertificates(),
          config.rpcTimeoutMillis());
    }
  }

  public NativeMinionRuntime(PinotConfiguration config) {
    _metadataConfig = new NativeMetadataConfig(config);
    String instanceId = new MinionConf(config.toMap()).getInstanceId();
    checkArgument(_metadataConfig.participantId().equals(instanceId) && instanceId.startsWith("Minion_"),
        "Native minion requires an explicit Minion_ instance ID matching its participant ID");
    _dataDirectory = directory(config, CommonConstants.Helix.Instance.DATA_DIR_KEY);
    _sourceRoot = directory(config, PREFIX + "sourceRoot");
    _outputRoot = directory(config, PREFIX + "outputRoot");
    _maxSourceBytes = config.getProperty(PREFIX + "maxSourceBytes", 16L * 1024 * 1024);
    _maxRows = config.getProperty(PREFIX + "maxRows", 100_000);
    _pollMillis = config.getProperty(PREFIX + "pollMillis", 1000L);
    checkArgument(_pollMillis > 0 && _pollMillis <= 30_000, "Native task poll interval must be between 1 and 30000 ms");
    _revision = MetadataRpc.Revision.newBuilder().setHistoryId(_metadataConfig.historyId().toString()).build();
  }

  @VisibleForTesting
  NativeMinionRuntime(PinotConfiguration config, Rpc membership, Rpc work) {
    this(config);
    checkArgument(membership != work, "Membership requires independent transport admission");
    _membership = membership;
    _work = work;
  }

  private static Path directory(PinotConfiguration config, String key) {
    String value = config.getProperty(key);
    checkArgument(value != null && !value.isBlank() && Path.of(value).isAbsolute(),
        "Native minion requires an explicit absolute directory: %s", key);
    return Path.of(value).normalize();
  }

  public String instanceId() {
    return _metadataConfig.participantId();
  }

  public synchronized void start() throws Exception {
    checkState(!_started && !_closed, "Native minion is already started or closed");
    _started = true;
    try {
      Files.createDirectories(_dataDirectory);
      _lockFile = FileChannel.open(_dataDirectory.resolve(".native-minion.lock"), StandardOpenOption.CREATE,
          StandardOpenOption.WRITE);
      _directoryLock = _lockFile.tryLock();
      checkState(_directoryLock != null, "Native minion directory is already owned");
      _generator = new NativeSegmentGenerationTask(_sourceRoot, _outputRoot, _maxSourceBytes, _maxRows);
      if (_membership == null) {
        _membership = new Connection(_metadataConfig);
      }
      if (_work == null) {
        _work = new Connection(_metadataConfig);
      }
      MetadataRpc.MembershipResponse current = _membership.membershipRead(MetadataRpc.MembershipReadRequest
          .newBuilder().setHistoryId(_revision.getHistoryId()).build(), timeout());
      checkState(current.getStatus() == ResultStatus.OK || current.getStatus() == ResultStatus.NOT_FOUND,
          "Cannot inspect native minion registration: %s", current.getStatus());
      long previousEpoch = current.hasParticipant()
          ? current.getParticipant().getParticipant().getRegistrationEpoch() : 0;
      MetadataRpc.MembershipResponse registered = _membership.membershipExecute(command()
          .setRegister(MetadataRpc.ParticipantRegistration.newBuilder().setIncarnation(_incarnation.toString())
              .setExpectedEpoch(previousEpoch)).build(), timeout());
      checkState(registered.getStatus() == ResultStatus.OK && registered.hasParticipant(),
          "Cannot register native minion: %s", registered.getStatus());
      _participant = registered.getParticipant().getParticipant();
      checkState(_participant.getHistoryId().equals(_revision.getHistoryId())
          && _participant.getParticipantId().equals(instanceId())
          && _participant.getIncarnation().equals(_incarnation.toString())
          && _participant.getRole() == MetadataRpc.ParticipantRole.MINION, "Native minion registration differs");
      snapshot();
      MetadataRpc.MembershipResponse ready = _membership.membershipExecute(command().setReady(
          MetadataRpc.ParticipantReadiness.newBuilder().setSession(session()).setAppliedRevision(_revision)
              .setReady(true)).build(), timeout());
      checkState(ready.getStatus() == ResultStatus.OK, "Native minion readiness was rejected: %s", ready.getStatus());
      snapshot();
      Path sessions = _dataDirectory.resolve("task-sessions");
      Files.createDirectories(sessions);
      try (var entries = Files.list(sessions)) {
        checkState(entries.limit(128).count() < 128, "Native minion recovery evidence requires explicit archival");
      }
      _outbox = new NativeTaskOutbox(_work, _participant, sessions.resolve(_incarnation.toString()), timeout());
      heartbeat();
      checkState(_healthy, "Native minion initial heartbeat failed: %s", _failure);
      ServiceStatus.setServiceStatusCallback(instanceId(), this);
      _statusRegistered = true;
      _heartbeats.scheduleWithFixedDelay(this::heartbeat, _metadataConfig.heartbeatMillis(),
          _metadataConfig.heartbeatMillis(), TimeUnit.MILLISECONDS);
      _worker.scheduleWithFixedDelay(this::poll, 0, _pollMillis, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      try {
        close();
      } catch (Exception cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
  }

  private long timeout() {
    return _metadataConfig.rpcTimeoutMillis();
  }

  private MetadataRpc.MembershipCommandRequest.Builder command() {
    return MetadataRpc.MembershipCommandRequest.newBuilder().setHistoryId(_metadataConfig.historyId().toString());
  }

  private MetadataRpc.ParticipantSession session() {
    checkState(_participant != null, "Native minion is unregistered");
    return MetadataRpc.ParticipantSession.newBuilder().setIncarnation(_participant.getIncarnation())
        .setRegistrationEpoch(_participant.getRegistrationEpoch()).build();
  }

  private MetadataRpc.TaskWorkResult snapshot() {
    MetadataRpc.TaskWorkResult image = _work.taskWork(MetadataRpc.TaskWorkRequest.newBuilder()
        .setHistoryId(_metadataConfig.historyId().toString()).setSession(session()).setMaxRecords(128)
        .setMaxBytes(2 * 1024 * 1024).build(), timeout());
    if (image.getStatus() == ResultStatus.FENCED || image.getStatus() == ResultStatus.HISTORY_MISMATCH) {
      _fenced = true;
    }
    checkState(image.getStatus() == ResultStatus.OK && image.hasParticipant() && image.hasControl()
        && image.getParticipant().getParticipant().equals(_participant)
        && image.getRevision().getHistoryId().equals(_participant.getHistoryId())
        && image.getRevision().getIndex() >= _revision.getIndex() && image.getRecordsCount() <= 128
        && image.getSerializedSize() <= 2 * 1024 * 1024, "Native minion work snapshot is unavailable or inconsistent");
    checkState(!image.getParticipant().getRetired() && !image.getParticipant().getDraining()
        && image.getParticipant().getExpiryRound() > image.getControl().getRound()
        && image.getParticipant().getCapabilitiesList().equals(List.of(MetadataRpc.ServingCapability.MINION_TASK)),
        "Native minion task capability or live registration is unavailable");
    _revision = image.getRevision();
    _hydrated = true;
    return image;
  }

  private void heartbeat() {
    if (_closed || _fenced) {
      return;
    }
    try {
      if (_pendingRenewal == null) {
        _pendingRenewal = command().setRenew(MetadataRpc.ParticipantRenewal.newBuilder().setSession(session())
            .setSequence(_renewal).setAppliedRevision(_revision)).build();
      }
      MetadataRpc.MembershipResponse response = _membership.membershipExecute(_pendingRenewal, timeout());
      if (response.getStatus() == ResultStatus.FENCED || response.getStatus() == ResultStatus.HISTORY_MISMATCH) {
        _fenced = true;
      }
      checkState(response.getStatus() == ResultStatus.OK && response.getRenewalsCount() == 1,
          "Native minion renewal rejected: %s", response.getStatus());
      MetadataRpc.RenewalResult renewal = response.getRenewals(0);
      if (renewal.getStatus() == ResultStatus.FENCED) {
        _fenced = true;
      }
      checkState(renewal.getParticipant().equals(_participant) && renewal.getSequence() == _renewal,
          "Native minion renewal identity differs");
      checkState((renewal.getStatus() == ResultStatus.OK || renewal.getStatus() == ResultStatus.NOT_READY)
          && _renewal < Long.MAX_VALUE, "Native minion renewal has an unusable durable outcome");
      // NOT_READY is a committed consumed sequence too; retrying it can never become a lease grant.
      _renewal++;
      _pendingRenewal = null;
      _healthy = renewal.getStatus() == ResultStatus.OK;
      if (!_healthy) {
        _failure = "Native minion heartbeat awaits a current metadata image";
      } else if (_hydrated) {
        _failure = null;
      }
    } catch (Exception e) {
      _healthy = false;
      _failure = "Native minion heartbeat is unavailable";
      LOGGER.debug("Native minion heartbeat failed", e);
    }
  }

  private void poll() {
    if (_closed || _fenced) {
      return;
    }
    try {
      // Hydrate even when renewal is not ready, so advancing applied revision can recover a lagging heartbeat.
      MetadataRpc.TaskWorkResult image = snapshot();
      if (!_healthy) {
        return;
      }
      if (_outbox.pending()) {
        MetadataRpc.CommandResult result = _outbox.resolve();
        if (result.getStatus() != ResultStatus.OK) {
          _failure = "Native task transition rejected: " + result.getStatus();
        }
        return;
      }
      Map<String, MetadataRpc.CatalogRecord> records = new HashMap<>();
      Map<String, Long> running = new HashMap<>();
      for (MetadataRpc.CatalogRecord record : image.getRecordsList()) {
        checkState(records.putIfAbsent(record.getObjectId() + ":" + record.getVersion(), record) == null,
            "Duplicate task projection record");
        if (record.getKind() == MetadataRpc.RecordKind.TASK
            && record.getValue().getTask().getState() == MetadataRpc.TaskExecutionState.State.RUNNING) {
          running.put(record.getValue().getTask().getTaskId(), record.getValue().getTask().getAttempt());
        }
      }
      _executedAttempts.entrySet().removeIf(entry -> !entry.getValue().equals(running.get(entry.getKey())));
      for (MetadataRpc.CatalogRecord record : image.getRecordsList()) {
        if (record.getKind() != MetadataRpc.RecordKind.TASK || record.getDeleted()) {
          continue;
        }
        MetadataRpc.TaskExecutionState task = record.getValue().getTask();
        if (!task.getInput().getMinionId().equals(instanceId())) {
          throw new IllegalStateException("Task belongs to another minion");
        }
        if (task.getState() == MetadataRpc.TaskExecutionState.State.RUNNING && ownAttempt(image, task)
            && !_executedAttempts.containsKey(task.getTaskId())) {
          execute(task, record.getVersion(), records);
          _executedAttempts.put(task.getTaskId(), task.getAttempt());
          return;
        }
      }
      // A READY task must not starve a later RUNNING attempt by repeatedly failing the one-active-task check.
      if (!running.isEmpty()) {
        return;
      }
      for (MetadataRpc.CatalogRecord record : image.getRecordsList()) {
        if (record.getKind() == MetadataRpc.RecordKind.TASK && !record.getDeleted()) {
          MetadataRpc.TaskExecutionState task = record.getValue().getTask();
          if (task.getState() == MetadataRpc.TaskExecutionState.State.READY && !queuePaused(image, task)) {
            _outbox.enqueue(MetadataRpc.SequencedTaskOperation.newBuilder().setClaim(MetadataRpc.TaskClaim.newBuilder()
                .setTaskId(task.getTaskId()).setExpectedVersion(record.getVersion())));
            return;
          }
        }
      }
    } catch (Exception e) {
      _hydrated = false;
      _failure = "Native minion work is unavailable";
      LOGGER.debug("Native minion task poll failed", e);
    }
  }

  private static boolean queuePaused(MetadataRpc.TaskWorkResult image, MetadataRpc.TaskExecutionState task) {
    return image.getRecordsList().stream().filter(record -> record.getKind() == MetadataRpc.RecordKind.TASK_QUEUE)
        .map(record -> record.getValue().getTaskQueue())
        .filter(queue -> queue.getQueue().equals(task.getInput().getQueue()))
        .findFirst().map(MetadataRpc.TaskQueueState::getPaused).orElse(true);
  }

  private boolean ownAttempt(MetadataRpc.TaskWorkResult image, MetadataRpc.TaskExecutionState task) {
    return image.getRecordsList().stream().filter(record -> record.getKind() == MetadataRpc.RecordKind.TASK_ATTEMPT)
        .map(record -> record.getValue().getTaskAttempt())
        .anyMatch(attempt -> attempt.getTaskId().equals(task.getTaskId()) && attempt.getAttempt() == task.getAttempt()
            && attempt.getState() == MetadataRpc.TaskAttemptState.State.RUNNING
            && attempt.getParticipantAtClaim().getParticipant().equals(_participant));
  }

  private void execute(MetadataRpc.TaskExecutionState task, long version,
      Map<String, MetadataRpc.CatalogRecord> records) throws Exception {
    try {
      TableConfig table = null;
      Schema schema = null;
      for (MetadataRpc.ObjectVersion dependency : task.getDependenciesList()) {
        MetadataRpc.CatalogRecord record = records.get(dependency.getObjectId() + ":" + dependency.getVersion());
        checkState(record != null && dependency.getHistoryId().equals(_participant.getHistoryId()),
            "Captured task dependency is absent");
        if (record.getKind() == MetadataRpc.RecordKind.TABLE_CONFIG) {
          checkState(table == null && record.getObjectId().equals(task.getInput().getQueue().getTableObjectId()),
              "Captured task table differs");
          table = JsonUtils.bytesToObject(record.getValue().getTableConfigJson().toByteArray(), TableConfig.class);
        } else if (record.getKind() == MetadataRpc.RecordKind.SCHEMA) {
          checkState(schema == null, "Captured task schema is ambiguous");
          schema = Schema.fromString(record.getValue().getSchemaJson().toStringUtf8());
        }
      }
      checkState(table != null && schema != null, "Captured task table and schema are required");
      MetadataRpc.OfflineSegmentContent output = _generator.generate(_participant, task, table, schema,
          _dataDirectory.resolve("working"), this::checkRunning);
      checkRunning();
      _outbox.enqueue(MetadataRpc.SequencedTaskOperation.newBuilder().setPublish(
          MetadataRpc.TaskPublishOfflineOutputs.newBuilder().setTaskId(task.getTaskId()).setExpectedVersion(version)
              .setAttempt(task.getAttempt()).addOutputs(output).setProgress(MetadataRpc.TaskProgress.newBuilder()
                  .setCompletedUnits(1).setDetail("Generated immutable segment"))));
    } catch (CancellationException e) {
      if (_closed || _fenced || Thread.currentThread().isInterrupted()) {
        throw e;
      }
      // Heartbeat uncertainty stops local work. Resolve this attempt after recovery instead of regenerating it.
      _outbox.enqueue(MetadataRpc.SequencedTaskOperation.newBuilder().setComplete(MetadataRpc.TaskComplete.newBuilder()
          .setTaskId(task.getTaskId()).setExpectedVersion(version).setAttempt(task.getAttempt())
          .setResult(MetadataRpc.TaskAttemptState.State.RETRYABLE_FAILED)
          .setProgress(MetadataRpc.TaskProgress.newBuilder()
              .setDetail("Native segment generation stopped during heartbeat uncertainty"))));
    } catch (Exception e) {
      checkRunning();
      _outbox.enqueue(MetadataRpc.SequencedTaskOperation.newBuilder().setComplete(MetadataRpc.TaskComplete.newBuilder()
          .setTaskId(task.getTaskId()).setExpectedVersion(version).setAttempt(task.getAttempt())
          .setResult(MetadataRpc.TaskAttemptState.State.FATAL_FAILED).setProgress(MetadataRpc.TaskProgress.newBuilder()
              .setDetail("Native segment generation failed"))));
      LOGGER.warn("Native task generation failed for {}", task.getTaskId(), e);
    }
  }

  private void checkRunning() {
    if (_closed || _fenced || !_healthy || Thread.currentThread().isInterrupted()) {
      throw new CancellationException("Native minion execution was cancelled or its live session is uncertain");
    }
  }

  @Override
  public ServiceStatus.Status getServiceStatus() {
    return _closed ? ServiceStatus.Status.NOT_STARTED
        : _healthy && _hydrated && !_fenced ? ServiceStatus.Status.GOOD : ServiceStatus.Status.BAD;
  }

  @Override
  public String getStatusDescription() {
    return _failure == null ? "Native task worker ready; legacy plugins and query listeners disabled" : _failure;
  }

  @Override
  public synchronized void close() throws Exception {
    if (_closed) {
      return;
    }
    _closed = true;
    _healthy = false;
    _worker.shutdownNow();
    _heartbeats.shutdownNow();
    if (_statusRegistered) {
      ServiceStatus.removeServiceStatusCallback(instanceId(), this);
      _statusRegistered = false;
    }
    if (_participant != null && _membership != null) {
      try {
        _membership.membershipExecute(command().setUnregister(session()).build(), timeout());
      } catch (Exception e) {
        LOGGER.debug("Native minion retirement could not be confirmed; lease expiry fences the session", e);
      }
    }
    if (_work != null) {
      _work.close();
    }
    if (_membership != null) {
      _membership.close();
    }
    // Keep ownership if an uninterruptible plugin/IO action is still running; a replacement must not overlap it.
    checkState(_worker.awaitTermination(30, TimeUnit.SECONDS), "Native minion worker is still stopping; lock retained");
    checkState(_heartbeats.awaitTermination(5, TimeUnit.SECONDS), "Native minion heartbeat is still stopping");
    if (_directoryLock != null) {
      _directoryLock.release();
    }
    if (_lockFile != null) {
      _lockFile.close();
    }
  }
}
