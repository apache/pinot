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
package org.apache.pinot.server.starter;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.apache.pinot.common.coordination.MetadataRpcClient;
import org.apache.pinot.common.coordination.NativeOfflineSegmentWorker;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.CommandResult;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.InstallationCommandRequest;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.InstallationOutcomeRequest;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.InstallationReport;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;

import static java.util.Objects.requireNonNull;


/// One durable, bounded report stream for an exact SERVER registration. Unknown registration, report and ack
/// responses always reuse their persisted identity. A result is durable before acknowledgement; a new sequence is
/// allocated only after a known successful acknowledgement. Old-session files remain recovery evidence only;
/// reaching the bounded evidence capacity requires explicit archival before another session can be opened.
public final class InstallationOutbox {
  private static final String STREAM = "installations";
  private static final int FORMAT = 1;
  private static final int MAX_TOKEN_BYTES = 4096;
  private static final int MAX_REPORT_BYTES = 60 * 1024;
  private static final int MAX_RESULT_BYTES = 64 * 1024;
  private static final int MAX_STATE_BYTES = 256 * 1024;
  private static final int MAX_EVIDENCE_FILES = 128;
  private final NativeInstanceDataManager _manager;
  private final ParticipantToken _server;
  private final Rpc _rpc;
  private final long _timeoutMillis;
  private final Path _file;
  private final ReentrantLock _busy = new ReentrantLock();
  private State _state;
  private boolean _retired;
  @Nullable
  private IOException _storageFailure;

  /// Transport implementations must bound each call to timeoutMillis and preserve the exact supplied payload.
  public interface Rpc {
    CommandResult execute(InstallationCommandRequest request, long timeoutMillis) throws Exception;

    CommandResult outcome(InstallationOutcomeRequest request, long timeoutMillis) throws Exception;
  }

  private enum Phase {
    NEW, READY, PENDING, RESOLVED, COMPLETE, FAILED
  }

  private record State(UUID clientIncarnation, long epoch, long sequence, Phase phase,
                       @Nullable InstallationReport report, @Nullable CommandResult result, ResultStatus rejection) {
  }

  InstallationOutbox(NativeInstanceDataManager manager, ParticipantToken server, Rpc rpc, long timeoutMillis)
      throws IOException {
    _manager = requireNonNull(manager, "manager");
    _server = requireNonNull(server, "server");
    _rpc = requireNonNull(rpc, "rpc");
    require(timeoutMillis > 0 && timeoutMillis <= 30_000, "Outbox timeout must be between 1 and 30000 milliseconds");
    _timeoutMillis = timeoutMillis;
    validateServer(server);
    require(server.getParticipantId().equals(manager.getInstanceId()), "Outbox belongs to another server");
    try (NativeInstanceDataManager.DirectoryLease lease = manager.acquireDirectoryOwnership()) {
      Path directory = lease.directory().resolve(".installation-outbox");
      if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
        Files.createDirectory(directory);
      }
      require(Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS), "Outbox directory must not be a symbolic link");
      int evidenceFiles = 0;
      try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
        for (Path entry : entries) {
          require(++evidenceFiles <= MAX_EVIDENCE_FILES && Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)
              && Files.size(entry) <= MAX_STATE_BYTES, "Outbox evidence exceeds storage bounds or has an invalid type");
        }
      }
      // The native instance may have just created the data directory and its ancestors, before any segment install.
      for (Path current = directory; current != null; current = current.getParent()) {
        forceDirectory(current);
      }
      _file = directory.resolve(HexFormat.of().formatHex(digest(server.toByteArray())) + ".state");
      if (Files.exists(_file, LinkOption.NOFOLLOW_LINKS)) {
        _state = read();
      } else {
        require(evidenceFiles < MAX_EVIDENCE_FILES,
            "Outbox evidence requires explicit archival before another session");
        persist(new State(UUID.randomUUID(), 0, 1, Phase.NEW, null, null, ResultStatus.RESULT_STATUS_UNSPECIFIED));
      }
    }
  }

  ParticipantToken server() {
    return _server;
  }

  void retire() throws IOException {
    require(_busy.tryLock(), "An installation outbox call is still active");
    try {
      _retired = true;
    } finally {
      _busy.unlock();
    }
  }

  /// Never queues a competing caller. An unresolved different report must first be recovered explicitly.
  public CommandResult publish(InstallationReport report) throws Exception {
    validateReport(report);
    require(_busy.tryLock(), "Installation outbox is busy");
    try (NativeInstanceDataManager.DirectoryLease ignored = _manager.acquireDirectoryOwnership()) {
      usable();
      long deadline = deadline();
      register(deadline);
      if (_state.report() != null) {
        if (_state.report().equals(report)) {
          return finish(deadline, true);
        }
        require(!sameOperation(_state.report(), report),
            "Installation evidence changed for the same operation attempt");
        require(_state.phase() == Phase.COMPLETE, "Another installation report has an unresolved outcome");
      }
      require(_state.phase() == Phase.READY || _state.phase() == Phase.COMPLETE, "Outbox cannot accept a report");
      require(_state.phase() != Phase.COMPLETE || _state.sequence() < Long.MAX_VALUE, "Report sequence exhausted");
      long sequence = _state.phase() == Phase.COMPLETE ? _state.sequence() + 1 : _state.sequence();
      State pending = new State(_state.clientIncarnation(), _state.epoch(), sequence, Phase.PENDING, report, null,
          ResultStatus.RESULT_STATUS_UNSPECIFIED);
      require(reportRequest(pending).getSerializedSize() <= MetadataRpcClient.MAX_INSTALLATION_REQUEST_BYTES,
          "Installation request exceeds transport bounds");
      persist(pending);
      return finish(deadline, false);
    } finally {
      _busy.unlock();
    }
  }

  /// Resolves the persisted report or acknowledgement after an unknown outcome. Returns null before any report.
  @Nullable
  public CommandResult recover() throws Exception {
    require(_busy.tryLock(), "Installation outbox is busy");
    try (NativeInstanceDataManager.DirectoryLease ignored = _manager.acquireDirectoryOwnership()) {
      usable();
      long deadline = deadline();
      register(deadline);
      return _state.report() == null ? null : finish(deadline, true);
    } finally {
      _busy.unlock();
    }
  }

  private void usable() throws IOException {
    require(!_retired, "Installation outbox belongs to a retired SERVER session");
    if (_storageFailure != null) {
      throw new IOException("Outbox storage outcome is uncertain; reopen under exclusive ownership", _storageFailure);
    }
    require(_state.phase() != Phase.FAILED, "Installation outbox is fenced by result: "
        + _state.rejection());
  }

  private void register(long deadline) throws Exception {
    if (_state.phase() != Phase.NEW) {
      return;
    }
    CommandResult result = _rpc.execute(base().setRegister(MetadataRpc.RegisterClient.newBuilder()
        .setIncarnation(_state.clientIncarnation().toString()).setExpectedEpoch(0)).build(), remaining(deadline));
    validateResult(result);
    if (result.getStatus() != ResultStatus.OK) {
      fail(result);
    }
    require(result.hasClientEpoch() && result.getClientEpoch() == 1, "Unexpected installation client epoch");
    persist(new State(_state.clientIncarnation(), 1, 1, Phase.READY, null, null,
        ResultStatus.RESULT_STATUS_UNSPECIFIED));
  }

  private CommandResult finish(long deadline, boolean resolve) throws Exception {
    if (_state.phase() == Phase.PENDING) {
      CommandResult result = null;
      boolean fromOutcome = false;
      if (resolve) {
        result = _rpc.outcome(outcomeRequest(), remaining(deadline));
        validateResult(result);
        if (result.getStatus() == ResultStatus.NOT_FOUND) {
          result = null;
        } else {
          fromOutcome = true;
        }
      }
      if (result == null) {
        result = _rpc.execute(reportRequest(_state), remaining(deadline));
        validateResult(result);
      }
      if (result.getStatus() != ResultStatus.OK) {
        if (!operationRejection(result.getStatus())) {
          fail(result);
        }
        // Session/client/digest rejection can precede stream execution. Only a retained per-operation outcome
        // returns the identical result, including its original committed revision, from both exact command
        // replay and outcome lookup. A fresh gate rejection has a new command/read-barrier revision. Never ack
        // or advance an ambiguous rejection, but a proven stale operation must not block unrelated future work.
        CommandResult confirmation = fromOutcome
            ? _rpc.execute(reportRequest(_state), remaining(deadline))
            : _rpc.outcome(outcomeRequest(), remaining(deadline));
        validateResult(confirmation);
        if (!result.equals(confirmation)) {
          fail(result);
        }
      }
      persist(new State(_state.clientIncarnation(), _state.epoch(), _state.sequence(), Phase.RESOLVED,
          _state.report(), result, ResultStatus.RESULT_STATUS_UNSPECIFIED));
    }
    if (_state.phase() == Phase.RESOLVED) {
      // Repeating the same acknowledgement is idempotent, including after its original response was lost.
      CommandResult ack = _rpc.execute(base().setAcknowledge(MetadataRpc.Acknowledge.newBuilder()
          .setClientEpoch(_state.epoch()).setStreamId(STREAM).setThroughSequence(_state.sequence())).build(),
          remaining(deadline));
      validateResult(ack);
      if (ack.getStatus() != ResultStatus.OK) {
        fail(ack);
      }
      persist(new State(_state.clientIncarnation(), _state.epoch(), _state.sequence(), Phase.COMPLETE,
          _state.report(), _state.result(), ResultStatus.RESULT_STATUS_UNSPECIFIED));
    }
    require(_state.phase() == Phase.COMPLETE, "Installation outcome is unresolved");
    return _state.result();
  }

  private void fail(CommandResult result) throws IOException {
    persist(new State(_state.clientIncarnation(), _state.epoch(), _state.sequence(), Phase.FAILED,
        _state.report(), _state.result() == null ? result : _state.result(), result.getStatus()));
    throw new IOException("Installation outbox rejected by coordination: " + result.getStatus());
  }

  private InstallationCommandRequest.Builder base() {
    return InstallationCommandRequest.newBuilder().setHistoryId(_server.getHistoryId()).setSession(session());
  }

  private MetadataRpc.ParticipantSession session() {
    return MetadataRpc.ParticipantSession.newBuilder().setIncarnation(_server.getIncarnation())
        .setRegistrationEpoch(_server.getRegistrationEpoch()).build();
  }

  private InstallationCommandRequest reportRequest(State state) {
    return base().setReport(MetadataRpc.SequencedInstallationReport.newBuilder().setClientEpoch(state.epoch())
        .setStreamId(STREAM).setSequence(state.sequence()).setReport(state.report())).build();
  }

  private InstallationOutcomeRequest outcomeRequest() {
    return InstallationOutcomeRequest.newBuilder().setHistoryId(_server.getHistoryId()).setSession(session())
        .setOutcome(MetadataRpc.OutcomeQuery.newBuilder().setClientEpoch(_state.epoch()).setStreamId(STREAM)
            .setSequence(_state.sequence())).build();
  }

  private static boolean operationRejection(ResultStatus status) {
    return status == ResultStatus.FENCED || status == ResultStatus.CONFLICT || status == ResultStatus.NOT_FOUND
        || status == ResultStatus.NOT_READY;
  }

  private long deadline() {
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(_timeoutMillis);
  }

  private static long remaining(long deadline) throws IOException {
    long nanos = deadline - System.nanoTime();
    require(nanos > 0, "Installation outbox deadline expired");
    return Math.max(1, TimeUnit.NANOSECONDS.toMillis(nanos));
  }

  private void validateResult(CommandResult result) throws IOException {
    require(result != null && result.getSerializedSize() <= MAX_RESULT_BYTES, "Missing or oversized outbox result");
    NativeOfflineSegmentWorker.validateMessage(result);
    require(result.getStatus() != ResultStatus.RESULT_STATUS_UNSPECIFIED
        && result.getStatus() != ResultStatus.UNRECOGNIZED && result.hasRevision()
        && _server.getHistoryId().equals(result.getRevision().getHistoryId()) && result.getRevision().getIndex() >= 0,
        "Invalid outbox result revision or status");
  }

  static void validateServer(ParticipantToken server) throws IOException {
    require(server.getSerializedSize() <= MAX_TOKEN_BYTES, "Oversized SERVER token");
    NativeOfflineSegmentWorker.validateMessage(server);
    canonicalUuid(server.getHistoryId());
    canonicalUuid(server.getIncarnation());
    require(server.getRole() == MetadataRpc.ParticipantRole.SERVER && server.getRegistrationEpoch() > 0
        && !server.getParticipantId().isBlank() && server.getParticipantId().length() <= 256, "Invalid SERVER token");
  }

  private static void validateReport(InstallationReport report) throws IOException {
    require(report != null && report.getSerializedSize() <= MAX_REPORT_BYTES,
        "Missing or oversized installation report");
    NativeOfflineSegmentWorker.validateMessage(report);
    canonicalUuid(report.getOperationId());
    canonicalUuid(report.getInstallationId());
    require(report.getAttempt() > 0 && report.hasOwner() && report.hasInstalled()
        && report.getManifestSha256().matches("[0-9a-f]{64}"), "Incomplete installation report");
  }

  private static void canonicalUuid(String value) throws IOException {
    try {
      require(UUID.fromString(value).toString().equals(value), "Noncanonical UUID");
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid UUID", e);
    }
  }

  private static boolean sameOperation(InstallationReport first, InstallationReport second) {
    return first.getOperationId().equals(second.getOperationId()) && first.getAttempt() == second.getAttempt();
  }

  private void persist(State next) throws IOException {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (DataOutputStream out = new DataOutputStream(bytes)) {
        out.writeInt(FORMAT);
        write(out, _server.toByteArray());
        out.writeLong(next.clientIncarnation().getMostSignificantBits());
        out.writeLong(next.clientIncarnation().getLeastSignificantBits());
        out.writeLong(next.epoch());
        out.writeLong(next.sequence());
        out.writeInt(next.phase().ordinal());
        out.writeInt(next.rejection().getNumber());
        write(out, next.report() == null ? new byte[0] : next.report().toByteArray());
        write(out, next.result() == null ? new byte[0] : next.result().toByteArray());
      }
      byte[] payload = bytes.toByteArray();
      require(payload.length + 32 <= MAX_STATE_BYTES, "Outbox state exceeds byte limit");
      bytes.write(digest(payload));
      atomicWrite(bytes.toByteArray());
      _state = next;
    } catch (IOException e) {
      _storageFailure = e;
      throw e;
    }
  }

  private State read() throws IOException {
    require(Files.isRegularFile(_file, LinkOption.NOFOLLOW_LINKS), "Outbox state is not a regular file");
    byte[] bytes;
    try (FileChannel channel = FileChannel.open(_file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      long size = channel.size();
      require(size >= 36 && size <= MAX_STATE_BYTES, "Invalid outbox state size");
      ByteBuffer buffer = ByteBuffer.allocate((int) size);
      while (buffer.hasRemaining()) {
        require(channel.read(buffer) >= 0, "Truncated outbox state");
      }
      require(channel.size() == size, "Outbox state changed while reading");
      bytes = buffer.array();
    }
    byte[] payload = Arrays.copyOf(bytes, bytes.length - 32);
    require(MessageDigest.isEqual(digest(payload), Arrays.copyOfRange(bytes, payload.length, bytes.length)),
        "Outbox state checksum differs");
    try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
      require(input.readInt() == FORMAT, "Unsupported outbox state format");
      ParticipantToken server = parse(ParticipantToken.parser(), read(input, MAX_TOKEN_BYTES));
      require(server.equals(_server), "Outbox state belongs to another SERVER session");
      UUID incarnation = new UUID(input.readLong(), input.readLong());
      long epoch = input.readLong();
      long sequence = input.readLong();
      int ordinal = input.readInt();
      require(ordinal >= 0 && ordinal < Phase.values().length && epoch >= 0 && epoch <= 1 && sequence > 0,
          "Invalid outbox state counters");
      Phase phase = Phase.values()[ordinal];
      ResultStatus rejection = ResultStatus.forNumber(input.readInt());
      require(rejection != null, "Unknown persisted rejection status");
      byte[] reportBytes = read(input, MAX_REPORT_BYTES);
      byte[] resultBytes = read(input, MAX_RESULT_BYTES);
      require(input.available() == 0, "Trailing outbox state bytes");
      InstallationReport report = reportBytes.length == 0 ? null : parse(InstallationReport.parser(), reportBytes);
      CommandResult result = resultBytes.length == 0 ? null : parse(CommandResult.parser(), resultBytes);
      if (report != null) {
        validateReport(report);
      }
      if (result != null) {
        validateResult(result);
      }
      boolean valid = switch (phase) {
        case NEW -> epoch == 0 && sequence == 1 && report == null && result == null;
        case READY -> epoch == 1 && sequence == 1 && report == null && result == null;
        case PENDING -> epoch == 1 && report != null && result == null;
        case RESOLVED, COMPLETE -> epoch == 1 && report != null && result != null
            && (result.getStatus() == ResultStatus.OK || operationRejection(result.getStatus()));
        case FAILED -> result != null && (epoch == 0 ? sequence == 1 && report == null : report != null)
            && rejection != ResultStatus.OK
            && rejection != ResultStatus.RESULT_STATUS_UNSPECIFIED;
      };
      require(valid && (phase == Phase.FAILED || rejection == ResultStatus.RESULT_STATUS_UNSPECIFIED),
          "Inconsistent outbox phase");
      return new State(incarnation, epoch, sequence, phase, report, result, rejection);
    }
  }

  private void atomicWrite(byte[] bytes) throws IOException {
    require(Files.isDirectory(_file.getParent(), LinkOption.NOFOLLOW_LINKS)
        && _file.getParent().toRealPath().equals(_file.getParent()), "Outbox directory path changed");
    require(!Files.exists(_file, LinkOption.NOFOLLOW_LINKS)
        || Files.isRegularFile(_file, LinkOption.NOFOLLOW_LINKS), "Outbox state is not a regular file");
    Path temporary = _file.resolveSibling(_file.getFileName() + "." + UUID.randomUUID() + ".tmp");
    try {
      try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
        channel.force(true);
      }
      Files.move(temporary, _file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      forceDirectory(_file.getParent());
    } finally {
      Files.deleteIfExists(temporary);
    }
  }

  private static void forceDirectory(Path path) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
      channel.force(true);
    }
  }

  private static byte[] digest(byte[] bytes) {
    try {
      return MessageDigest.getInstance("SHA-256").digest(bytes);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void write(DataOutputStream out, byte[] bytes) throws IOException {
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static byte[] read(DataInputStream input, int maximum) throws IOException {
    int size = input.readInt();
    require(size >= 0 && size <= maximum && size <= input.available(), "Invalid outbox field length");
    return input.readNBytes(size);
  }

  private static <T extends Message> T parse(Parser<T> parser, byte[] bytes) throws IOException {
    CodedInputStream input = CodedInputStream.newInstance(bytes);
    input.setRecursionLimit(32);
    T message = parser.parseFrom(input);
    require(input.isAtEnd(), "Trailing protocol bytes");
    NativeOfflineSegmentWorker.validateMessage(message);
    return message;
  }

  private static void require(boolean condition, String message) throws IOException {
    if (!condition) {
      throw new IOException(message);
    }
  }
}
