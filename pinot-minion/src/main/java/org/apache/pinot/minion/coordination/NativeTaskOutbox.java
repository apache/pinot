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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;

import static com.google.common.base.Preconditions.checkState;


/// One serial durable request stream for an exact MINION incarnation. A pending payload and its known outcome are
/// persisted before acknowledgement; unknown replies reuse identical bytes. Old-session journals are evidence only
/// and are never replayed as a new process. Published artifacts remain independent of this journal's lifetime.
final class NativeTaskOutbox {
  private static final String STREAM = "tasks";
  private final NativeMinionRuntime.Rpc _rpc;
  private final MetadataRpc.TaskCommandRequest _envelope;
  private final Path _directory;
  private final long _timeoutMillis;
  private long _sequence = 1;
  @Nullable
  private MetadataRpc.TaskCommandRequest _pending;
  @Nullable
  private MetadataRpc.CommandResult _result;

  NativeTaskOutbox(NativeMinionRuntime.Rpc rpc, MetadataRpc.ParticipantToken token, Path directory, long timeoutMillis)
      throws Exception {
    _rpc = rpc;
    _directory = directory;
    _timeoutMillis = timeoutMillis;
    Files.createDirectory(directory);
    write(directory.resolve("session.pb"), token.toByteArray());
    _envelope = MetadataRpc.TaskCommandRequest.newBuilder().setHistoryId(token.getHistoryId())
        .setSession(MetadataRpc.ParticipantSession.newBuilder().setIncarnation(token.getIncarnation())
            .setRegistrationEpoch(token.getRegistrationEpoch())).build();
    MetadataRpc.TaskCommandRequest registration = _envelope.toBuilder().setRegister(MetadataRpc.RegisterClient
        .newBuilder().setIncarnation(token.getIncarnation())).build();
    write(directory.resolve("registration.pb"), registration.toByteArray());
    MetadataRpc.CommandResult registered = _rpc.taskExecute(registration, timeoutMillis);
    require(registered);
    checkState(registered.getClientEpoch() == 1, "Native task session client epoch differs");
  }

  boolean pending() {
    return _pending != null;
  }

  void enqueue(MetadataRpc.SequencedTaskOperation.Builder operation) throws IOException {
    checkState(_pending == null && _sequence < Long.MAX_VALUE, "Task stream has pending work or is exhausted");
    MetadataRpc.TaskCommandRequest request = _envelope.toBuilder().setOperation(operation
        .setClientEpoch(1).setStreamId(STREAM).setSequence(_sequence)).build();
    checkState(request.getSerializedSize() <= 64 * 1024, "Task command exceeds its durable bound");
    write(_directory.resolve("pending.pb"), request.toByteArray());
    _pending = request;
    _result = null;
  }

  MetadataRpc.CommandResult resolve() throws Exception {
    checkState(_pending != null, "Task stream has no pending operation");
    if (_result == null) {
      MetadataRpc.CommandResult result = _rpc.taskExecute(_pending, _timeoutMillis);
      checkState(result.hasRevision() && result.getRevision().getHistoryId().equals(_envelope.getHistoryId()),
          "Task command response history differs");
      // A retained domain rejection is acknowledged too. Session rejection cannot advance the stream because
      // acknowledgement is independently fenced by that same exact participant session.
      write(_directory.resolve("result.pb"), result.toByteArray());
      _result = result;
    }
    MetadataRpc.CommandResult acknowledgement = _rpc.taskExecute(_envelope.toBuilder()
        .setAcknowledge(MetadataRpc.Acknowledge.newBuilder().setClientEpoch(1).setStreamId(STREAM)
            .setThroughSequence(_sequence)).build(), _timeoutMillis);
    require(acknowledgement);
    MetadataRpc.CommandResult result = _result;
    // Retain the last request/result as bounded evidence; the next exact payload replaces them after this ack.
    _pending = null;
    _result = null;
    _sequence++;
    return result;
  }

  private static void require(MetadataRpc.CommandResult result) {
    checkState(result.getStatus() == ResultStatus.OK, "Task stream request rejected: %s", result.getStatus());
  }

  private static void write(Path destination, byte[] bytes) throws IOException {
    Path temporary = Files.createTempFile(destination.getParent(), ".write-", ".tmp");
    try {
      try (FileChannel output = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining()) {
          output.write(buffer);
        }
        output.force(true);
      }
      Files.move(temporary, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      try (FileChannel directory = FileChannel.open(destination.getParent(), StandardOpenOption.READ)) {
        directory.force(true);
      }
    } finally {
      Files.deleteIfExists(temporary);
    }
  }
}
