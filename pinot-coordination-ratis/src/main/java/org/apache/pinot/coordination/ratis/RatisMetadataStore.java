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
package org.apache.pinot.coordination.ratis;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.net.HostAndPort;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.client.ServingPermitGuard.MonotonicClock;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ReadBarrier;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;


/// Embedded durable metadata voter with explicit bootstrap and recovery. This initial fixed-membership transport is
/// loopback-only. Normal Pinot starters continue to use Helix; authenticated role RPC is a separate integration.
/// Clients are thread-safe. Closing a store requires the caller to stop using it first.
public final class RatisMetadataStore implements AutoCloseable {
  private static final String IDENTITY_FILE = "pinot-ratis-identity.json";
  private static final String LOCK_FILE = "pinot-ratis.lock";
  private final RaftServer _server;
  private final RatisConfiguration _config;
  private final MetadataStateMachine _stateMachine;
  private final FileChannel _directoryLockChannel;
  private RatisMembershipClock _membershipClock;
  private boolean _closed;

  /// Explicit deployment-qualified clock bounds, with no provider or numeric defaults. `maxClockRatePpm` bounds
  /// this clock's maximum rate relative to real elapsed time, including suspension, virtualization and clock
  /// adjustments. The provider must report lost qualification; configuring these values does not qualify a clock.
  public record MembershipClockSettings(MonotonicClock clock, long maxClockRatePpm,
                                        long maxClockUncertaintyNanos) {
    public MembershipClockSettings {
      Objects.requireNonNull(clock, "clock");
      Preconditions.checkArgument(maxClockRatePpm > 0 && maxClockUncertaintyNanos >= 0,
          "Invalid membership clock qualification bounds");
    }
  }

  public RatisMetadataStore(PinotConfiguration config)
      throws IOException {
    this(config, properties -> { }, new Parameters());
  }

  @VisibleForTesting
  RatisMetadataStore(PinotConfiguration config, Consumer<RaftProperties> configure, Parameters parameters)
      throws IOException {
    _config = new RatisConfiguration(config);
    _stateMachine = new MetadataStateMachine(_config._clusterId, _config._historyId);
    String voterId = RatisConfiguration.required(config, "id");
    RaftPeer peer = _config._group.getPeer(RaftPeerId.valueOf(voterId));
    Preconditions.checkArgument(peer != null, "Voter id must appear in configured voters: %s", voterId);
    long snapshotThreshold = config.getProperty(RatisConfiguration.PREFIX + "snapshot.threshold", 10_000L);
    Preconditions.checkArgument(snapshotThreshold > 0, "Snapshot threshold must be positive");
    Path directory = Path.of(RatisConfiguration.required(config, "dataDir")).toAbsolutePath().normalize();
    boolean bootstrap = config.getProperty(RatisConfiguration.PREFIX + "bootstrap", false);
    if (!Files.exists(directory) && !bootstrap) {
      throw new IOException("Missing voter storage; first startup requires bootstrap=true: " + directory);
    }
    MetadataSnapshotStorage.createDirectoriesDurably(directory);
    _directoryLockChannel = FileChannel.open(directory.resolve(LOCK_FILE), StandardOpenOption.CREATE,
        StandardOpenOption.WRITE);
    RaftServer server = null;
    try {
      FileLock lock = _directoryLockChannel.tryLock();
      if (lock == null) {
        throw new IOException("Voter storage is already in use: " + directory);
      }
      verifyIdentity(directory, voterId, bootstrap);
      RaftProperties properties = properties();
      RaftServerConfigKeys.setStorageDir(properties, List.of(directory.toFile()));
      RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
      RaftServerConfigKeys.Log.setUseMemory(properties, false);
      RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false);
      RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false);
      RaftServerConfigKeys.Log.setPurgeGap(properties, 1);
      RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
      RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, true);
      RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
      RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, snapshotThreshold);
      RaftServerConfigKeys.Snapshot.setRetentionFileNum(properties, 2);
      RaftServerConfigKeys.Snapshot.setTriggerWhenStopEnabled(properties, false);
      RaftServerConfigKeys.Snapshot.setTriggerWhenRemoveEnabled(properties, false);
      HostAndPort address = HostAndPort.fromString(peer.getAddress());
      GrpcConfigKeys.Server.setHost(properties, address.getHost());
      GrpcConfigKeys.Server.setPort(properties, address.getPort());
      configure.accept(properties);
      server = RaftServer.newBuilder().setGroup(_config._group).setServerId(peer.getId()).setProperties(properties)
          .setParameters(parameters).setStateMachine(_stateMachine)
          .setOption(RaftStorage.StartupOption.RECOVER).build();
      server.start();
      _server = server;
    } catch (IOException | RuntimeException | Error e) {
      if (server != null) {
        try {
          server.close();
        } catch (IOException closeException) {
          e.addSuppressed(closeException);
        }
      }
      try {
        _directoryLockChannel.close();
      } catch (IOException closeException) {
        e.addSuppressed(closeException);
      }
      if (e instanceof CompletionException) {
        for (Throwable cause = e.getCause(); cause != null; cause = cause.getCause()) {
          if (cause instanceof IOException) {
            throw new IOException("Cannot initialize durable Raft metadata", e);
          }
        }
      }
      throw e;
    }
  }

  public static Client connect(PinotConfiguration config) {
    RatisConfiguration parsed = new RatisConfiguration(config);
    return connect(parsed);
  }

  private static Client connect(RatisConfiguration parsed) {
    return new Client(newRaftClient(parsed), parsed._requestTimeoutMs, 16);
  }

  private static RaftClient newRaftClient(RatisConfiguration parsed) {
    return RaftClient.newBuilder().setRaftGroup(parsed._group).setProperties(properties())
        .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(10,
            TimeDuration.valueOf(200, TimeUnit.MILLISECONDS))).build();
  }

  /// Opens an authenticated-service backend with linearizable barriers and local image capture. The caller owns
  /// the returned client and must close it before closing this voter. This method does not authorize a remote actor.
  public RatisMetadataAccess openMetadataAccess() {
    return new RatisMetadataAccess(_stateMachine, connect(_config),
        new Client(newRaftClient(_config), _config._requestTimeoutMs, 2), _config._historyId,
        _config._requestTimeoutMs);
  }

  /// Starts this voter's leader-only membership clock. Every voter must enable it before admitting participants.
  /// Policy is explicitly committed through ConfigurePolicy; this method supplies no production timing defaults.
  /// The store owns the clock and closes it before shutting down its voter.
  public synchronized void startMembershipClock(MembershipClockSettings settings)
      throws IOException {
    Objects.requireNonNull(settings, "settings");
    if (_closed) {
      throw new IOException("Metadata voter is closed");
    }
    if (_membershipClock != null) {
      return;
    }
    RaftClient client = newRaftClient(_config);
    _membershipClock = new RatisMembershipClock(new RatisMembershipClock.Backend() {
      @Override
      public RatisMembershipClock.Observation observe()
          throws IOException {
        RaftServer.Division division = division();
        return new RatisMembershipClock.Observation(division.getInfo().isLeaderReady(),
            division.getInfo().getCurrentTerm(), _server.getId().toString(), _stateMachine.membershipControl());
      }

      @Override
      public CompletableFuture<?> submit(Operation operation)
          throws IOException {
        byte[] command = MetadataProtocol.encode(new MembershipCommand(_config._historyId,
            "membership-clock:" + _server.getId(), operation));
        return client.async().send(Message.valueOf(ByteString.copyFrom(command))).thenApply(reply -> {
          try {
            if (!reply.isSuccess()) {
              throw new IOException("Membership clock proposal failed", reply.getException());
            }
            MembershipProtocol.Response response = MembershipProtocol.decodeResponse(
                reply.getMessage().getContent().toByteArray());
            if (response.status() != Status.OK) {
              throw new IOException("Membership clock proposal rejected: " + response.status());
            }
            return response;
          } catch (IOException e) {
            throw new CompletionException(e);
          }
        });
      }

      @Override
      public void close()
          throws IOException {
        client.close();
      }
    }, settings);
    _membershipClock.start();
  }

  /// Returns a terminal local clock-qualification failure, if any. A null result is not a readiness guarantee;
  /// it also applies before the clock starts, while this voter is a follower, or while a proposal is outstanding.
  @Nullable
  public synchronized String membershipClockFailure() {
    return _membershipClock == null ? null : _membershipClock.failure();
  }

  private static RaftProperties properties() {
    RaftProperties properties = new RaftProperties();
    RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
    RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(3, TimeUnit.SECONDS));
    return properties;
  }

  private void verifyIdentity(Path directory, String voterId, boolean bootstrap)
      throws IOException {
    ObjectNode identity = JsonUtils.newObjectNode().put("formatVersion", 1)
        .put("clusterId", _config._clusterId.toString()).put("historyId", _config._historyId.toString())
        .put("voterId", voterId);
    _config._group.getPeers().stream().sorted(Comparator.comparing(peer -> peer.getId().toString()))
        .forEach(peer -> identity.withArray("bootstrapVoters").add(peer.getId() + "@" + peer.getAddress()));
    Path marker = directory.resolve(IDENTITY_FILE);
    if (Files.exists(marker)) {
      if (Files.size(marker) > 65_536
          || !identity.equals(MetadataSnapshotStorage.readJson(Files.readAllBytes(marker)))) {
        throw new IOException("Voter storage identity differs from configured cluster, history, voter or peers: "
            + marker);
      }
      // Ratis RECOVER otherwise silently formats missing group storage. Never permit that after initialization.
      Path raftMetadata = directory.resolve(_config._historyId.toString()).resolve("current/raft-meta");
      if (!Files.isRegularFile(raftMetadata)) {
        throw new IOException("Previously initialized Raft storage is missing; restore it instead of resetting: "
            + raftMetadata);
      }
      return;
    }
    if (!bootstrap) {
      throw new IOException("New voter storage requires bootstrap=true: " + directory);
    }
    try (Stream<Path> entries = Files.list(directory)) {
      if (entries.anyMatch(path -> !path.getFileName().toString().equals(LOCK_FILE))) {
        throw new IOException("Bootstrap requires an empty directory: " + directory);
      }
    }
    MetadataSnapshotStorage.atomicWrite(marker, JsonUtils.objectToBytes(identity));
  }

  @VisibleForTesting
  RaftServer.Division division()
      throws IOException {
    return _server.getDivision(_config._group.getGroupId());
  }

  @Override
  public void close()
      throws IOException {
    RatisMembershipClock clock;
    synchronized (this) {
      if (_closed) {
        return;
      }
      _closed = true;
      clock = _membershipClock;
    }
    try {
      if (clock != null) {
        clock.close();
      }
    } finally {
      try {
        _server.close();
      } finally {
        _directoryLockChannel.close();
      }
    }
  }

  /// Bounded, thread-safe transport. Application request identities, not this client's transient retry cache, define
  /// retry semantics. A timeout means the outcome is unknown and the same serialized command must be resolved/retried.
  public static final class Client implements AutoCloseable {
    private final RaftClient _client;
    private final long _requestTimeoutMs;
    private final Semaphore _requests;

    @VisibleForTesting
    Client(RaftClient client, long requestTimeoutMs, int maxRequests) {
      _client = client;
      _requestTimeoutMs = requestTimeoutMs;
      _requests = new Semaphore(maxRequests);
    }

    public byte[] execute(byte[] command)
        throws IOException {
      return execute(command, _requestTimeoutMs);
    }

    public byte[] execute(byte[] command, long timeoutMillis)
        throws IOException {
      return request(command, deadline(timeoutMillis));
    }

    public byte[] query(byte[] query)
        throws IOException {
      return query(query, _requestTimeoutMs);
    }

    public byte[] query(byte[] query, long timeoutMillis)
        throws IOException {
      long deadline = deadline(timeoutMillis);
      // A new committed command establishes authority for this invocation. Ratis 3.3.1's read-only path can reuse
      // an acknowledged index on an isolated old leader after a different majority has committed newer state.
      return request(MetadataProtocol.encode(new ReadBarrier(MetadataProtocol.decodeQuery(query))), deadline);
    }

    private long deadline(long timeoutMillis)
        throws IOException {
      if (timeoutMillis <= 0) {
        throw new IOException("Metadata request deadline has expired");
      }
      return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.min(timeoutMillis, _requestTimeoutMs));
    }

    private byte[] request(byte[] bytes, long deadline)
        throws IOException {
      if (bytes == null || bytes.length == 0 || bytes.length > MetadataProtocol.MAX_COMMAND_BYTES) {
        throw new IOException("Metadata request must contain between 1 and " + MetadataProtocol.MAX_COMMAND_BYTES
            + " bytes");
      }
      try {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || !_requests.tryAcquire(remaining, TimeUnit.NANOSECONDS)) {
          throw new IOException("Timed out waiting for Raft metadata request capacity");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for Raft metadata request capacity", e);
      }
      if (deadline - System.nanoTime() <= 0) {
        _requests.release();
        throw new IOException("Timed out waiting for Raft metadata request capacity");
      }
      CompletableFuture<RaftClientReply> future;
      try {
        Message message = Message.valueOf(ByteString.copyFrom(bytes));
        future = _client.async().send(message);
      } catch (RuntimeException | Error e) {
        _requests.release();
        throw e;
      }
      // Cancellation can prevent Ratis from releasing its internal permits. Its finite retry policy retires the
      // outstanding request; retain our permit until that completion even if this caller's deadline has expired.
      future.whenComplete((reply, error) -> _requests.release());
      try {
        RaftClientReply reply = future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
        if (!reply.isSuccess()) {
          throw new IOException("Raft metadata request failed", reply.getException());
        }
        return reply.getMessage().getContent().toByteArray();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for Raft metadata", e);
      } catch (TimeoutException e) {
        throw new IOException("Timed out waiting for Raft metadata; a write may already have committed", e);
      } catch (ExecutionException e) {
        throw new IOException("Raft metadata request failed", e.getCause());
      }
    }

    @Override
    public void close()
        throws IOException {
      _client.close();
    }
  }
}
