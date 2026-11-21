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

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Acknowledge;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto;
import org.apache.ratis.proto.RaftProtos.ReadIndexRequestProto;
import org.apache.ratis.proto.RaftProtos.RequestVoteRequestProto;
import org.apache.ratis.proto.RaftProtos.StartLeaderElectionRequestProto;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.util.TimeDuration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// A reachable former leader must not serve an old image after the other two real voters commit newer metadata.
/// Peer RPC interception partitions existing streams as well as new calls; client traffic remains independently live.
public class RatisReadSafetyTest {
  @Test(timeOut = 90_000)
  public void testFreshClientCannotReadStaleIsolatedLeader()
      throws Exception {
    try (PartitionedCluster cluster = new PartitionedCluster()) {
      waitUntil(() -> cluster._stores[0].division().getInfo().isLeaderReady());
      UUID objectId = UUID.randomUUID();
      byte[] query = MetadataProtocol.encode(new GetRecord(cluster._history, RecordKind.CLUSTER_CONFIG,
          MetadataProtocol.CLUSTER_CONFIG_NAME));
      try (RatisMetadataStore.Client client = cluster.client(0, false)) {
        assertOk(client.execute(MetadataProtocol.encode(
            new RegisterClient(cluster._history, "writer", UUID.randomUUID(), 0))));
        assertOk(client.execute(MetadataProtocol.encode(new Transaction(cluster._history, "writer", 1, "main", 1,
            List.of(Mutation.createClusterConfig(cluster._history, objectId, Map.of("value", "before")))))));
        // Ratis appends a metadata log entry after committing application commands. Wait for that entry too.
        waitUntil(() -> cluster._stores[0].division().getRaftLog().getLastCommittedIndex()
            == cluster._stores[0].division().getRaftLog().getNextIndex() - 1);
        // Prime the old leader's previously acknowledged read index, not the later fresh client's history.
        assertEquals(value(client.query(query)), "before");
        assertEquals(value(client.query(query)), "before");
      }
      long originalTerm = cluster._stores[0].division().getInfo().getCurrentTerm();
      cluster._partition.set(true);
      waitUntil(() -> cluster.majorityLeader(originalTerm) >= 0);
      int newLeader = cluster.majorityLeader(originalTerm);
      try (RatisMetadataStore.Client client = cluster.client(newLeader, false)) {
        assertOk(client.execute(MetadataProtocol.encode(new Acknowledge(cluster._history, "writer", 1, "main", 1))));
        assertOk(client.execute(MetadataProtocol.encode(new Transaction(cluster._history, "writer", 1, "main", 2,
            List.of(Mutation.updateClusterConfig(new ObjectVersion(cluster._history, objectId, 1),
                Map.of("value", "after")))))));
        assertEquals(value(client.query(query)), "after");
      }
      assertTrue(cluster._stores[0].division().getInfo().isLeaderReady(),
          "The regression requires a reachable old leader that has not yet stepped down");
      assertEquals(cluster._stores[0].division().getInfo().getCurrentTerm(), originalTerm);
      // Diagnostic local state-machine access proves the partition left different images. It is not a public read.
      MetadataStateMachine oldState = (MetadataStateMachine) cluster._stores[0].division().getStateMachine();
      FeedScope scope = new FeedScope(Set.of(RecordKind.CLUSTER_CONFIG), "local-diagnostic");
      CatalogImage oldImage = oldState.captureImage(scope, 1, 4096, 0,
          System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
      assertEquals(oldImage.records().size(), 1);
      assertEquals(oldImage.records().getFirst().value().path("value").asText(), "before");
      assertTrue(cluster._blockedPeerMessages.get() > 0, "Peer messages must actually cross the partition filter");

      cluster._rejectMajorityClients.set(true);
      int before = cluster._oldLeaderClientMessages.get();
      // No prior operation can seed this client's read index. Its only known peer is the reachable old leader;
      // additionally reject client requests at the majority so a transparent redirect cannot hide a stale read.
      try (RatisMetadataStore.Client fresh = cluster.client(0, true)) {
        byte[] response = null;
        boolean unavailable = false;
        try {
          response = fresh.query(query, 2000);
        } catch (IOException fenced) {
          // Unavailability is the correct result when this endpoint cannot establish a fresh quorum read barrier.
          unavailable = true;
        }
        if (!unavailable) {
          assertEquals(value(response), "after",
              "A fresh client must never receive the old image after a new majority commits");
        }
      }
      assertTrue(cluster._oldLeaderClientMessages.get() > before,
          "The actual Client.query request must reach the isolated old leader's client endpoint");
      assertTrue(cluster._stores[0].division().getInfo().isLeaderReady(),
          "Safety must hold before the old leader's eventual quorum-loss demotion");
    }
  }

  private static void assertOk(byte[] bytes)
      throws IOException {
    Result result = MetadataProtocol.decodeResult(bytes);
    assertEquals(result.status(), Status.OK, result.detail());
  }

  private static String value(byte[] bytes)
      throws IOException {
    Result result = MetadataProtocol.decodeResult(bytes);
    assertEquals(result.status(), Status.OK, result.detail());
    return result.record().value().path("value").asText();
  }

  private static void waitUntil(Check condition)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
    while (System.nanoTime() < deadline) {
      if (condition.get()) {
        return;
      }
      Thread.sleep(20);
    }
    assertTrue(condition.get(), "Timed out waiting for the required real-quorum state");
  }

  private interface Check {
    boolean get() throws Exception;
  }

  /// Independent election timings keep voter zero locally ready while voters one and two form a new majority.
  private static final class PartitionedCluster implements AutoCloseable {
    private final Path _directory = Files.createTempDirectory("pinot-ratis-read-safety-");
    private final UUID _history = UUID.randomUUID();
    private final UUID _cluster = UUID.randomUUID();
    private final RaftPeer[] _peers = new RaftPeer[3];
    private final RatisMetadataStore[] _stores = new RatisMetadataStore[3];
    private final AtomicBoolean _partition = new AtomicBoolean();
    private final AtomicBoolean _rejectMajorityClients = new AtomicBoolean();
    private final AtomicInteger _blockedPeerMessages = new AtomicInteger();
    private final AtomicInteger _oldLeaderClientMessages = new AtomicInteger();

    private PartitionedCluster()
        throws IOException {
      try (ServerSocket first = new ServerSocket(0); ServerSocket second = new ServerSocket(0);
          ServerSocket third = new ServerSocket(0)) {
        int[] ports = {first.getLocalPort(), second.getLocalPort(), third.getLocalPort()};
        for (int i = 0; i < 3; i++) {
          _peers[i] = RaftPeer.newBuilder().setId("voter-" + i).setAddress("127.0.0.1:" + ports[i]).build();
        }
      }
      try {
        // All followers are listening before the preferred leader's first election begins.
        for (int voter : new int[]{1, 2, 0}) {
          Parameters parameters = new Parameters();
          GrpcConfigKeys.Server.setServicesCustomizer(parameters,
              (builder, types) -> builder.intercept(interceptor(voter)));
          _stores[voter] = new RatisMetadataStore(configuration(voter), properties -> {
            RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties,
                TimeDuration.valueOf(voter == 0 ? 100 : 3000, TimeUnit.MILLISECONDS));
            RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties,
                TimeDuration.valueOf(voter == 0 ? 200 : 4000, TimeUnit.MILLISECONDS));
            // Ratis schedules heartbeats from the minimum timeout, but checks quorum loss against the maximum.
            RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(400, TimeUnit.MILLISECONDS));
            RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
                TimeDuration.valueOf(voter == 0 ? 65_000 : 800, TimeUnit.MILLISECONDS));
          }, parameters);
        }
      } catch (IOException | RuntimeException | Error failure) {
        try {
          close();
        } catch (IOException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }

    private PinotConfiguration configuration(int voter) {
      String prefix = RatisConfiguration.PREFIX;
      String voters = Arrays.stream(_peers).map(peer -> peer.getId() + "@" + peer.getAddress())
          .collect(Collectors.joining(","));
      return new PinotConfiguration(Map.of(prefix + "clusterId", _cluster.toString(),
          prefix + "historyId", _history.toString(), prefix + "voters", voters, prefix + "id", "voter-" + voter,
          prefix + "dataDir", _directory.resolve("voter-" + voter).toString(), prefix + "bootstrap", true,
          prefix + "request.timeoutMs", 5000L), Map.of());
    }

    private RatisMetadataStore.Client client(int leader, boolean onlyLeader) {
      RaftProperties properties = new RaftProperties();
      RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
      RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
      RaftGroup group = RaftGroup.valueOf(RaftGroupId.valueOf(_history),
          onlyLeader ? List.of(_peers[leader]) : Arrays.asList(_peers));
      RaftClient client = RaftClient.newBuilder().setRaftGroup(group).setLeaderId(_peers[leader].getId())
          .setProperties(properties).setRetryPolicy(RetryPolicies.noRetry()).build();
      return new RatisMetadataStore.Client(client, 5000, 4);
    }

    private int majorityLeader(long oldTerm)
        throws IOException {
      for (int voter = 1; voter < 3; voter++) {
        if (_stores[voter].division().getInfo().isLeaderReady()
            && _stores[voter].division().getInfo().getCurrentTerm() > oldTerm) {
          return voter;
        }
      }
      return -1;
    }


    private ServerInterceptor interceptor(int voter) {
      return new ServerInterceptor() {
        @Override
        public <REQ, RESP> ServerCall.Listener<REQ> interceptCall(ServerCall<REQ, RESP> call, Metadata headers,
            ServerCallHandler<REQ, RESP> next) {
          return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
            private boolean _rejected;

            @Override
            public void onMessage(REQ message) {
              RaftRpcRequestProto peer = peerRequest(message);
              if (_partition.get() && peer != null
                  && ((voter == 0) != peer.getRequestorId().toStringUtf8().equals("voter-0"))) {
                _blockedPeerMessages.incrementAndGet();
                reject();
              } else if (message instanceof RaftClientRequestProto && _rejectMajorityClients.get()) {
                if (voter == 0) {
                  _oldLeaderClientMessages.incrementAndGet();
                } else {
                  reject();
                }
              }
              if (!_rejected) {
                super.onMessage(message);
              }
            }

            @Override
            public void onHalfClose() {
              if (!_rejected) {
                super.onHalfClose();
              }
            }

            private void reject() {
              if (!_rejected) {
                _rejected = true;
                call.close(org.apache.ratis.thirdparty.io.grpc.Status.UNAVAILABLE
                    .withDescription("Test network partition"), new Metadata());
              }
            }
          };
        }
      };
    }

    private static RaftRpcRequestProto peerRequest(Object message) {
      return switch (message) {
        case AppendEntriesRequestProto request -> request.getServerRequest();
        case RequestVoteRequestProto request -> request.getServerRequest();
        case InstallSnapshotRequestProto request -> request.getServerRequest();
        case ReadIndexRequestProto request -> request.getServerRequest();
        case StartLeaderElectionRequestProto request -> request.getServerRequest();
        default -> null;
      };
    }

    @Override
    public void close()
        throws IOException {
      _partition.set(false);
      _rejectMajorityClients.set(false);
      IOException failure = null;
      for (RatisMetadataStore store : _stores) {
        if (store != null) {
          try {
            store.close();
          } catch (IOException e) {
            if (failure == null) {
              failure = e;
            } else {
              failure.addSuppressed(e);
            }
          }
        }
      }
      FileUtils.deleteDirectory(_directory.toFile());
      if (failure != null) {
        throw failure;
      }
    }
  }
}
