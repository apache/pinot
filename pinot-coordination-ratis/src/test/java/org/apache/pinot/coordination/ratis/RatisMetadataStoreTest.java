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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.coordination.ServingPermitGuard.ClockReading;
import org.apache.pinot.coordination.ratis.RatisMetadataStore.MembershipClockSettings;
import org.apache.pinot.coordination.ratis.client.MetadataImage;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ConfigurePolicy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.GetControl;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Register;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Response;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Tick;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Acknowledge;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipQuery;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises real disk-backed three-voter consensus, including a lagging voter that must install a snapshot because
/// its missing log prefix has been purged. A separate state-machine test covers the install/updater race directly.
public class RatisMetadataStoreTest {
  @Test(timeOut = 90_000)
  public void testMembershipClockElectionFencingAndQuorumLoss()
      throws Exception {
    try (Cluster cluster = new Cluster(10_000)) {
      cluster.start();
      for (RatisMetadataStore store : cluster.stores()) {
        // Test-only clock assumptions do not qualify a production provider.
        store.startMembershipClock(new MembershipClockSettings(
            () -> new ClockReading(System.nanoTime(), 0, true), 1_000_000, 0));
      }
      try (RatisMetadataStore.Client client = RatisMetadataStore.connect(cluster.config(0, false))) {
        waitUntil(() -> control(client, cluster._historyId).controlEpoch() != null);
        ControlState initial = control(client, cluster._historyId);
        assertEquals(membership(client, cluster._historyId, new ConfigurePolicy(initial.controlEpoch(), 0,
            new Policy(1, 100, 20, 1000, 500, 20))).status(), Status.OK);
        Response registered = membership(client, cluster._historyId,
            new Register("server", Role.SERVER, UUID.randomUUID(), 0, Map.of(), Set.of(Capability.SERVER_QUERY)));
        assertEquals(registered.status(), Status.OK);
        waitUntil(() -> control(client, cluster._historyId).round() > initial.round());
        ControlState beforeElection = control(client, cluster._historyId);
        cluster.closeVoter(cluster._stores.indexOf(cluster.leader()));
        waitUntil(() -> control(client, cluster._historyId).controlEpoch().raftTerm()
            > beforeElection.controlEpoch().raftTerm());
        ControlState afterElection = control(client, cluster._historyId);
        assertTrue(afterElection.round() >= beforeElection.round(), "Leadership never rewinds committed time");
        assertEquals(membership(client, cluster._historyId,
            new Tick(beforeElection.controlEpoch(), afterElection.round(), 1)).status(), Status.FENCED);
        waitUntil(() -> control(client, cluster._historyId).round() > afterElection.round());
        RatisMetadataStore survivor = cluster.leader();
        for (int i = 0; i < 3; i++) {
          if (cluster.store(i) != null && cluster.store(i) != survivor) {
            cluster.closeVoter(i);
          }
        }
        // Each metadata read requires its own committed barrier, even before quorum-loss demotion.
        byte[] read = MetadataProtocol.encode(new MembershipQuery(cluster._historyId, new GetControl()));
        expectThrows(IOException.class, () -> client.query(read, 500));
        MetadataStateMachine machine = (MetadataStateMachine) survivor.division().getStateMachine();
        long stoppedRound = machine.membershipControl().round();
        Thread.sleep(500);
        assertEquals(machine.membershipControl().round(), stoppedRound,
            "A local scheduler cannot advance membership without a committed majority");
        byte[] write = MetadataProtocol.encode(new MembershipCommand(cluster._historyId, "test", new Register(
            "another-server", Role.SERVER, UUID.randomUUID(), 0, Map.of(), Set.of(Capability.SERVER_QUERY))));
        expectThrows(IOException.class, () -> client.execute(write, 500));
      }
    }
  }

  private static ControlState control(RatisMetadataStore.Client client, UUID history)
      throws IOException {
    return MembershipProtocol.decodeResponse(client.query(
        MetadataProtocol.encode(new MembershipQuery(history, new GetControl())))).control();
  }

  private static Response membership(RatisMetadataStore.Client client, UUID history, Operation operation)
      throws IOException {
    return MembershipProtocol.decodeResponse(client.execute(
        MetadataProtocol.encode(new MembershipCommand(history, "test", operation))));
  }

  @Test(timeOut = 60_000)
  public void testLocalImagesUseQuorumBarrierAndCompleteChanges()
      throws Exception {
    try (Cluster cluster = new Cluster(10_000)) {
      cluster.start();
      try (RatisMetadataStore.Client writer = RatisMetadataStore.connect(cluster.config(0, false))) {
        cluster.register(writer);
        cluster.update(writer, 1, "first");
        int follower = cluster.nonLeader();
        FeedScope scope = new FeedScope(Set.of(RecordKind.CLUSTER_CONFIG), "controller");
        try (RatisMetadataAccess access = cluster.store(follower).openMetadataAccess()) {
          CatalogImage image = access.image(scope, 10, 8192, 10_000);
          assertEquals(image.records().size(), 1);
          assertEquals(image.records().get(0).value().path("value").asText(), "first");
          MetadataImage current = MetadataImage.fromSnapshot(scope, image);
          cluster.update(writer, 2, "second");
          ChangePage page = access.changes(scope, image.cursor(), 10, 10, 8192, 10_000);
          assertEquals(page.status(), Status.OK);
          assertEquals(page.transactions().size(), 3);
          assertTrue(page.transactions().get(0).records().isEmpty(), "Acknowledgements advance feed progress");
          assertEquals(page.transactions().get(1).records().get(0).value().path("value").asText(), "second");
          assertTrue(page.transactions().get(2).records().isEmpty(), "The read barrier advances feed progress");
          assertTrue(page.next().index() > cluster._lastCommittedIndex);
          current = current.apply(page);
          assertEquals(current.records(RecordKind.CLUSTER_CONFIG).values().iterator().next().value()
              .path("value").asText(), "second");
          assertEquals(current.cursor(), page.next());
          for (int i = 0; i < 3; i++) {
            if (i != follower) {
              cluster.closeVoter(i);
            }
          }
          long start = System.nanoTime();
          expectThrows(IOException.class, () -> access.image(scope, 10, 8192, 500));
          assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(5),
              "Unavailable quorum must respect the caller's bounded deadline");
        }
      }
    }
  }

  @Test
  public void testInstallPauseAllowsInflightCommittedEntry()
      throws Exception {
    Path directory = Files.createTempDirectory("pinot-ratis-install-race-");
    UUID clusterId = UUID.randomUUID();
    UUID historyId = UUID.randomUUID();
    RaftServer server = mock(RaftServer.class);
    when(server.getId()).thenReturn(RaftPeerId.valueOf("voter"));
    try (RaftStorage storage = RaftStorage.newBuilder().setDirectory(directory.resolve("group").toFile())
        .setStorageFreeSpaceMin(RaftServerConfigKeys.STORAGE_FREE_SPACE_MIN_DEFAULT)
        .setLogCorruptionPolicy(RaftServerConfigKeys.Log.CORRUPTION_POLICY_DEFAULT)
        .setOption(RaftStorage.StartupOption.FORMAT).build();
        MetadataStateMachine machine = new MetadataStateMachine(clusterId, historyId)) {
      storage.initialize();
      machine.initialize(server, RaftGroupId.valueOf(historyId), storage);
      byte[] query = MetadataProtocol.encode(new GetRecord(historyId, RecordKind.CLUSTER_CONFIG,
          MetadataProtocol.CLUSTER_CONFIG_NAME));
      expectThrows(CompletionException.class, () -> machine.query(Message.valueOf(ByteString.copyFrom(query))).join());
      machine.pause();
      byte[] command = MetadataProtocol.encode(new RegisterClient(historyId, "client", UUID.randomUUID(), 0));
      LogEntryProto entry = LogEntryProto.newBuilder().setTerm(1).setIndex(1)
          .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(command)))
          .build();
      Message response = machine.applyTransaction(TransactionContext.newBuilder().setStateMachine(machine)
          .setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(entry).build()).join();
      assertEquals(JsonUtils.bytesToJsonNode(response.getContent().toByteArray()).path("status").asText(), "OK");
      assertEquals(machine.getLastAppliedTermIndex().getIndex(), 1L);
      assertEquals(machine.takeSnapshot(), -1L);
      expectThrows(CompletionException.class, () -> machine.query(Message.valueOf(ByteString.copyFrom(query))).join());
    } finally {
      FileUtils.deleteDirectory(directory.toFile());
    }
  }

  @Test(timeOut = 180_000)
  public void testMajoritySnapshotRecoveryAndStorageGuards()
      throws Exception {
    try (Cluster cluster = new Cluster(10_000)) {
      assertStartupRejected(cluster.config(0, false), "bootstrap=true");
      cluster.start();
      byte[] committed;
      JsonNode expected;
      JsonNode preciseExpected;
      BigDecimal precise = new BigDecimal("1.123456789012345678901234567890123456789");
      byte[] preciseQuery = MetadataProtocol.encode(new GetRecord(cluster._historyId, RecordKind.TABLE_CONFIG,
          "precision_OFFLINE"));
      try (RatisMetadataStore.Client client = RatisMetadataStore.connect(cluster.config(0, false))) {
        cluster.register(client);
        committed = cluster.update(client, 1, "before-snapshot");
        ObjectNode indexes = JsonUtils.newObjectNode();
        indexes.putObject("custom").put("decimal", precise);
        TableConfig preciseTable = new TableConfigBuilder(TableType.OFFLINE).setTableName("precision")
            .setFieldConfigList(List.of(new FieldConfig.Builder("column").withIndexes(indexes).build())).build();
        JsonNode preciseWrite = MetadataSnapshotStorage.readJson(client.execute(MetadataProtocol.encode(
            new Transaction(cluster._historyId, "test-client", 1, "precision", 1,
                List.of(Mutation.createTableConfig(cluster._historyId, UUID.randomUUID(), preciseTable))))));
        assertEquals(preciseWrite.path("status").asText(), "OK", preciseWrite.toString());
        cluster._lastCommittedIndex = preciseWrite.at("/revision/index").asLong();
        preciseExpected = MetadataSnapshotStorage.readJson(client.query(preciseQuery)).get("record");
        assertEquals(preciseExpected.at("/value/fieldConfigList/0/indexes/custom/decimal").decimalValue(), precise);
        waitUntil(() -> cluster.stores().stream().allMatch(store -> {
          try {
            return store.division().getInfo().getLastAppliedIndex() >= cluster._lastCommittedIndex;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }));
        for (RatisMetadataStore store : cluster.stores()) {
          assertTrue(store.division().getStateMachine().takeSnapshot() >= cluster._lastCommittedIndex);
        }
        cluster.closeVoter(0);
        cluster.update(client, 2, "after-snapshot");
        expected = cluster.read(client);
        assertEquals(expected.at("/record/value/value").asText(), "after-snapshot");
        expectThrows(IOException.class, () -> client.execute(new byte[MetadataProtocol.MAX_COMMAND_BYTES + 1]));
      }
      cluster.closeVoters();

      PinotConfiguration wrongHistory = cluster.config(0, false);
      wrongHistory.setProperty(RatisConfiguration.PREFIX + "historyId", UUID.randomUUID().toString());
      assertStartupRejected(wrongHistory, "identity differs");
      Path group = cluster._directory.resolve("voter-0").resolve(cluster._historyId.toString());
      Path saved = cluster._directory.resolve("saved-group");
      Files.move(group, saved);
      try {
        assertStartupRejected(cluster.config(0, false), "storage is missing");
      } finally {
        Files.move(saved, group);
      }

      cluster.restart();
      try (RatisMetadataStore.Client client = RatisMetadataStore.connect(cluster.config(1, false))) {
        assertEquals(cluster.read(client).get("record"), expected.get("record"));
        assertEquals(MetadataSnapshotStorage.readJson(client.query(preciseQuery)).get("record"), preciseExpected);
        // Result history survives a snapshot and subsequent committed log replay. Sequence 1 was acknowledged by 2.
        JsonNode expired = JsonUtils.bytesToJsonNode(client.execute(committed));
        assertEquals(expired.path("status").asText(), "RESULT_EXPIRED");
        cluster.closeVoter(0);
        cluster.closeVoter(1);
        expectThrows(IOException.class, () -> cluster.update(client, 3, "without-majority"));
      }
      cluster.closeVoters();

      Path snapshot = newestSnapshot(cluster._directory.resolve("voter-2").resolve(cluster._historyId.toString()));
      byte[] valid = Files.readAllBytes(snapshot);
      byte[] corrupted = valid.clone();
      corrupted[corrupted.length - 1] ^= 1;
      Files.write(snapshot, corrupted);
      try {
        assertStartupRejected(cluster.config(2, false), "checksum");
      } finally {
        Files.write(snapshot, valid);
      }
      Path checkpoint = snapshot.getParent().getParent().resolve("pinot-snapshot-checkpoint");
      byte[] checkpointBytes = Files.readAllBytes(checkpoint);
      try {
        for (String invalid : List.of("-1", "")) {
          Files.writeString(checkpoint, invalid);
          assertStartupRejected(cluster.config(2, false), "recovery marker");
        }
      } finally {
        Files.write(checkpoint, checkpointBytes);
      }
    }
  }

  @Test(timeOut = 240_000)
  public void testOfflineVoterInstallsCompactedSnapshot()
      throws Exception {
    try (Cluster cluster = new Cluster(10)) {
      cluster.start();
      try (RatisMetadataStore.Client client = RatisMetadataStore.connect(cluster.config(0, false))) {
        cluster.register(client);
        cluster.update(client, 1, "initial");
        int offline = cluster.nonLeader();
        long offlineIndex = cluster.store(offline).division().getInfo().getLastAppliedIndex();
        cluster.closeVoter(offline);
        // Fill multiple default Ratis log segments; purging only a logical index is insufficient evidence of install.
        String payload = "x".repeat(512 * 1024);
        for (int sequence = 2; sequence <= 65; sequence++) {
          cluster.update(client, sequence, payload + sequence);
        }
        waitUntil(() -> cluster.leader().division().getRaftLog().getStartIndex() > offlineIndex);
        JsonNode expected = cluster.read(client);
        long committedIndex = cluster._lastCommittedIndex;
        cluster.startVoter(offline, false);
        waitUntil(() -> {
          SnapshotInfo snapshot = cluster.store(offline).division().getStateMachine().getLatestSnapshot();
          return snapshot != null && snapshot.getIndex() > offlineIndex
              && cluster.store(offline).division().getInfo().getLastAppliedIndex() >= committedIndex;
        });
        assertEquals(cluster.read(client).get("record"), expected.get("record"));
        assertNotNull(cluster.store(offline).division().getStateMachine().getLatestSnapshot());
        // Local capture follows the observed application barrier and proves the installed catalog's content.
        MetadataStateMachine machine = (MetadataStateMachine) cluster.store(offline).division().getStateMachine();
        CatalogImage localImage = machine.captureImage(new FeedScope(Set.of(RecordKind.CLUSTER_CONFIG), "test"),
            10, MetadataProtocol.MAX_IMAGE_BYTES, committedIndex, System.nanoTime() + TimeUnit.SECONDS.toNanos(10));
        assertEquals(localImage.records().getFirst(), MetadataProtocol.decodeResult(JsonUtils.objectToBytes(expected))
            .record());
      }
      cluster.closeVoters();
      cluster.restart();
      try (RatisMetadataStore.Client client = RatisMetadataStore.connect(cluster.config(2, false))) {
        assertTrue(cluster.read(client).at("/record/value/value").asText().endsWith("65"));
      }
    }
  }

  private static Path newestSnapshot(Path group)
      throws IOException {
    try (Stream<Path> paths = Files.walk(group)) {
      return paths.filter(path -> path.getFileName().toString().matches("snapshot\\.\\d+_\\d+"))
          .max(Comparator.comparingLong(path -> Long.parseLong(path.getFileName().toString().split("_")[1])))
          .orElseThrow(() -> new IOException("No durable snapshot in " + group));
    }
  }

  private static void assertStartupRejected(PinotConfiguration config, String expectedMessage) {
    IOException error = expectThrows(IOException.class, () -> {
      try (RatisMetadataStore ignored = new RatisMetadataStore(config)) {
        // Close if the expected failure did not occur.
      }
    });
    StringBuilder messages = new StringBuilder();
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      messages.append(cause.getMessage()).append('\n');
    }
    assertTrue(messages.toString().contains(expectedMessage), messages.toString());
  }

  private static void waitUntil(Condition condition)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!condition.satisfied()) {
      assertTrue(System.nanoTime() < deadline, "Timed out waiting for quorum progress");
      Thread.sleep(25);
    }
  }

  private interface Condition {
    boolean satisfied() throws Exception;
  }

  /// Each test owns independent ports, disks, identities and all servers created by it.
  private static final class Cluster implements AutoCloseable {
    final Path _directory = Files.createTempDirectory("pinot-ratis-core-");
    final UUID _clusterId = UUID.randomUUID();
    final UUID _historyId = UUID.randomUUID();
    final UUID _objectId = UUID.randomUUID();
    final String _voters;
    final long _snapshotThreshold;
    final List<RatisMetadataStore> _stores = new ArrayList<>();
    long _lastCommittedIndex;

    Cluster(long threshold)
        throws IOException {
      _snapshotThreshold = threshold;
      try (ServerSocket first = new ServerSocket(0); ServerSocket second = new ServerSocket(0);
          ServerSocket third = new ServerSocket(0)) {
        _voters = "voter-0@127.0.0.1:" + first.getLocalPort() + ",voter-1@127.0.0.1:" + second.getLocalPort()
            + ",voter-2@127.0.0.1:" + third.getLocalPort();
      }
      for (int i = 0; i < 3; i++) {
        _stores.add(null);
      }
    }

    PinotConfiguration config(int voter, boolean bootstrap) {
      Map<String, Object> values = new HashMap<>();
      values.put(RatisConfiguration.PREFIX + "clusterId", _clusterId.toString());
      values.put(RatisConfiguration.PREFIX + "historyId", _historyId.toString());
      values.put(RatisConfiguration.PREFIX + "voters", _voters);
      values.put(RatisConfiguration.PREFIX + "id", "voter-" + voter);
      values.put(RatisConfiguration.PREFIX + "dataDir", _directory.resolve("voter-" + voter).toString());
      values.put(RatisConfiguration.PREFIX + "bootstrap", bootstrap);
      values.put(RatisConfiguration.PREFIX + "snapshot.threshold", _snapshotThreshold);
      values.put(RatisConfiguration.PREFIX + "request.timeoutMs", 10_000L);
      return new PinotConfiguration(values, Map.of());
    }

    void start()
        throws IOException {
      for (int i = 0; i < 3; i++) {
        startVoter(i, true);
      }
    }

    void restart()
        throws IOException {
      for (int i = 0; i < 3; i++) {
        startVoter(i, false);
      }
    }

    void startVoter(int voter, boolean bootstrap)
        throws IOException {
      _stores.set(voter, new RatisMetadataStore(config(voter, bootstrap)));
    }

    RatisMetadataStore store(int voter) {
      return _stores.get(voter);
    }

    List<RatisMetadataStore> stores() {
      return _stores.stream().filter(store -> store != null).toList();
    }

    RatisMetadataStore leader()
        throws IOException {
      for (RatisMetadataStore store : stores()) {
        if (store.division().getInfo().isLeaderReady()) {
          return store;
        }
      }
      throw new IOException("No ready leader");
    }

    int nonLeader()
        throws IOException {
      for (int i = 0; i < 3; i++) {
        if (!store(i).division().getInfo().isLeader()) {
          return i;
        }
      }
      throw new IOException("No follower");
    }

    void register(RatisMetadataStore.Client client)
        throws IOException {
      JsonNode result = JsonUtils.bytesToJsonNode(client.execute(
          MetadataProtocol.encode(new RegisterClient(_historyId, "test-client", UUID.randomUUID(), 0))));
      assertEquals(result.path("status").asText(), "OK");
    }

    byte[] update(RatisMetadataStore.Client client, long sequence, String value)
        throws IOException {
      if (sequence > 1) {
        JsonNode ack = JsonUtils.bytesToJsonNode(client.execute(
            MetadataProtocol.encode(new Acknowledge(_historyId, "test-client", 1, "main", sequence - 1))));
        assertEquals(ack.path("status").asText(), "OK");
      }
      Mutation mutation = sequence == 1 ? Mutation.createClusterConfig(_historyId, _objectId, Map.of("value", value))
          : Mutation.updateClusterConfig(new ObjectVersion(_historyId, _objectId, sequence - 1),
              Map.of("value", value));
      byte[] command = MetadataProtocol.encode(
          new Transaction(_historyId, "test-client", 1, "main", sequence, List.of(mutation)));
      JsonNode result = JsonUtils.bytesToJsonNode(client.execute(command));
      assertEquals(result.path("status").asText(), "OK", result.toString());
      _lastCommittedIndex = result.path("revision").path("index").asLong();
      assertTrue(_lastCommittedIndex > 0, result.toString());
      return command;
    }

    JsonNode read(RatisMetadataStore.Client client)
        throws IOException {
      byte[] query = MetadataProtocol.encode(
          new GetRecord(_historyId, RecordKind.CLUSTER_CONFIG, MetadataProtocol.CLUSTER_CONFIG_NAME));
      return JsonUtils.bytesToJsonNode(client.query(query));
    }

    void closeVoter(int voter)
        throws IOException {
      RatisMetadataStore store = _stores.set(voter, null);
      if (store != null) {
        store.close();
      }
    }

    void closeVoters()
        throws IOException {
      IOException failure = null;
      for (int i = 0; i < 3; i++) {
        try {
          closeVoter(i);
        } catch (IOException e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    }

    @Override
    public void close()
        throws IOException {
      try {
        closeVoters();
      } finally {
        FileUtils.deleteDirectory(_directory.toFile());
      }
    }
  }
}
