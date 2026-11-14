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
package org.apache.pinot.query.mailbox;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.runtime.blocks.ErrorMseBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.query.testutils.TcpRelay;
import org.apache.pinot.query.testutils.TestDnsNameResolverProvider;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Mailbox data reaching the wrong server after a receiver's address is handed to another server.
///
/// Every server is addressed by its own hostname and one pinned mailbox port, as on Kubernetes with fixed ports. When
/// two server pods restart together they can swap nodes and take each other's pod IPs. gRPC resolves a hostname again
/// only when a connection fails or is lost, so a sender whose channel to server A last resolved while A's old IP was
/// still in DNS reconnects to that IP later. If server B now holds it and listens on the same port, the connection
/// works, and B used to accept the stream as its own.
///
/// [TestDnsNameResolverProvider] plays DNS, a [TcpRelay] plays the IP address that moves from A to B, and loopback
/// ports play the other IP addresses.
public class MailboxMisdeliveryTest {
  private static final String SERVER_A = "server-a.pinot-test";
  private static final String SERVER_A_ID = "Server_" + SERVER_A + "_8098";
  private static final String SERVER_B = "server-b.pinot-test";
  private static final String SERVER_B_ID = "Server_" + SERVER_B + "_8098";
  private static final String SENDER = "sender.pinot-test";
  /// The mailbox port every server advertises. Nothing binds it: the test DNS maps names to the real ports.
  private static final int PINNED_MAILBOX_PORT = 8101;
  private static final long SERVER_A_ID_HASH = QueryServerInstance.hashInstanceId(SERVER_A_ID);
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"col"}, new ColumnDataType[]{ColumnDataType.STRING});

  private final List<MailboxService> _mailboxServices = new ArrayList<>();
  private TestDnsNameResolverProvider _dns;
  private TcpRelay _addressX;
  private int _requestId;

  @BeforeMethod
  public void setUp() {
    ServerMetrics.deregister();
    ServerMetrics.register(new ServerMetrics(PinotMetricUtils.getPinotMetricsRegistry()));
    _dns = TestDnsNameResolverProvider.register();
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    try {
      for (MailboxService mailboxService : _mailboxServices) {
        mailboxService.shutdown();
      }
      _mailboxServices.clear();
      if (_addressX != null) {
        _addressX.close();
        _addressX = null;
      }
    } finally {
      _dns.close();
      ServerMetrics.deregister();
    }
  }

  /// The incident, with a plan that does not identify the receiver (as from an older broker): the stale channel
  /// delivers the block for A to B, which keeps it in a mailbox nobody reads, while A never hears from the sender and
  /// its stage waits until the query times out.
  @Test
  public void testStaleChannelMisdeliversWithoutReceiverIdentity()
      throws Exception {
    Swap swap = swapAddresses(true);

    String mailboxId = nextMailboxId();
    sendBlockAndEos(swap._sender, mailboxId, 0);

    // B accepts the stream and keeps the block in a receiving mailbox that no operator of B will ever read. It is
    // only dropped when the cache evicts it, which logs "Evicting dangling receiving mailbox".
    TestUtils.waitForCondition(aVoid -> {
      ReceivingMailbox orphan = swap._serverB.getReceivingMailboxIfPresent(mailboxId);
      return orphan != null && orphan.getNumPendingBlocks() > 0;
    }, 10_000L, "The block for server A never reached server B");
    assertNull(swap._restartedServerA.getReceivingMailboxIfPresent(mailboxId),
        "Server A received a stream that the stale channel sent to server B");
  }

  /// With the receiver's identity in the plan, B rejects the stream. The sender resets the channel, fails its later
  /// sends, and tells A through a new stream, so A fails at once instead of at the query deadline. The next stream
  /// reaches A.
  @Test
  public void testMisroutedStreamIsRejectedAndFailsFast()
      throws Exception {
    Swap swap = swapAddresses(true);
    long rejectedBefore = meterCount(ServerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED);
    long rejectedByPeerBefore = meterCount(ServerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED_BY_PEER);

    String mailboxId = nextMailboxId();
    ReceivingMailbox atA = readMailbox(swap._restartedServerA, mailboxId);
    SendingMailbox sendingMailbox = getSendingMailbox(swap._sender, mailboxId, SERVER_A_ID_HASH);
    try {
      sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{"row"}));
    } catch (QueryException e) {
      // The rejection can arrive before the first send returns
      assertEquals(e.getErrorCode(), QueryErrorCode.SERVER_NOT_RESPONDING);
    }

    // A receives an error that says what happened, well before any query deadline
    MseBlock block = awaitBlock(atA);
    assertTrue(block.isError(), "Expected an error block, got: " + block);
    String errorMessage = ((ErrorMseBlock) block).getErrorMessages().get(QueryErrorCode.SERVER_NOT_RESPONDING);
    assertNotNull(errorMessage);
    assertTrue(errorMessage.contains(SERVER_B_ID) && errorMessage.contains("stale"), errorMessage);

    // B rejected the stream before creating a receiving mailbox for it
    assertNull(swap._serverB.getReceivingMailboxIfPresent(mailboxId));
    assertEquals(meterCount(ServerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED), rejectedBefore + 1);
    assertEquals(meterCount(ServerMeter.MULTI_STAGE_MISROUTED_REQUESTS_REJECTED_BY_PEER), rejectedByPeerBefore + 1);

    // The sender fails the stage instead of sending more data into the void
    QueryException e = expectThrows(QueryException.class,
        () -> sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{"row"})));
    assertEquals(e.getErrorCode(), QueryErrorCode.SERVER_NOT_RESPONDING);
    assertTrue(e.getMessage().contains(SERVER_B_ID), e.getMessage());

    // The channel resolved the hostname again: the next stream reaches A
    String nextMailboxId = nextMailboxId();
    sendBlockAndEos(swap._sender, nextMailboxId, SERVER_A_ID_HASH);
    assertReceivesBlockAndEos(swap._restartedServerA, nextMailboxId);
  }

  /// With the check disabled on the receiver, it accepts any stream, as before.
  @Test
  public void testRejectionCanBeDisabled()
      throws Exception {
    Swap swap = swapAddresses(false);

    String mailboxId = nextMailboxId();
    sendBlockAndEos(swap._sender, mailboxId, SERVER_A_ID_HASH);

    TestUtils.waitForCondition(aVoid -> {
      ReceivingMailbox orphan = swap._serverB.getReceivingMailboxIfPresent(mailboxId);
      return orphan != null && orphan.getNumPendingBlocks() > 0;
    }, 10_000L, "Server B with the check disabled did not accept the stream");
  }

  /// Puts the test in the state of the incident: the sender's channel to A points to A's old address X, which server
  /// B has taken over, while A runs again at a new address that DNS now returns.
  private Swap swapAddresses(boolean serverBRejectsMisrouted)
      throws Exception {
    int portA = QueryTestUtils.getAvailablePort();
    MailboxService serverA = startMailboxService(SERVER_A, portA, SERVER_A_ID, true);
    _addressX = new TcpRelay(portA);
    _dns.map(SERVER_A, _addressX.getPort());
    MailboxService sender = startMailboxService(SENDER, QueryTestUtils.getAvailablePort(), null, true);

    String mailboxId = nextMailboxId();
    sendBlockAndEos(sender, mailboxId, SERVER_A_ID_HASH);
    assertReceivesBlockAndEos(serverA, mailboxId);

    // A's pod goes away. gRPC resolves A's hostname right then, while DNS still returns X: a terminating pod stays in
    // DNS for a while.
    ManagedChannel channel = sender.getChannelManager().getChannel(SERVER_A, PINNED_MAILBOX_PORT);
    int numResolutions = _dns.getNumResolutions();
    stopMailboxService(serverA);
    TestUtils.waitForCondition(
        aVoid -> channel.getState(false) != ConnectivityState.READY && _dns.getNumResolutions() > numResolutions,
        10_000L, "The channel did not notice that server A went away");

    // B takes over address X, on the same pinned port. A comes back at another address, and DNS returns that one from
    // now on, but the channel has no reason to ask again.
    MailboxService serverB =
        startMailboxService(SERVER_B, QueryTestUtils.getAvailablePort(), SERVER_B_ID, serverBRejectsMisrouted);
    _addressX.switchTo(serverB.getPort());
    int portY = QueryTestUtils.getAvailablePort();
    MailboxService restartedServerA = startMailboxService(SERVER_A, portY, SERVER_A_ID, true);
    _dns.map(SERVER_A, portY);
    return new Swap(sender, serverB, restartedServerA);
  }

  private static class Swap {
    final MailboxService _sender;
    final MailboxService _serverB;
    final MailboxService _restartedServerA;

    Swap(MailboxService sender, MailboxService serverB, MailboxService restartedServerA) {
      _sender = sender;
      _serverB = serverB;
      _restartedServerA = restartedServerA;
    }
  }

  private MailboxService startMailboxService(String hostname, int port, @Nullable String instanceId,
      boolean rejectMisrouted) {
    PinotConfiguration config =
        new PinotConfiguration(Map.of(MultiStageQueryRunner.KEY_OF_REJECT_MISROUTED_REQUESTS, rejectMisrouted));
    MailboxService mailboxService =
        new MailboxService(hostname, port, InstanceType.SERVER, config, null, null, instanceId);
    mailboxService.start();
    _mailboxServices.add(mailboxService);
    return mailboxService;
  }

  private void stopMailboxService(MailboxService mailboxService) {
    mailboxService.shutdown();
    _mailboxServices.remove(mailboxService);
  }

  private String nextMailboxId() {
    return MailboxIdUtils.toMailboxId(++_requestId, 1, 0, 0, 0);
  }

  private static SendingMailbox getSendingMailbox(MailboxService sender, String mailboxId,
      long receiverInstanceIdHash) {
    StatMap<MailboxSendOperator.StatKey> stats = new StatMap<>(MailboxSendOperator.StatKey.class);
    return sender.getSendingMailbox(SERVER_A, PINNED_MAILBOX_PORT, receiverInstanceIdHash, mailboxId,
        System.currentTimeMillis() + 30_000L, stats);
  }

  private static void sendBlockAndEos(MailboxService sender, String mailboxId, long receiverInstanceIdHash)
      throws Exception {
    SendingMailbox sendingMailbox = getSendingMailbox(sender, mailboxId, receiverInstanceIdHash);
    sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, new Object[]{"row"}));
    sendingMailbox.send(SuccessMseBlock.INSTANCE, MultiStageQueryStats.emptyStats(1).serialize());
  }

  private static ReceivingMailbox readMailbox(MailboxService receiver, String mailboxId) {
    ReceivingMailbox receivingMailbox = receiver.getReceivingMailbox(mailboxId);
    receivingMailbox.registeredReader(() -> {
    });
    return receivingMailbox;
  }

  private static MseBlock awaitBlock(ReceivingMailbox receivingMailbox) {
    MseBlock[] block = new MseBlock[1];
    TestUtils.waitForCondition(aVoid -> {
      ReceivingMailbox.MseBlockWithStats blockWithStats = receivingMailbox.poll();
      block[0] = blockWithStats != null ? blockWithStats.getBlock() : null;
      return block[0] != null;
    }, 10_000L, "No block received");
    return block[0];
  }

  private static void assertReceivesBlockAndEos(MailboxService receiver, String mailboxId) {
    ReceivingMailbox receivingMailbox = readMailbox(receiver, mailboxId);
    MseBlock first = awaitBlock(receivingMailbox);
    assertTrue(first.isData(), "Expected a data block, got: " + first);
    assertTrue(awaitBlock(receivingMailbox).isSuccess());
  }

  private static long meterCount(ServerMeter meter) {
    return ServerMetrics.get().getMeteredValue(meter).count();
  }
}
