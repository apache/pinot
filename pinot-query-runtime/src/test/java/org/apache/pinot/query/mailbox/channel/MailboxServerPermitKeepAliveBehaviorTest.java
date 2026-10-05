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
package org.apache.pinot.query.mailbox.channel;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.grpc.GrpcKeepAliveConfig;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.MultiStageQueryRunner;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Proves the mailbox server's keep-alive **enforcement** actually changes behaviour, which the
/// config-parsing tests in [MailboxServerPermitKeepAliveTest] cannot: delete both `permitKeepAlive`
/// calls from [GrpcMailboxServer] and those still pass, because they only assert the values it read.
///
/// The enforcement is the half that hurts a live cluster when it is wrong. A server whose
/// `permitKeepAliveTime` is above the client's keep-alive time counts the pings as "bad" and, past
/// gRPC's two-strike threshold, answers `GOAWAY(ENHANCE_YOUR_CALM)` with `too_many_pings` — dropping a
/// mailbox channel mid-query. That is what makes the client interval untunable without this knob.
///
/// Two servers are driven through one wait window: one permitting a fast ping, one left at Netty's
/// default. Only the second may lose its stream. Ablating the permit wiring makes the *first*
/// assertion fail, so the test cannot pass for the wrong reason.
public class MailboxServerPermitKeepAliveBehaviorTest {
  /// gRPC clamps a client keep-alive time up to its own 10s floor, so this is the fastest ping a client
  /// can be made to send, and the enforcement window is sized around it.
  private static final int CLIENT_KEEP_ALIVE_TIME_MS = 10_000;
  /// A permit below the client interval: every ping is then acceptable and no strike is recorded.
  private static final int PERMISSIVE_PERMIT_MS = 1_000;
  /// Three pings at 10s plus slack. gRPC allows two bad pings and sends `GOAWAY` on the third, so a
  /// server that does not permit the rate gives up at roughly 30s.
  private static final long WINDOW_MS = 60_000;

  @Test(timeOut = 180_000)
  public void testPermitKeepAliveTimeDecidesWhetherAFastPingerSurvives()
      throws Exception {
    MailboxService permissive = startMailboxService(PERMISSIVE_PERMIT_MS);
    MailboxService restrictive = startMailboxService(-1);  // leaves Netty's 5-minute default in place
    ChannelManager channelManager = new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES,
        // `withoutCalls` off, as in production: the open stream below is what makes the client ping.
        new GrpcKeepAliveConfig(CLIENT_KEEP_ALIVE_TIME_MS, 5_000, false));

    ManagedChannel toPermissive = channelManager.getChannel("localhost", permissive.getPort());
    ManagedChannel toRestrictive = channelManager.getChannel("localhost", restrictive.getPort());
    try {
      // No content is ever sent: `GrpcMailboxServer#open` accepts a stream without the mailbox-id
      // header and only reads it when a message arrives, so the stream stays open with no handshake and
      // the connection carries nothing but keep-alive pings.
      OpenStream onPermissive = openStream(toPermissive);
      OpenStream onRestrictive = openStream(toRestrictive);

      assertTrue(onRestrictive.awaitTermination(WINDOW_MS),
          "a server that does not permit the client's ping rate must end the stream; without the "
              + "permit knob this is what every mailbox channel would do once the interval is tuned down");
      Throwable rejection = onRestrictive._error.get();
      assertNotNull(rejection);
      assertTrue(String.valueOf(Status.fromThrowable(rejection).getDescription()).contains("too_many_pings"),
          "expected GOAWAY(ENHANCE_YOUR_CALM) for too_many_pings, got: " + rejection);

      // Same client, same interval, same window — the only difference is the permit.
      assertNull(onPermissive._error.get(),
          "the permitted server must keep the stream; if this fails the permit is not reaching the "
              + "gRPC server builder");
    } finally {
      toPermissive.shutdownNow();
      toRestrictive.shutdownNow();
      permissive.shutdown();
      restrictive.shutdown();
    }
  }

  private static MailboxService startMailboxService(int permitKeepAliveTimeMs) {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        MultiStageQueryRunner.KEY_OF_MAILBOX_SERVER_PERMIT_KEEP_ALIVE_TIME_MS, permitKeepAliveTimeMs));
    MailboxService mailboxService = new MailboxService(
        "localhost", QueryTestUtils.getAvailablePort(), InstanceType.SERVER, config);
    mailboxService.start();
    return mailboxService;
  }

  private static OpenStream openStream(ManagedChannel channel) {
    OpenStream stream = new OpenStream();
    PinotMailboxGrpc.newStub(channel).open(stream);
    return stream;
  }

  /// Client-side observer of one `open` stream: records how it ended, if it ended.
  private static final class OpenStream implements StreamObserver<Mailbox.MailboxStatus> {
    private final CountDownLatch _terminated = new CountDownLatch(1);
    private final AtomicReference<Throwable> _error = new AtomicReference<>();

    @Override
    public void onNext(Mailbox.MailboxStatus value) {
    }

    @Override
    public void onError(Throwable t) {
      _error.compareAndSet(null, t);
      _terminated.countDown();
    }

    @Override
    public void onCompleted() {
      _terminated.countDown();
    }

    boolean awaitTermination(long timeoutMs)
        throws InterruptedException {
      return _terminated.await(timeoutMs, TimeUnit.MILLISECONDS);
    }
  }
}
