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
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.grpc.GrpcKeepAliveConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Proves that a mailbox channel notices a peer that stops answering *without* closing its socket, which is the
/// failure keep-alive exists for: no `RST` ever arrives, so gRPC keeps the cached channel in `READY` and every send
/// on it parks until the query deadline — for every later query too, since channels are cached per peer.
///
/// The peer is faked with a TCP relay that stops moving bytes in both directions while holding both sockets open.
/// That is indistinguishable, from the client's side, from a host whose kernel is hung or that sits behind a
/// one-way partition. Killing the server process instead would prove nothing: it sends a `RST`, and gRPC has always
/// handled that.
///
/// Both channels in this test are black-holed at the same instant and only the keep-alive one is expected to fail,
/// so the assertion that the other is still `READY` is what keeps this test honest — ablate the keep-alive wiring
/// and the first assertion fails rather than the test passing for the wrong reason.
public class MailboxChannelKeepAliveTest {
  /// gRPC clamps `keepAliveTime` up to its own 10s floor, so this is as fast as detection can be configured.
  private static final int KEEP_ALIVE_TIME_MS = 10_000;
  private static final int KEEP_ALIVE_TIMEOUT_MS = 1_000;
  /// Generous headroom over the ~11s the settings above imply; a loaded CI box must not flake this.
  private static final long DETECTION_BUDGET_MS = 45_000;

  @Test(timeOut = 120_000)
  public void testKeepAliveDetectsSilentPeer()
      throws Exception {
    SilentMailbox silentPeer = new SilentMailbox(2);
    Server peer = NettyServerBuilder.forPort(0).addService(silentPeer).build().start();
    try (BlackHoleRelay relay = new BlackHoleRelay(peer.getPort())) {
      ChannelManager keepAliveManager = newChannelManager(
          new GrpcKeepAliveConfig(KEEP_ALIVE_TIME_MS, KEEP_ALIVE_TIMEOUT_MS, false));
      ChannelManager noKeepAliveManager = newChannelManager(GrpcKeepAliveConfig.DISABLED);

      ManagedChannel keepAliveChannel = keepAliveManager.getChannel("localhost", relay.getPort());
      ManagedChannel noKeepAliveChannel = noKeepAliveManager.getChannel("localhost", relay.getPort());
      try {
        // Keep-alive with `withoutCalls` off only pings while a call is active, which is the production default, so
        // both channels get an open stream. Sending one message is what forces the RPC onto the transport.
        OpenStream withKeepAlive = openStream(keepAliveChannel);
        OpenStream withoutKeepAlive = openStream(noKeepAliveChannel);

        // Wait until the *peer* has received both streams, not merely until the relay accepted both TCP
        // connections. Accepting happens before a single HTTP/2 byte is relayed, so black-holing on that signal can
        // strand the RPC before it ever starts — and with `withoutCalls` off, a channel with no active call is
        // never pinged, so the test would fail with keep-alive working perfectly.
        assertTrue(silentPeer.awaitStreams(30_000), "streams did not reach the peer");

        relay.blackHole();

        assertTrue(withKeepAlive.await(DETECTION_BUDGET_MS),
            "keep-alive channel never failed: a silent peer must not be able to park a mailbox send forever");
        Throwable error = withKeepAlive._error.get();
        assertNotNull(error);
        assertEquals(Status.fromThrowable(error).getCode(), Status.Code.UNAVAILABLE,
            "expected the transport to be declared dead, got: " + error);

        // Same black hole, same elapsed time, keep-alive off: still believed healthy. This is the state the
        // production default used to leave every mailbox channel in.
        assertNull(withoutKeepAlive._error.get(),
            "channel without keep-alive should still believe the dead peer is healthy");
      } finally {
        keepAliveChannel.shutdownNow();
        noKeepAliveChannel.shutdownNow();
      }
    } finally {
      peer.shutdownNow();
    }
  }

  private static ChannelManager newChannelManager(GrpcKeepAliveConfig keepAliveConfig) {
    return new ChannelManager(null, 4_000_000, Duration.ofDays(365),
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_HIGH_WATER_MARK_BYTES,
        CommonConstants.MultiStageQueryRunner.DEFAULT_GRPC_WRITE_BUFFER_LOW_WATER_MARK_BYTES, keepAliveConfig);
  }

  private static OpenStream openStream(ManagedChannel channel) {
    OpenStream stream = new OpenStream();
    StreamObserver<Mailbox.MailboxContent> sender = PinotMailboxGrpc.newStub(channel).open(stream);
    sender.onNext(Mailbox.MailboxContent.getDefaultInstance());
    return stream;
  }

  /// Client-side observer of one `open` stream: records the terminal error, if any.
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
      _error.compareAndSet(null, new StatusRuntimeException(Status.INTERNAL.withDescription("unexpected completion")));
      _terminated.countDown();
    }

    boolean await(long timeoutMs)
        throws InterruptedException {
      return _terminated.await(timeoutMs, TimeUnit.MILLISECONDS);
    }
  }

  /// A mailbox peer that accepts `open` and then says nothing at all, so the only traffic left on the connection is
  /// the keep-alive ping this test is about.
  private static final class SilentMailbox extends PinotMailboxGrpc.PinotMailboxImplBase {
    private final CountDownLatch _streamsReceived;

    private SilentMailbox(int expectedStreams) {
      _streamsReceived = new CountDownLatch(expectedStreams);
    }

    boolean awaitStreams(long timeoutMs)
        throws InterruptedException {
      return _streamsReceived.await(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public StreamObserver<Mailbox.MailboxContent> open(StreamObserver<Mailbox.MailboxStatus> responseObserver) {
      return new StreamObserver<>() {
        private final AtomicBoolean _counted = new AtomicBoolean();

        @Override
        public void onNext(Mailbox.MailboxContent value) {
          // Counting the first message rather than `open` itself: a message proves the client's stream is live on
          // the transport, which is precisely the precondition for keep-alive pings.
          if (_counted.compareAndSet(false, true)) {
            _streamsReceived.countDown();
          }
        }

        @Override
        public void onError(Throwable t) {
        }

        @Override
        public void onCompleted() {
        }
      };
    }
  }

  /// TCP relay that can stop forwarding while keeping every socket open.
  ///
  /// Sockets are read with a short `SO_TIMEOUT` rather than blocking indefinitely, so a relay thread parked in
  /// `read` still notices [#blackHole] promptly. Nothing is ever closed until [#close], because closing is exactly
  /// what this fixture must not do: a closed socket produces the `FIN`/`RST` that gRPC already detects.
  private static final class BlackHoleRelay implements Closeable {
    private final ServerSocket _listener;
    private final int _upstreamPort;
    private final ExecutorService _executor = Executors.newCachedThreadPool();
    private final List<Socket> _sockets = new CopyOnWriteArrayList<>();
    private final AtomicBoolean _blackHole = new AtomicBoolean();
    private final AtomicBoolean _closed = new AtomicBoolean();

    BlackHoleRelay(int upstreamPort)
        throws IOException {
      _upstreamPort = upstreamPort;
      _listener = new ServerSocket();
      _listener.bind(new InetSocketAddress("localhost", 0));
      _executor.submit(this::acceptLoop);
    }

    int getPort() {
      return _listener.getLocalPort();
    }

    /// Stops moving bytes in both directions, holding all sockets open.
    void blackHole() {
      _blackHole.set(true);
    }

    private void acceptLoop() {
      while (!_closed.get()) {
        try {
          Socket downstream = _listener.accept();
          Socket upstream = new Socket("localhost", _upstreamPort);
          downstream.setSoTimeout(100);
          upstream.setSoTimeout(100);
          _sockets.add(downstream);
          _sockets.add(upstream);
          _executor.submit(() -> relay(downstream, upstream));
          _executor.submit(() -> relay(upstream, downstream));
        } catch (IOException e) {
          return;
        }
      }
    }

    private void relay(Socket from, Socket to) {
      byte[] buffer = new byte[8192];
      try {
        InputStream in = from.getInputStream();
        OutputStream out = to.getOutputStream();
        while (!_closed.get()) {
          if (_blackHole.get()) {
            // Park instead of returning: returning would let the streams be garbage collected and the sockets
            // closed, which would hand the client the RST this fixture must withhold.
            Thread.sleep(50);
            continue;
          }
          int read;
          try {
            read = in.read(buffer);
          } catch (SocketTimeoutException e) {
            continue;
          }
          if (read < 0) {
            return;
          }
          out.write(buffer, 0, read);
          out.flush();
        }
      } catch (IOException | InterruptedException e) {
        // Relay is done; the test asserts on the client's view, not on this thread.
      }
    }

    @Override
    public void close()
        throws IOException {
      _closed.set(true);
      _listener.close();
      for (Socket socket : _sockets) {
        try {
          socket.close();
        } catch (IOException e) {
          // Best effort teardown.
        }
      }
      _executor.shutdownNow();
    }
  }
}
