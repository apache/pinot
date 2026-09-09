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

import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.datablock.ZeroCopyDataBlockSerde;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Mailbox.MailboxContent;
import org.apache.pinot.common.proto.Mailbox.MailboxStatus;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.mailbox.channel.ChannelManager;
import org.apache.pinot.query.mailbox.channel.ChannelUtils;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.ArrowBlock;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.memory.ArrowBuffers;
import org.apache.pinot.query.runtime.memory.ArrowQueryContext;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.apache.pinot.segment.spi.memory.PinotByteBuffer;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/**
 * Tests localhost gRPC interoperability with a codec-pinned pre-Arrow peer, not an older binary.
 * Peer callbacks are stream-confined; blocking queues and futures synchronize assertions with delivery.
 */
public class ArrowLegacyMailboxPeerTest {
  private static final int LEGACY_VERSION = 2;
  private static final int TYPE_SHIFT = 5;
  private static final int VERSION_MASK = (1 << TYPE_SHIFT) - 1;
  private static final int CHUNK_BYTES = 127;
  private static final int TIMEOUT_SECONDS = 5;
  private static final String BUFFER_SIZE = "buffer.size";
  private static final String EARLY_TERMINATE = "request.early.terminate";
  private static final String ARROW_VERSION = "arrow.ipc.version";

  @Test
  public void testArrowEnabledSenderToLegacyReceiver()
      throws Exception {
    String mailboxId = "arrow-to-pinned-legacy";
    LegacyReceiver peer = new LegacyReceiver(mailboxId);
    Server server =
        NettyServerBuilder.forAddress(new InetSocketAddress("localhost", 0)).addService(peer).build().start();
    try {
      LegacyFeedback feedback = new LegacyFeedback();
      ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext()
          .intercept(new AckBarrier(feedback)).build();
      try {
        // Only channel construction is customized, to observe ACKs after the real sender has processed them.
        ChannelManager channels = new ChannelManager(null, 1024, Duration.ofMinutes(1), 64 * 1024, 32 * 1024) {
          @Override
          public ManagedChannel getChannel(String hostname, int port) {
            return channel;
          }
        };
        StatMap<MailboxSendOperator.StatKey> stats = new StatMap<>(MailboxSendOperator.StatKey.class);
        try (ArrowBuffers buffers = ArrowMailboxTest.newBuffers();
            ArrowQueryContext sourceContext = ArrowMailboxTest.newContext(buffers, 0);
            QueryThreadContext ignored = QueryThreadContext.openForMseTest();
            GrpcSendingMailbox sending = new GrpcSendingMailbox(mailboxId, channels, "localhost", server.getPort(),
                System.currentTimeMillis() + 30_000, stats, CHUNK_BYTES * 2, true, true)) {
          ArrowBlock source = ArrowMailboxTest.newBlock(sourceContext);
          try {
            assertFalse(sending.isArrowIpcSupported());
            assertEquals(sending.getReceiverBufferSize(), 5);
            sending.send(source);
            MailboxStatus firstAck = feedback.next();
            assertEquals(firstAck.getMailboxId(), mailboxId);
            assertEquals(firstAck.getMetadataMap(), Map.of(BUFFER_SIZE, "0"));
            assertEquals(sending.getReceiverBufferSize(), 0, "The sender must have consumed the classic ACK");
            assertFalse(sending.isArrowIpcSupported(), "A classic ACK must not enable Arrow");
            assertLegacyRows(peer.next());

            // This send occurs after the upgraded status observer consumed the legacy ACK, not just after send().
            sending.send(source);
          } finally {
            source.release();
          }
          sourceContext.close();
          assertEquals(buffers.getAllocatedMemory(), 0L);
          assertLegacyRows(peer.next());
          assertEquals(feedback.next().getMetadataMap(), Map.of(BUFFER_SIZE, "3", EARLY_TERMINATE, "true"));
          assertTrue(sending.isEarlyTerminated());
          assertFalse(sending.isArrowIpcSupported());

          // Data must now be skipped, but the legacy EOS and its statistics must still be delivered.
          sending.send(new RowHeapDataBlock(ArrowMailboxTest.rows(), ArrowMailboxTest.SCHEMA));
          List<DataBuffer> serializedStats = MultiStageQueryStats.emptyStats(1).serialize();
          sending.send(SuccessMseBlock.INSTANCE, serializedStats);
          LegacyFrame eos = peer.next();
          assertEquals(eos.versionAndType(), LEGACY_VERSION + (2 << TYPE_SHIFT));
          assertEquals(((MetadataBlock) eos.block()).getType(), MetadataBlock.MetadataBlockType.EOS);
          assertStats(eos.block().getStatsByStage(), serializedStats);
          peer._completed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
          feedback._completed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
          assertTrue(sending.isTerminated());
          assertEquals(stats.getInt(MailboxSendOperator.StatKey.RAW_MESSAGES), 3);
          assertTrue(peer._frames.isEmpty(), "Early-terminated data must not reach the old receiver");
        }
      } finally {
        closeChannel(channel);
      }
    } finally {
      server.shutdown();
      try {
        assertTrue(server.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      } finally {
        server.shutdownNow();
      }
    }
  }

  @DataProvider
  public Object[][] legacyVersions() {
    return new Object[][]{{1}, {LEGACY_VERSION}};
  }

  @Test(dataProvider = "legacyVersions")
  public void testLegacySenderToArrowCapableReceiver(int version)
      throws Exception {
    PinotConfiguration config = new PinotConfiguration(Map.of(
        CommonConstants.Helix.CONFIG_OF_MULTI_STAGE_ENGINE_USE_ARROW, true,
        CommonConstants.MultiStageQueryRunner.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_SIZE_BYTES, 1024));
    MailboxService service =
        new MailboxService("localhost", QueryTestUtils.getAvailablePort(), InstanceType.SERVER, config);
    WorkerMetadata worker = new WorkerMetadata(0, Map.of(), Map.of());
    OpChainExecutionContext context = OperatorTestUtil.getOpChainContext(service, System.currentTimeMillis() + 30_000,
        new StageMetadata(0, List.of(worker), Map.of()));
    String mailboxId = "pinned-legacy-v" + version + "-to-arrow";
    ReceivingMailbox receiving = service.getReceivingMailbox(mailboxId);
    receiving.registeredReader(() -> { });
    try {
      service.start();
      ManagedChannel channel = NettyChannelBuilder.forAddress("localhost", service.getPort()).usePlaintext().build();
      try (QueryThreadContext ignored = QueryThreadContext.openForMseTest()) {
        LegacyFeedback feedback = new LegacyFeedback();
        StreamObserver<MailboxContent> outgoing = PinotMailboxGrpc.newStub(channel)
            .withDeadlineAfter(30, TimeUnit.SECONDS).open(feedback);
        DataBlock rows =
            new RowHeapDataBlock(ArrowMailboxTest.rows(), ArrowMailboxTest.SCHEMA).asSerialized().getDataBlock();

        assertFalse(receiving.canReceiveArrow());
        assertTrue(sendLegacy(outgoing, mailboxId, rows, version) > 1);
        assertEquals(feedback.next().getMetadataMap(), Map.of(BUFFER_SIZE, "1"));
        assertEquals(feedback._bufferSize, 1);
        assertReceivedRows(receiving);

        receiving.enableArrow(context.getOrCreateArrowContext());
        assertTrue(receiving.canReceiveArrow());
        // The old sender parses only classic feedback keys. It keeps using its pinned codec even after this ACK.
        for (int frame = 0; frame < 2; frame++) {
          assertTrue(sendLegacy(outgoing, mailboxId, rows, version) > 1);
          MailboxStatus ack = feedback.next();
          assertEquals(ack.getMailboxId(), mailboxId);
          assertEquals(ack.getMetadataMap(),
              Map.of(BUFFER_SIZE, "1", ARROW_VERSION, ChannelUtils.ARROW_IPC_VERSION));
          assertEquals(feedback._bufferSize, 1);
          assertFalse(feedback._earlyTerminated);
          assertReceivedRows(receiving);
        }

        receiving.earlyTerminate();
        sendLegacy(outgoing, mailboxId, rows, version);
        assertEquals(feedback.next().getMetadataMap(), Map.of(EARLY_TERMINATE, "true"));
        assertTrue(feedback._earlyTerminated);
        assertEquals(receiving.getNumPendingBlocks(), 0);
        List<DataBuffer> serializedStats = MultiStageQueryStats.emptyStats(1).serialize();
        sendLegacy(outgoing, mailboxId, MetadataBlock.newEosWithStats(serializedStats), version);
        outgoing.onCompleted();
        feedback._completed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        ReceivingMailbox.MseBlockWithStats eos = receiving.poll();
        assertNotNull(eos);
        assertTrue(eos.getBlock().isSuccess());
        assertStats(eos.getSerializedStats(), serializedStats);
        assertEquals(receiving.getNumPendingBlocks(), 0);
        receiving.closeArrow();
        assertFalse(receiving.canReceiveArrow());
        context.closeArrowResources();
        service.releaseReceivingMailbox(receiving);
      } finally {
        closeChannel(channel);
      }
    } finally {
      receiving.closeArrow();
      context.closeArrowResources();
      service.shutdown();
    }
  }

  @Test
  public void testPinnedCodecRejectsActualArrowIpc()
      throws Exception {
    try (ArrowBuffers buffers = ArrowMailboxTest.newBuffers();
        ArrowQueryContext context = ArrowMailboxTest.newContext(buffers, 0)) {
      ArrowBlock source = ArrowMailboxTest.newBlock(context);
      ByteString payload;
      try {
        // Only this rejection fixture uses upgraded serialization; neither legacy peer uses version dispatch.
        payload = ByteString.copyFrom(source.getDataBlock().serialize().stream().map(ByteString::copyFrom).toList());
      } finally {
        source.release();
      }
      context.close();
      assertEquals(buffers.getAllocatedMemory(), 0L);
      assertEquals(payload.asReadOnlyByteBuffer().getInt(), 3 + (3 << TYPE_SHIFT));
      expectThrows(IOException.class, () -> deserializeLegacy(payload));
    }
  }

  @DataProvider
  public Object[][] unsupportedLegacyHeaders() {
    return new Object[][]{{3, 0}, {LEGACY_VERSION, 3}};
  }

  @Test(dataProvider = "unsupportedLegacyHeaders")
  public void testPinnedCodecRejectsNewVersionAndNewTypeIndependently(int version, int type)
      throws Exception {
    byte[] payload = serializeLegacy(MetadataBlock.newEos(), LEGACY_VERSION).toByteArray();
    ByteBuffer.wrap(payload).putInt(version + (type << TYPE_SHIFT));
    expectThrows(IOException.class, () -> deserializeLegacy(ByteString.copyFrom(payload)));
  }

  private static void assertLegacyRows(LegacyFrame frame) {
    assertEquals(frame.versionAndType(), LEGACY_VERSION);
    assertTrue(frame.chunks() > 1, "The pinned receiver must assemble multiple protobuf messages");
    assertEquals(frame.block().getDataBlockType(), DataBlock.Type.ROW);
    ArrowMailboxTest.assertRows(new SerializedDataBlock(frame.block()));
  }

  private static void assertReceivedRows(ReceivingMailbox receiving) {
    // A status ACK is sent only after offerRaw returns, so polling here needs neither a sleep nor a retry.
    ReceivingMailbox.MseBlockWithStats received = receiving.poll();
    assertNotNull(received);
    MseBlock.Data data = (MseBlock.Data) received.getBlock();
    assertTrue(data.isSerialized(), "A legacy sender must keep sending legacy bytes after an Arrow advertisement");
    assertFalse(data.isArrow());
    ArrowMailboxTest.assertRows(data);
  }

  private static void assertStats(List<DataBuffer> actual, List<DataBuffer> expected) {
    assertNotNull(actual);
    assertEquals(actual.size(), expected.size());
    for (int stage = 0; stage < expected.size(); stage++) {
      DataBuffer expectedStage = expected.get(stage);
      if (expectedStage == null) {
        assertNull(actual.get(stage), "Missing-stage statistics must stay absent");
      } else {
        assertNotNull(actual.get(stage));
        assertEquals(bytes(actual.get(stage)), bytes(expectedStage), "Statistics for stage " + stage);
      }
    }
  }

  private static byte[] bytes(DataBuffer buffer) {
    byte[] bytes = new byte[Math.toIntExact(buffer.size())];
    buffer.copyTo(0, bytes);
    return bytes;
  }

  private static int sendLegacy(StreamObserver<MailboxContent> outgoing, String mailboxId, DataBlock block, int version)
      throws IOException {
    ByteString payload = serializeLegacy(block, version);
    int chunks = 0;
    for (int offset = 0; offset < payload.size(); offset += CHUNK_BYTES) {
      int end = Math.min(offset + CHUNK_BYTES, payload.size());
      outgoing.onNext(MailboxContent.newBuilder().setMailboxId(mailboxId)
          .setPayload(payload.substring(offset, end)).setWaitForMore(end < payload.size()).build());
      chunks++;
    }
    return chunks;
  }

  // ZeroCopyDataBlockSerde is unchanged from foundation 7ed46ec06aa466992750a078cbb77084c33b63d4.
  // Pin its entry points and the old wire header here, never DataBlockUtils or the extensible Version/Type lookup.
  private static ByteString serializeLegacy(DataBlock block, int version)
      throws IOException {
    checkLegacyVersion(version);
    int type = block.getDataBlockType().ordinal();
    legacyType(type);
    try (DataBuffer buffer = new ZeroCopyDataBlockSerde().serialize(block, version + (type << TYPE_SHIFT))) {
      // Copy before closing the source buffer; the outbound gRPC stream must not borrow its lifetime.
      return ByteString.copyFrom(bytes(buffer));
    }
  }

  private static DataBlock deserializeLegacy(ByteString payload)
      throws IOException {
    if (payload.size() < Integer.BYTES) {
      throw new IOException("Truncated legacy header");
    }
    DataBuffer buffer = PinotByteBuffer.wrap(payload.asReadOnlyByteBuffer());
    int header = buffer.getInt(0);
    checkLegacyVersion(header & VERSION_MASK);
    return new ZeroCopyDataBlockSerde().deserialize(buffer, 0, legacyType(header >>> TYPE_SHIFT), null);
  }

  private static void checkLegacyVersion(int version)
      throws IOException {
    if (version != 1 && version != LEGACY_VERSION) {
      throw new IOException("Unsupported legacy version: " + version);
    }
  }

  private static DataBlock.Type legacyType(int ordinal)
      throws IOException {
    switch (ordinal) {
      case 0:
        return DataBlock.Type.ROW;
      case 1:
        return DataBlock.Type.COLUMNAR;
      case 2:
        return DataBlock.Type.METADATA;
      default:
        throw new IOException("Unsupported legacy type: " + ordinal);
    }
  }

  private static void closeChannel(ManagedChannel channel)
      throws InterruptedException {
    channel.shutdown();
    try {
      assertTrue(channel.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    } finally {
      channel.shutdownNow();
    }
  }

  private record LegacyFrame(int versionAndType, int chunks, DataBlock block) {
  }

  private static final class LegacyReceiver extends PinotMailboxGrpc.PinotMailboxImplBase {
    private final String _mailboxId;
    private final BlockingQueue<LegacyFrame> _frames = new LinkedBlockingQueue<>();
    private final CompletableFuture<Void> _completed = new CompletableFuture<>();

    private LegacyReceiver(String mailboxId) {
      _mailboxId = mailboxId;
    }

    private LegacyFrame next()
        throws InterruptedException {
      LegacyFrame frame = _frames.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertNotNull(frame, "No legacy frame received; stream completion: " + _completed);
      return frame;
    }

    @Override
    public StreamObserver<MailboxContent> open(StreamObserver<MailboxStatus> response) {
      return new StreamObserver<MailboxContent>() {
        private ByteString _payload = ByteString.EMPTY;
        private int _chunks;
        private int _dataFrames;
        private boolean _eos;

        @Override
        public void onNext(MailboxContent content) {
          try {
            if (!_mailboxId.equals(content.getMailboxId()) || _eos) {
              throw new IOException("Unexpected mailbox or frame after EOS");
            }
            _payload = _payload.concat(content.getPayload());
            _chunks++;
            if (content.getWaitForMore()) {
              return;
            }
            DataBlock block = deserializeLegacy(_payload);
            _frames.add(new LegacyFrame(_payload.asReadOnlyByteBuffer().getInt(), _chunks, block));
            _payload = ByteString.EMPTY;
            _chunks = 0;
            if (block instanceof MetadataBlock) {
              _eos = true;
            } else {
              _dataFrames++;
              MailboxStatus.Builder ack = MailboxStatus.newBuilder().setMailboxId(_mailboxId)
                  .putMetadata(BUFFER_SIZE, _dataFrames == 1 ? "0" : "3");
              if (_dataFrames == 2) {
                ack.putMetadata(EARLY_TERMINATE, "true");
              }
              // No Arrow advertisement: this peer only knows the two classic feedback keys.
              response.onNext(ack.build());
            }
          } catch (IOException | RuntimeException e) {
            _completed.completeExceptionally(e);
            response.onError(Status.INVALID_ARGUMENT.withCause(e).asRuntimeException());
          }
        }

        @Override
        public void onError(Throwable t) {
          _completed.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
          if (!_eos || !_payload.isEmpty()) {
            IOException error = new IOException("Legacy stream closed without a complete EOS");
            _completed.completeExceptionally(error);
            response.onError(Status.INVALID_ARGUMENT.withCause(error).asRuntimeException());
          } else {
            response.onCompleted();
            _completed.complete(null);
          }
        }
      };
    }
  }

  /** Pinned status handling ignores unknown keys, including an upgraded receiver's Arrow advertisement. */
  private static final class LegacyFeedback implements StreamObserver<MailboxStatus> {
    private final BlockingQueue<MailboxStatus> _statuses = new LinkedBlockingQueue<>();
    private final CompletableFuture<Void> _completed = new CompletableFuture<>();
    private volatile int _bufferSize = 5;
    private volatile boolean _earlyTerminated;

    @Override
    public void onNext(MailboxStatus status) {
      _bufferSize = Integer.parseInt(status.getMetadataMap().getOrDefault(BUFFER_SIZE, "5"));
      _earlyTerminated |= Boolean.parseBoolean(status.getMetadataMap().get(EARLY_TERMINATE));
      _statuses.add(status);
    }

    @Override
    public void onError(Throwable t) {
      _completed.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
      _completed.complete(null);
    }

    private MailboxStatus next()
        throws InterruptedException {
      MailboxStatus status = _statuses.poll(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      assertNotNull(status, "No classic feedback received; stream completion: " + _completed);
      return status;
    }
  }

  /** Publishes ACKs only after the upgraded sender's actual observer has consumed them. */
  private static final class AckBarrier implements ClientInterceptor {
    private final LegacyFeedback _feedback;

    private AckBarrier(LegacyFeedback feedback) {
      _feedback = feedback;
    }

    @Override
    public <REQ, RESP> ClientCall<REQ, RESP> interceptCall(MethodDescriptor<REQ, RESP> method,
        CallOptions callOptions, Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
        @Override
        public void start(Listener<RESP> listener, Metadata headers) {
          super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
            @Override
            public void onMessage(RESP message) {
              super.onMessage(message);
              _feedback.onNext((MailboxStatus) message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
              super.onClose(status, trailers);
              if (status.isOk()) {
                _feedback.onCompleted();
              } else {
                _feedback.onError(status.asRuntimeException());
              }
            }
          }, headers);
        }
      };
    }
  }
}
