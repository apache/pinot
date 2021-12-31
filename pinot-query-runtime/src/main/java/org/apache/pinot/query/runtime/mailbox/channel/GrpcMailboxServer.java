package org.apache.pinot.query.runtime.mailbox.channel;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.runtime.mailbox.GrpcMailboxService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class GrpcMailboxServer extends PinotMailboxGrpc.PinotMailboxImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcMailboxServer.class);
  private static final long DEFAULT_GRPC_MAILBOX_SERVER_TIMEOUT = 10000L;

  private final GrpcMailboxService _mailboxService;
  private final Server _server;

  public GrpcMailboxServer(GrpcMailboxService mailboxService, int port) {
    _mailboxService = mailboxService;
    _server = ServerBuilder.forPort(port).addService(this).build();
    LOGGER.info("Initialized GrpcMailboxServer on port: {}", port);
  }

  public void start() {
    try {
      _server.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void shutdown() {
    try {
      _server.shutdown().awaitTermination(DEFAULT_GRPC_MAILBOX_SERVER_TIMEOUT, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public StreamObserver<Mailbox.MailboxContent> open(StreamObserver<Mailbox.MailboxStatus> responseObserver) {
    return new MailboxContentStreamObserver(_mailboxService, responseObserver);
  }
}
