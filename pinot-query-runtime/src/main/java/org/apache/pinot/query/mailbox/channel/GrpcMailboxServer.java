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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.proto.Mailbox;
import org.apache.pinot.common.proto.PinotMailboxGrpc;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.query.service.QueryConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@code GrpcMailboxServer} manages GRPC-based mailboxes by creating a stream-stream GRPC server.
 *
 * <p>This GRPC server is responsible for constructing {@link StreamObserver} out of an initial "open" request
 * send by the sender of the sender/receiver pair.
 */
public class GrpcMailboxServer extends PinotMailboxGrpc.PinotMailboxImplBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(GrpcMailboxServer.class);
  private static final long DEFAULT_GRPC_MAILBOX_SERVER_TIMEOUT = 10000L;

  private final GrpcMailboxService _mailboxService;
  private final Server _server;

  public GrpcMailboxServer(GrpcMailboxService mailboxService, int port, PinotConfiguration extraConfig) {
    _mailboxService = mailboxService;
    _server = ServerBuilder.forPort(port)
        .addService(this)
        .maxInboundMessageSize(extraConfig.getProperty(QueryConfig.KEY_OF_MAX_INBOUND_QUERY_DATA_BLOCK_BYTES_SIZE,
            QueryConfig.DEFAULT_MAX_INBOUND_QUERY_DATA_BLOCK_BYTES_SIZE))
        .build();
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
