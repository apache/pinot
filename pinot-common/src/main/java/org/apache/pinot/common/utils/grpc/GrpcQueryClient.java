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
package org.apache.pinot.common.utils.grpc;

import io.grpc.ManagedChannelBuilder;
import java.util.Iterator;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server;


public class GrpcQueryClient {
  private final PinotQueryServerGrpc.PinotQueryServerBlockingStub _blockingStub;

  public GrpcQueryClient(String host, int port) {
    this(host, port, new Config());
  }

  public GrpcQueryClient(String host, int port, Config config) {
    ManagedChannelBuilder managedChannelBuilder = ManagedChannelBuilder
        .forAddress(host, port)
        .maxInboundMessageSize(config.getMaxInboundMessageSizeBytes());
    if (config.isUsePlainText()) {
      managedChannelBuilder.usePlaintext();
    }
    _blockingStub = PinotQueryServerGrpc.newBlockingStub(managedChannelBuilder.build());
  }

  public Iterator<Server.ServerResponse> submit(Server.ServerRequest request) {
    return _blockingStub.submit(request);
  }

  public static class Config {
    // Default max message size to 128MB
    private static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE = 128 * 1024 * 1024;
    private final int _maxInboundMessageSizeBytes;
    private final boolean _usePlainText;

    public Config() {
      this(DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE, true);
    }

    public Config(int maxInboundMessageSizeBytes, boolean usePlainText) {
      _maxInboundMessageSizeBytes = maxInboundMessageSizeBytes;
      _usePlainText = usePlainText;
    }

    public int getMaxInboundMessageSizeBytes() {
      return _maxInboundMessageSizeBytes;
    }

    public boolean isUsePlainText() {
      return _usePlainText;
    }
  }
}
