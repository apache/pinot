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

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import java.util.Iterator;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server;


public class GrpcQueryClient {
  private final PinotQueryServerGrpc.PinotQueryServerBlockingStub _blockingStub;

  public GrpcQueryClient(String host, int port) {
    // Set max message size to 128MB
    Channel channel =
        ManagedChannelBuilder.forAddress(host, port).maxInboundMessageSize(128 * 1024 * 1024).usePlaintext().build();
    _blockingStub = PinotQueryServerGrpc.newBlockingStub(channel);
  }

  public Iterator<Server.ServerResponse> submit(Server.ServerRequest request) {
    return _blockingStub.submit(request);
  }
}
