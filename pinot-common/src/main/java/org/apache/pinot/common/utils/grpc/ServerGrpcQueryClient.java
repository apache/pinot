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

import java.util.Collections;
import java.util.Iterator;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.proto.PinotQueryServerGrpc;
import org.apache.pinot.common.proto.Server;


public class ServerGrpcQueryClient extends BaseGrpcQueryClient<Server.ServerRequest, Server.ServerResponse> {
  private final PinotQueryServerGrpc.PinotQueryServerBlockingStub _blockingStub;

  public ServerGrpcQueryClient(String host, int port) {
    this(host, port, new GrpcConfig(Collections.emptyMap()));
  }

  public ServerGrpcQueryClient(String host, int port, GrpcConfig config) {
    super(host, port, config);
    _blockingStub = PinotQueryServerGrpc.newBlockingStub(getChannel());
  }

  public Iterator<Server.ServerResponse> submit(Server.ServerRequest request) {
    return _blockingStub.submit(request);
  }
}
