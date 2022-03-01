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
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pinot.connector.presto.grpc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.apache.pinot.common.utils.grpc.GrpcRequestBuilder;


/**
 * Grpc based Pinot query client.
 */
public class PinotStreamingQueryClient {
  private final Map<String, GrpcQueryClient> _grpcQueryClientMap = new HashMap<>();
  private final GrpcQueryClient.Config _config;

  public PinotStreamingQueryClient(GrpcQueryClient.Config config) {
    _config = config;
  }

  public Iterator<Server.ServerResponse> submit(String host, int port, GrpcRequestBuilder requestBuilder) {
    GrpcQueryClient client = getOrCreateGrpcQueryClient(host, port);
    return client.submit(requestBuilder.build());
  }

  private GrpcQueryClient getOrCreateGrpcQueryClient(String host, int port) {
    String key = String.format("%s_%d", host, port);
    if (!_grpcQueryClientMap.containsKey(key)) {
      _grpcQueryClientMap.put(key, new GrpcQueryClient(host, port, _config));
    }
    return _grpcQueryClientMap.get(key);
  }
}
