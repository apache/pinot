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

import java.util.Iterator;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.proto.Broker;
import org.apache.pinot.common.proto.PinotQueryBrokerGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BrokerGrpcQueryClient extends BaseGrpcQueryClient<Broker.BrokerRequest, Broker.BrokerResponse> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerGrpcQueryClient.class);

  private final PinotQueryBrokerGrpc.PinotQueryBrokerBlockingStub _blockingStub;

  public BrokerGrpcQueryClient(String host, int port, GrpcConfig config) {
    super(host, port, config);
    _blockingStub = PinotQueryBrokerGrpc.newBlockingStub(getChannel());
  }

  @Override
  public Iterator<Broker.BrokerResponse> submit(Broker.BrokerRequest request) {
    return _blockingStub.submit(request);
  }
}
