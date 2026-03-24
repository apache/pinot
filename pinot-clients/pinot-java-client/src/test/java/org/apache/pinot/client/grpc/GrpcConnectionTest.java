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
package org.apache.pinot.client.grpc;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.client.BrokerSelector;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.common.proto.Broker;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GrpcConnectionTest {

  @Test
  public void testConnectionValidationQueryRunsOnConnectAndMergesDefaultMetadata() {
    Properties properties = new Properties();
    properties.setProperty("headers.Authorization", "Bearer token");
    RecordingBrokerStreamingQueryClient grpcQueryClient = new RecordingBrokerStreamingQueryClient();

    try (GrpcConnection connection =
        new GrpcConnection(properties, new FixedBrokerSelector("localhost:8099"), grpcQueryClient)) {
      Assert.assertEquals(grpcQueryClient.getSubmittedRequests().size(), 1);
      Broker.BrokerRequest validationRequest = grpcQueryClient.getSubmittedRequests().get(0);
      Assert.assertEquals(validationRequest.getSql(), GrpcConnection.CONNECTION_VALIDATION_QUERY);
      Assert.assertEquals(validationRequest.getMetadataOrThrow("Authorization"), "Bearer token");

      connection.executeWithIterator("SELECT * FROM testTable", Map.of("trace", "true"));

      Assert.assertEquals(grpcQueryClient.getSubmittedRequests().size(), 2);
      Broker.BrokerRequest queryRequest = grpcQueryClient.getSubmittedRequests().get(1);
      Assert.assertEquals(queryRequest.getSql(), "SELECT * FROM testTable");
      Assert.assertEquals(queryRequest.getMetadataOrThrow("Authorization"), "Bearer token");
      Assert.assertEquals(queryRequest.getMetadataOrThrow("trace"), "true");
    }
  }

  private static final class FixedBrokerSelector implements BrokerSelector {
    private final String _broker;

    private FixedBrokerSelector(String broker) {
      _broker = broker;
    }

    @Override
    public String selectBroker(String... tableNames) {
      return _broker;
    }

    @Override
    public List<String> getBrokers() {
      return Collections.singletonList(_broker);
    }

    @Override
    public void close() {
    }
  }

  private static final class RecordingBrokerStreamingQueryClient extends GrpcConnection.BrokerStreamingQueryClient {
    private final List<Broker.BrokerRequest> _submittedRequests = new ArrayList<>();

    private RecordingBrokerStreamingQueryClient() {
      super(new GrpcConfig(Collections.emptyMap()));
    }

    @Override
    public Iterator<Broker.BrokerResponse> submit(String brokerHost, int brokerGrpcPort,
        Broker.BrokerRequest brokerRequest) {
      _submittedRequests.add(brokerRequest);
      return Collections.singletonList(Broker.BrokerResponse.newBuilder()
          .setPayload(ByteString.copyFromUtf8("{}"))
          .build()).iterator();
    }

    private List<Broker.BrokerRequest> getSubmittedRequests() {
      return _submittedRequests;
    }
  }
}
