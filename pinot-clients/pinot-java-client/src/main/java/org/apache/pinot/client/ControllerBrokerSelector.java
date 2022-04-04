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

package org.apache.pinot.client;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Future;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;


// TODO maintain Map of table -> list of brokers, instead of single list of brokers
public class ControllerBrokerSelector implements BrokerSelector {
    private static final Random RANDOM = new Random();
    private List<String> _brokers;

    private static class BrokerInstance {
        @JsonProperty("host")
        public String _host;

        @JsonProperty("port")
        public int _port;

        @JsonProperty("instanceName")
        public String _instanceName;
    }

    private class BrokerUpdater extends Thread {
        private final AsyncHttpClient _client;
        private static final String ADDRESS_FORMAT = "http://%s:%d/v2/brokers/tables";
        private final ObjectMapper _objectMapper = new ObjectMapper();
        private final String _address;
        private final TypeReference<Map<String, List<BrokerInstance>>> _responseTypeRef =
            new TypeReference<Map<String, List<BrokerInstance>>>() { };


        public BrokerUpdater(String controllerHost, int controllerPort) {
            _client = Dsl.asyncHttpClient();
            _address = String.format(ADDRESS_FORMAT, controllerHost, controllerPort);
        }

        public void updateBrokers() {
            BoundRequestBuilder getRequest = _client.prepareGet(_address);
            Future<Response> responseFuture = getRequest.addHeader("accept", "application/json").execute();
            try {
                Response response = responseFuture.get();
                String responseBody = response.getResponseBody(StandardCharsets.UTF_8);
                Map<String, List<BrokerInstance>> responses = _objectMapper.readValue(responseBody, _responseTypeRef);
                List<String> brokers = new ArrayList<>();
                for (Map.Entry<String, List<BrokerInstance>> b: responses.entrySet()) {
                    b.getValue().forEach(br -> {
                        brokers.add(br._host + ":" + br._port);
                    });
                }
                _brokers = brokers;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    // TODO can we make this reactive instead of polling? websocket endpoint in controller?
                    Thread.sleep(1000);
                    updateBrokers();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public ControllerBrokerSelector(String controllerHost, int controllerPort) {
        _brokers = new ArrayList<>();
        BrokerUpdater brokerUpdater = new BrokerUpdater(controllerHost, controllerPort);
        brokerUpdater.updateBrokers();
        brokerUpdater.start();
    }

    @Override
    public String selectBroker(String table) {
        // TODO add table -> broker mapping
        return _brokers.get(RANDOM.nextInt(_brokers.size()));
    }

    @Override
    public List<String> getBrokers() {
        return _brokers;
    }

    @Override
    public void close() {
        // stop updaterThread? Run it via an executor?
    }
}
