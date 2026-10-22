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
package org.apache.pinot.controller.util;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.util.TableSizeMetadataProvider.Replicas;
import org.apache.pinot.controller.util.TableSizeMetadataProvider.Snapshot;
import org.apache.pinot.controller.util.TableSizeMetadataProvider.Table;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Exercises shared size aggregation with a captured non-Helix metadata source and rejected server observations.
public class TableSizeMetadataProviderTest {
  @Test
  public void testCapturedReplicasAndValidatedReports() throws Exception {
    AtomicReference<String> token = new AtomicReference<>("old-token");
    AuthProvider auth = mock(AuthProvider.class);
    when(auth.getRequestHeaders()).thenAnswer(ignored -> Map.of("Authorization", token.get()));
    AtomicBoolean owner = new AtomicBoolean(true);
    AtomicInteger snapshots = new AtomicInteger();
    ControllerMetrics metrics = mock(ControllerMetrics.class);
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try (PoolingHttpClientConnectionManager connections = new PoolingHttpClientConnectionManager()) {
      server.createContext("/tables/test_OFFLINE/size", exchange -> {
        assertEquals(exchange.getRequestHeaders().getFirst("Authorization"), "rotated-token");
        TableSizeInfo report = new TableSizeInfo("test_OFFLINE", 1000,
            List.of(new SegmentSizeInfo("current", 100), new SegmentSizeInfo("stale", 900)));
        byte[] json = JsonUtils.objectToString(report).getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, json.length);
        try (var response = exchange.getResponseBody()) {
          response.write(json);
        }
      });
      server.start();
      List<String> segments = new ArrayList<>(List.of("current", "stale"));
      Map<String, List<String>> assignments = new HashMap<>();
      assignments.put("server", segments);
      assignments.put("missing", List.of("current"));
      BiMap<String, String> endpoints = HashBiMap.create();
      endpoints.put("server", "http://127.0.0.1:" + server.getAddress().getPort());
      Replicas replicas = new Replicas(assignments, endpoints, auth);
      Snapshot snapshot = new Snapshot(new Table("test_OFFLINE", 2, false, replicas), null);
      // Caller mutations cannot rewrite the selected replicas, but auth rotation must remain live.
      segments.clear();
      assignments.clear();
      endpoints.clear();
      token.set("rotated-token");
      TableSizeMetadataProvider provider = new TableSizeMetadataProvider() {
        @Override
        public Snapshot readTableSizes(String tableName, boolean includeReplacedSegments) {
          assertEquals(tableName, "test");
          assertFalse(includeReplacedSegments);
          snapshots.incrementAndGet();
          return snapshot;
        }

        @Override
        public Replicas readReplicaAssignments(String tableNameWithType, boolean includeReplacedSegments) {
          throw new AssertionError("Collection must use the captured replica snapshot");
        }

        @Override
        public boolean canPublishMetrics(String tableNameWithType) {
          return owner.get();
        }

        @Override
        public Map<String, TableSizeInfo> validateServerReports(Replicas captured,
            Map<String, TableSizeInfo> reports) {
          assertSame(captured, replicas);
          assertEquals(reports.get("server").getSegments().size(), 2);
          // Reject a stale content report and lose ownership while HTTP collection is in flight.
          owner.set(false);
          return Map.of("server", new TableSizeInfo("test_OFFLINE", 100,
              List.of(reports.get("server").getSegments().get(0))));
        }
      };
      TableSizeReader.TableSizeDetails result =
          new TableSizeReader(executor, connections, metrics, provider).getTableSizeDetails("test", 5000, false);
      assertEquals(snapshots.get(), 1);
      assertEquals(result._reportedSizeInBytes, 100);
      assertEquals(result._estimatedSizeInBytes, 200);
      assertEquals(result._reportedSizePerReplicaInBytes, 100);
      assertEquals(result._offlineSegments._missingSegments, 1);
      assertEquals(result._offlineSegments._segments.get("stale")._reportedSizeInBytes, -1);
      assertTrue(result._offlineSegments._segments.get("current")._serverInfo.containsKey("missing"));
      verifyNoInteractions(metrics);
    } finally {
      server.stop(0);
      executor.shutdownNow();
    }
  }
}
