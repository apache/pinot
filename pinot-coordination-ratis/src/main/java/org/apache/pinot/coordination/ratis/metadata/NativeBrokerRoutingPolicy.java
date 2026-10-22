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
package org.apache.pinot.coordination.ratis.metadata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import javax.annotation.Nullable;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.spi.config.table.TableConfig;


/// Administrator-owned broker tenant access and query endpoint approval. Participant advertisements cannot create
/// these grants; both policy and advertisements must agree in the same committed routing observation.
public record NativeBrokerRoutingPolicy(Map<String, Set<String>> brokers, Map<String, String> servers,
                                       @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, String> principals) {
  public static final String CONFIG_KEY = "pinot.coordination.ratis.broker.routing";
  public static final int MAX_PRINCIPALS = 64;
  private static final int MAX_IDENTITIES = 64;
  private static final int MAX_TENANTS = 64;

  public NativeBrokerRoutingPolicy {
    Map<String, Set<String>> copy = new TreeMap<>();
    brokers.forEach((id, tenants) -> copy.put(id, Set.copyOf(tenants)));
    brokers = Map.copyOf(copy);
    servers = Map.copyOf(servers);
    principals = Map.copyOf(principals);
  }

  /// Legacy routing policy can discover installed replicas but grants no certificate-bound query authority.
  public NativeBrokerRoutingPolicy(Map<String, Set<String>> brokers, Map<String, String> servers) {
    this(brokers, servers, Map.of());
  }

  public static NativeBrokerRoutingPolicy parse(String json) {
    require(json != null && !json.isBlank(), "Native broker routing policy is not configured");
    try {
      JsonNode root = MetadataProtocol.decodeValue(json.getBytes(StandardCharsets.UTF_8), JsonNode.class);
      require(root.isObject() && root.path("brokers").isObject() && root.path("servers").isObject()
          && (root.size() == 2 || root.size() == 3 && root.path("principals").isObject()),
          "Routing policy requires brokers, servers and an optional principals object");
      require(root.path("brokers").size() <= MAX_IDENTITIES && root.path("servers").size() <= MAX_IDENTITIES,
          "Routing policy exceeds its identity bound");
      Map<String, Set<String>> brokers = new TreeMap<>();
      for (var entry : root.path("brokers").properties()) {
        identifier(entry.getKey());
        JsonNode values = entry.getValue();
        require(values.isArray() && values.size() > 0 && values.size() <= MAX_TENANTS,
            "Broker policy requires a bounded nonempty tenant array");
        Set<String> tenants = new HashSet<>();
        for (JsonNode value : values) {
          require(value.isTextual(), "Broker tenants must be strings");
          identifier(value.textValue());
          require(tenants.add(value.textValue()), "Duplicate broker tenant");
        }
        brokers.put(entry.getKey(), tenants);
      }
      Map<String, String> servers = new TreeMap<>();
      Set<String> endpoints = new HashSet<>();
      for (var entry : root.path("servers").properties()) {
        identifier(entry.getKey());
        require(entry.getValue().isTextual(), "Server endpoint must be a string");
        String endpoint = entry.getValue().textValue();
        validateEndpoint(endpoint);
        require(endpoints.add(endpoint), "Query endpoint cannot identify multiple servers");
        servers.put(entry.getKey(), endpoint);
      }
      Map<String, String> principals = new TreeMap<>();
      if (root.has("principals")) {
        JsonNode values = root.path("principals");
        require(values.isObject() && values.size() <= MAX_PRINCIPALS, "Query principals exceed their bound");
        for (var entry : values.properties()) {
          require(entry.getKey().matches("[0-9a-f]{64}") && entry.getValue().isTextual(),
              "Query principals require lowercase SHA-256 certificate fingerprints and broker IDs");
          String broker = entry.getValue().textValue();
          require(brokers.containsKey(broker), "Query principal references an unapproved broker");
          principals.put(entry.getKey(), broker);
        }
      }
      return new NativeBrokerRoutingPolicy(brokers, servers, principals);
    } catch (IOException e) {
      throw new CoordinationRejection(Status.INVALID_REQUEST, "Invalid native broker routing policy JSON");
    }
  }

  public boolean permits(String brokerId, TableConfig table) {
    return brokers.getOrDefault(brokerId, Set.of()).contains(brokerTenant(table));
  }

  public static String brokerTenant(TableConfig table) {
    require(table != null && table.getTenantConfig() != null, "Routing requires a broker tenant");
    String tenant = table.getTenantConfig().getBroker();
    // Match Pinot's existing omitted-tenant convention, without granting any default broker policy entry.
    tenant = tenant == null ? "DefaultTenant" : tenant;
    identifier(tenant);
    return tenant;
  }

  @Nullable
  public String endpoint(ParticipantState server) {
    String approved = servers.get(server.participant().participantId());
    return approved != null && server.capabilities().contains(Capability.SERVER_QUERY)
        && approved.equals(server.endpoints().get("queryGrpc"))
        && "2".equals(server.endpoints().get("nativeQueryVersion")) ? approved : null;
  }

  private static void validateEndpoint(String value) {
    require(value != null && value.length() <= MembershipProtocol.MAX_ENDPOINT_VALUE_LENGTH,
        "Query endpoint exceeds its bound");
    try {
      URI uri = new URI(value);
      require("grpcs".equals(uri.getScheme()) && uri.getHost() != null
          && uri.getHost().equals(uri.getHost().toLowerCase(Locale.ROOT))
          && uri.getPort() > 0 && uri.getPort() <= 65535 && uri.getRawUserInfo() == null
          && uri.getRawQuery() == null && uri.getRawFragment() == null
          && (uri.getRawPath() == null || uri.getRawPath().isEmpty())
          && value.equals(new URI("grpcs", null, uri.getHost(), uri.getPort(), null, null, null).toASCIIString()),
          "Query endpoint requires a canonical grpcs host and port without path or credentials");
    } catch (URISyntaxException e) {
      throw new CoordinationRejection(Status.INVALID_REQUEST, "Invalid native query endpoint");
    }
  }

  private static void identifier(String value) {
    OfflineSegmentCatalog.identifier(value, 1024);
    require(!value.contains("*"), "Routing policy does not accept wildcards");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new CoordinationRejection(Status.INVALID_REQUEST, message);
    }
  }
}
